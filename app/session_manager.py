"""会话注册表与快照生命周期管理。

设计要点
========
1. 每个会话独占一条 PostgreSQL 连接，并在其上开启
   ``START TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY``。
   连接以 "idle in transaction" 状态保持，事务快照在整个会话期间不变；
   源端其它连接上的增/改/删照常提交，本会话各页（含重复取页）读到的
   两张表始终是开启那一刻的同一快照（MVCC，读写互不阻塞）。
2. 会话状态机：``open -> closing -> closed/expired``。关闭请求受理的
   第一时间即把状态翻成 ``closing``，此后尚未开始响应的取页一律失败；
   已经在执行中的取页允许跑完，随后回滚事务、归还连接、释放名额。
3. 名额（默认 2）在内存注册表中控制，会话状态全部保存在本进程内；
   进程重启或与数据库的连接丢失后旧会话不可恢复，符合
   "不得换快照续取" 的要求。

收尾链路的失败处理（本模块重点）
================================
关闭/到期的收尾分两步：回滚快照事务 -> 把连接归还连接池。
**只有连接确实归还成功，会话才允许进入 closed/expired 终态并释放名额**：

- 归还失败（连接池故障/连接被拒绝接收等）时，会话停留在 ``closing``：
  旧会话不可再取页，状态仍可查询，名额继续占用（不会出现"接口报了
  closed 但连接其实泄漏、新会话借不到连接而卡死"）。
- 收尾可重试：用户重复调用 close、sweeper 每轮巡检都会对停留在
  ``closing`` 的会话再次尝试归还；连接池恢复后下一次收尾即成功，
  届时才回报对应的终态（主动关闭 -> closed，到期 -> expired）并开放名额。
- 并发重复收尾由 ``io_lock`` + ``conn_returned`` 标志双重保证：
  同一条连接最多被归还一次，绝不会重复 putconn。
- 任何情况下都不会为旧会话换一条连接续取：连接断裂的会话只能作废，
  快照连接与会话生命周期严格绑定。
"""
from __future__ import annotations

import threading
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from typing import Optional

import psycopg
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

from .config import Settings


class SessionError(Exception):
    """会话层面的可预期错误，路由层据此映射 HTTP 状态码。"""

    def __init__(self, code: str, message: str, http_status: int = 409):
        super().__init__(message)
        self.code = code
        self.message = message
        self.http_status = http_status


class CapacityReached(SessionError):
    def __init__(self, max_sessions: int):
        super().__init__(
            "capacity_reached",
            f"会话名额已满（最多 {max_sessions} 个），请关闭其它会话后重试",
            http_status=429,
        )


class SessionNotFound(SessionError):
    def __init__(self, session_id: str):
        super().__init__(
            "session_not_found",
            f"会话 {session_id} 不存在或已失效（服务重启/连接丢失后旧会话不可续取）",
            http_status=404,
        )


class SessionUnavailable(SessionError):
    """会话处于 closing/closed/expired，取页请求被拒绝。"""


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


@dataclass
class Session:
    session_id: str
    region: str
    conn: psycopg.Connection
    opened_at: datetime
    expires_at: datetime
    # state_lock 保护 status / 计数器 / 时间戳；
    # io_lock 串行化同一会话上的查询，保证取页与关闭不会并发使用连接。
    state_lock: threading.RLock = field(default_factory=threading.RLock)
    io_lock: threading.Lock = field(default_factory=threading.Lock)
    status: str = "open"
    pages_served: int = 0
    last_page_at: Optional[datetime] = None
    in_flight: int = 0
    snapshot_time: Optional[datetime] = None
    snapshot_id: Optional[str] = None
    # 关闭受理时记录的终态原因：closed（主动关闭）/ expired（到期）/
    # connection_lost（取页时连接断裂）/ shutdown（进程停止）。
    # 归还成功后据此决定终态是 closed 还是 expired。
    close_reason: Optional[str] = None
    # 快照事务是否已回滚（收尾只回滚一次，重试不重复操作连接事务状态）。
    rollback_done: bool = False
    # 连接是否已成功归还连接池。终态只允许在该标志置位后发生，
    # 同时它也是"同一条连接绝不重复归还"的最终凭据。
    conn_returned: bool = False
    # 收尾尝试次数与最近一次失败原因，供状态查询/演示排障使用。
    teardown_attempts: int = 0
    last_teardown_error: Optional[str] = None
    last_teardown_at: Optional[datetime] = None

    def is_open(self) -> bool:
        with self.state_lock:
            return self.status == "open" and _utcnow() < self.expires_at


class SessionManager:
    def __init__(self, settings: Settings):
        self._settings = settings
        self._registry: dict[str, Session] = {}
        self._registry_lock = threading.RLock()
        self._pool: Optional[ConnectionPool] = None
        self._stop_event = threading.Event()
        self._sweeper: Optional[threading.Thread] = None
        # 调试用故障注入开关（仅 ENABLE_DEBUG_HOOKS=true 时可通过接口拨动）：
        # 置位期间所有 putconn 一律失败，用于演示收尾链路的失败与恢复。
        self._fail_return = False
        self._fail_return_lock = threading.Lock()

    # ------------------------------------------------------------------
    # 生命周期
    # ------------------------------------------------------------------
    @staticmethod
    def _reset_connection(conn: psycopg.Connection) -> None:
        """连接归还池时：回滚残留事务并恢复自动提交，供下一个借用者干净使用。"""
        try:
            if conn.info.transaction_status != psycopg.pq.TransactionStatus.IDLE:
                conn.rollback()
        except psycopg.Error:
            pass
        try:
            conn.autocommit = True
        except psycopg.Error:
            pass

    def start(self) -> None:
        # 池默认自动提交：admin 写语句执行即提交；
        # 会话连接借出后显式 START TRANSACTION 长期持有快照事务。
        if self._pool is None or self._pool.closed:
            self._pool = ConnectionPool(
                self._settings.database_url,
                min_size=self._settings.pool_min_size,
                max_size=self._settings.pool_max_size,
                kwargs={"row_factory": dict_row, "autocommit": True},
                # 连接归还池时恢复自动提交并回滚任何残留事务
                reset=self._reset_connection,
                open=True,
            )
        # 故障注入开关随 start 复位，避免重启后残留模拟故障。
        with self._fail_return_lock:
            self._fail_return = False
        # 每次 start 都确保清理线程在运行（stop 后重新 start 时需要重建）
        if self._sweeper is None or not self._sweeper.is_alive():
            self._stop_event = threading.Event()
            self._sweeper = threading.Thread(
                target=self._sweep_loop, name="session-sweeper", daemon=True
            )
            self._sweeper.start()

    def stop(self) -> None:
        self._stop_event.set()
        if self._sweeper is not None:
            self._sweeper.join(timeout=5)
        self._sweeper = None
        with self._registry_lock:
            session_ids = list(self._registry.keys())
        for sid in session_ids:
            try:
                # shutdown 语义：尽力收尾；进程退出后注册表整体丢弃，
                # 未归还成功的连接随连接池 close 一并物理关闭，不会泄漏。
                self.close(sid, reason="shutdown")
            except SessionError:
                pass
        if self._pool is not None:
            self._pool.close()
            self._pool = None
        # 进程重启语义：内存注册表全部丢弃，旧会话 id 不可再查、不可续取
        with self._registry_lock:
            self._registry.clear()

    # ------------------------------------------------------------------
    # 调试用故障注入（仅演示/自测，ENABLE_DEBUG_HOOKS 关闭时路由层不可达）
    # ------------------------------------------------------------------
    def set_return_fault(self, enabled: bool) -> dict:
        with self._fail_return_lock:
            self._fail_return = enabled
        return self.debug_state()

    def debug_state(self) -> dict:
        with self._fail_return_lock:
            fault = self._fail_return
        stats = self._pool.get_stats() if self._pool is not None else {}
        return {
            "return_fault_enabled": fault,
            "pool_stats": stats,
        }

    def _return_conn(self, session: Session) -> None:
        """把会话连接归还连接池；故障注入或池异常时抛出，交由收尾链路重试。

        成功路径只执行一次（调用方持有 io_lock 且先查 conn_returned）。
        """
        with self._fail_return_lock:
            fail = self._fail_return
        if fail:
            # 模拟"连接池暂时不可用、拒绝接收归还连接"的真实故障：
            # 连接保持在会话手中不触碰，待故障解除后原样归还。
            raise psycopg.OperationalError(
                "模拟故障：连接池暂时拒绝归还连接（return fault injected）"
            )
        assert self._pool is not None
        self._pool.putconn(session.conn)

    # ------------------------------------------------------------------
    # 开启会话
    # ------------------------------------------------------------------
    def open_session(self, region: str) -> Session:
        with self._registry_lock:
            # 名额检查与注册表写入在同一把锁内完成，避免并发超额。
            # closing 会话在连接确实归还前继续占用名额（见模块文档）。
            active = [
                s for s in self._registry.values()
                if s.status in ("open", "closing")
            ]
            if len(active) >= self._settings.max_sessions:
                raise CapacityReached(self._settings.max_sessions)

            assert self._pool is not None
            conn = self._pool.getconn()
            now = _utcnow()
            session = Session(
                session_id=uuid.uuid4().hex,
                region=region,
                conn=conn,
                opened_at=now,
                expires_at=now
                + timedelta(seconds=self._settings.session_ttl_seconds),
            )
            try:
                # 保持 autocommit=True：直接发 START TRANSACTION 由服务端
                # 显式开启 RR 只读事务（若先关 autocommit，psycopg 会隐式
                # 发 BEGIN，再发 START TRANSACTION 会产生重复事务警告）。
                # 事务在首条语句冻结快照，连接随会话停留在
                # "idle in transaction" 状态，快照保持不变。
                conn.execute(
                    "START TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"
                )
                # pg_export_snapshot 返回当前事务快照 ID，
                # 同时确认快照已在事务内建立（RR 下首条语句即冻结快照）。
                row = conn.execute(
                    "SELECT now() AS snapshot_time, "
                    "pg_export_snapshot() AS snapshot_id"
                ).fetchone()
                session.snapshot_time = row["snapshot_time"]
                session.snapshot_id = row["snapshot_id"]
            except Exception:
                conn.rollback()
                self._pool.putconn(conn)
                raise
            self._registry[session.session_id] = session
            return session

    # ------------------------------------------------------------------
    # 取页
    # ------------------------------------------------------------------
    def fetch_page(self, session_id: str, page: int, page_size: int):
        """在会话快照上取第 ``page`` 页（从 1 开始），claim_id 升序。"""
        session = self._get(session_id)

        with session.state_lock:
            if session.status != "open":
                raise SessionUnavailable(
                    "session_not_open",
                    f"会话 {session_id} 状态为 {session.status}，"
                    "关闭已受理或会话已结束，取页失败且不返回明细",
                    http_status=409,
                )
            if _utcnow() >= session.expires_at:
                # 到期瞬间、清理线程尚未接手：直接拒绝，清理稍后完成。
                raise SessionUnavailable(
                    "session_expired",
                    f"会话 {session_id} 已到期，取页失败且不返回明细",
                    http_status=410,
                )
            session.in_flight += 1

        try:
            # io_lock 保证关闭流程与查询不会同时操作同一条连接。
            with session.io_lock:
                # 拿到连接使用权后再复核一次状态：关闭可能在等锁期间受理。
                with session.state_lock:
                    if session.status != "open":
                        raise SessionUnavailable(
                            "session_not_open",
                            f"会话 {session_id} 关闭已受理，"
                            "该取页尚未开始响应，已失败且不返回明细",
                            http_status=409,
                        )
                rows, has_more = self._query_page(session, page, page_size)
                with session.state_lock:
                    session.pages_served += 1
                    session.last_page_at = _utcnow()
                return rows, has_more
        except SessionUnavailable:
            raise
        except psycopg.Error as exc:
            # 连接丢失等数据库错误：会话作废，绝不换连接/换快照续取。
            # _teardown 同样遵守"归还成功才进终态"：若此刻连接池也不可用，
            # 会话停在 closing，由 sweeper/重复关闭继续重试收尾。
            self._teardown(session, reason="connection_lost")
            raise SessionError(
                "session_broken",
                f"数据库连接或快照失效，会话 {session_id} 已作废，"
                f"不会换快照续取：{exc.sqlstate_name if hasattr(exc, 'sqlstate_name') else exc}",
                http_status=503,
            )
        finally:
            with session.state_lock:
                session.in_flight -= 1

    def _query_page(self, session: Session, page: int, page_size: int):
        # 多取一行判断 has_more；OFFSET 分页基于快照内固定结果集，
        # 重复取同一页得到完全相同的数据。
        offset = (page - 1) * page_size
        sql = """
            SELECT c.id          AS claim_id,
                   c.policy_id   AS policy_id,
                   p.region      AS region,
                   c.amount      AS amount
              FROM claims c
              JOIN policies p ON p.id = c.policy_id
             WHERE p.region = %s
             ORDER BY c.id ASC
             LIMIT %s OFFSET %s
        """
        rows = session.conn.execute(
            sql, (session.region, page_size + 1, offset)
        ).fetchall()
        has_more = len(rows) > page_size
        if has_more:
            rows = rows[:page_size]
        return rows, has_more

    # ------------------------------------------------------------------
    # 关闭 / 到期
    # ------------------------------------------------------------------
    def close(self, session_id: str, reason: str = "closed") -> Session:
        """受理关闭并尝试收尾。

        - open 会话：第一时间翻成 closing 并记录终态原因，新取页立即失败；
        - 已在 closing 的会话（含并发重复关闭、sweeper 重试）：不覆盖原有
          终态原因，仅再尝试一次收尾；
        - 已到终态：幂等返回。
        收尾若归还连接失败，会话保持 closing 且不释放名额，调用方可稍后重试。
        """
        session = self._get(session_id)
        with session.state_lock:
            if session.status in ("closed", "expired"):
                return session
            if session.status == "open":
                # 第一时间受理：此后任何取页请求都失败。
                session.status = "closing"
                session.close_reason = reason
        # 等待在途取页结束（它们持有 io_lock），然后独占连接做清理。
        # 收尾失败不抛出：状态停留 closing，交由重复关闭/sweeper 续做。
        self._teardown(session, reason=reason)
        return session

    def _teardown(self, session: Session, reason: str) -> None:
        """单次收尾尝试，可安全重入、可安全重试。

        成功（连接已归还）才把状态翻成 closed/expired；任一步失败则保持
        closing 并记录原因，下一次 close/sweeper 巡检继续收尾。
        ``io_lock`` 串行化所有收尾与取页；``conn_returned`` 保证同一条
        连接在任何并发交错下最多归还一次。
        """
        with session.io_lock:
            with session.state_lock:
                if session.status in ("closed", "expired"):
                    return
                # 首次受理（例如取页路径上的 connection_lost）也要落原因。
                if session.status == "open":
                    session.status = "closing"
                    session.close_reason = reason
                elif session.close_reason is None:
                    session.close_reason = reason
                if session.conn_returned:
                    # 防御性分支：连接已还但状态未翻（正常不会发生）。
                    self._finalize(session)
                    return
                session.teardown_attempts += 1
                attempt = session.teardown_attempts

            # 1) 回滚快照事务（只做一次；失败不致命，归还时池的 reset
            #    还会再尝试回滚；连接本身断裂时 rollback 报错也照常进入
            #    归还步骤，由连接池决定丢弃并补建）。
            if not session.rollback_done:
                try:
                    session.conn.rollback()
                    session.rollback_done = True
                except psycopg.Error as exc:
                    with session.state_lock:
                        session.last_teardown_error = f"rollback 失败: {exc!r}"
                        session.last_teardown_at = _utcnow()

            # 2) 归还连接。失败 -> 保持 closing，不进终态、不释放名额。
            try:
                self._return_conn(session)
            except Exception as exc:
                with session.state_lock:
                    session.last_teardown_error = f"连接归还失败: {exc!r}"
                    session.last_teardown_at = _utcnow()
                print(
                    f"[teardown] 会话 {session.session_id} 第 {attempt} 次收尾"
                    f"归还连接失败，保持 closing 待重试: {exc!r}",
                    flush=True,
                )
                return

            # 3) 连接确实归还：置位并翻终态，名额自此开放。
            with session.state_lock:
                session.conn_returned = True
                session.last_teardown_error = None
                session.last_teardown_at = _utcnow()
                self._finalize(session)

    @staticmethod
    def _finalize(session: Session) -> None:
        """state_lock 内调用：按受理原因落终态。"""
        if session.status in ("closed", "expired"):
            return
        session.status = (
            "expired" if session.close_reason == "expired" else "closed"
        )

    def _sweep_loop(self) -> None:
        while not self._stop_event.wait(self._settings.sweeper_interval_seconds):
            try:
                self._sweep_once()
            except Exception as exc:
                # 清理线程永不因单次异常（含数据库连接丢失）退出。
                print(f"[sweeper] 本轮清理失败，将继续重试: {exc!r}", flush=True)

    def _sweep_once(self) -> None:
        now = _utcnow()
        with self._registry_lock:
            due = [
                s for s in list(self._registry.values())
                if s.status == "open" and now >= s.expires_at
            ]
            # 上一轮收尾归还失败、停在 closing 的会话：无论是否到期都重试，
            # 连接池恢复后即可完成收尾、落终态并释放名额。
            pending = [
                s for s in list(self._registry.values()) if s.status == "closing"
            ]
            # 终态记录保留 10 分钟供状态查询，之后回收注册表项。
            stale = [
                s for s in list(self._registry.values())
                if s.status in ("closed", "expired")
                and now - s.expires_at > timedelta(minutes=10)
            ]
        for session in due:
            # close 内部失败不抛、保持 closing，会进入下一轮 pending 重试。
            self.close(session.session_id, reason="expired")
        for session in pending:
            # 不覆盖首次受理原因（主动关闭仍是 closed，到期仍是 expired）。
            self.close(session.session_id)
        with self._registry_lock:
            for session in stale:
                self._registry.pop(session.session_id, None)

    # ------------------------------------------------------------------
    # 查询
    # ------------------------------------------------------------------
    def _get(self, session_id: str) -> Session:
        with self._registry_lock:
            session = self._registry.get(session_id)
        if session is None:
            raise SessionNotFound(session_id)
        return session

    def get_status(self, session_id: str) -> Session:
        return self._get(session_id)

    def list_sessions(self) -> list[Session]:
        with self._registry_lock:
            return list(self._registry.values())

    def pool(self) -> ConnectionPool:
        if self._pool is None:
            raise SessionError(
                "not_started", "服务尚未启动", http_status=503
            )
        return self._pool

    def ttl_seconds(self) -> int:
        return self._settings.session_ttl_seconds

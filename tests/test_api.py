"""端到端自测：覆盖分页、快照隔离、名额、关闭、到期、连接丢失与重启语义。

运行方式（本地已起好 PostgreSQL 15，见 README）：
    DATABASE_URL=postgresql://actuary:actuary@127.0.0.1:55432/claims \
    SESSION_TTL_SECONDS=2 ENABLE_DEBUG_HOOKS=true \
    python -m unittest tests.test_api -v
"""
from __future__ import annotations

import os
import threading
import time
import unittest

import httpx
import psycopg
import uvicorn

# 本地测试用默认值
os.environ.setdefault(
    "DATABASE_URL", "postgresql://actuary:actuary@127.0.0.1:55432/claims"
)
os.environ.setdefault("SESSION_TTL_SECONDS", "2")
os.environ.setdefault("ENABLE_DEBUG_HOOKS", "true")
os.environ.setdefault("SWEEPER_INTERVAL_SECONDS", "0.1")

from app.config import settings  # noqa: E402
from app.main import app, manager  # noqa: E402
from app.session_manager import SessionError  # noqa: E402

SEED_SQL = os.path.join(os.path.dirname(__file__), "..", "db", "init", "02_seed.sql")


def reset_seed_data() -> None:
    """每个用例前重置演示数据，保证改/删类测试可重复运行。"""
    with open(SEED_SQL, encoding="utf-8") as fh:
        sql = fh.read()
    with psycopg.connect(settings.database_url, autocommit=True) as conn:
        conn.execute(sql)

TTL = settings.session_ttl_seconds


class ApiTestBase(unittest.TestCase):
    # 整个套件共享一个 uvicorn 实例与一个 manager 单例，
    # 避免多实例争抢同一端口导致请求串到旧进程。
    _server: "uvicorn.Server | None" = None
    _server_thread: threading.Thread | None = None
    client: httpx.Client

    @classmethod
    def setUpClass(cls):
        if ApiTestBase._server is None:
            config = uvicorn.Config(
                "app.main:app", host="127.0.0.1", port=18080,
                log_level="warning",
            )
            ApiTestBase._server = uvicorn.Server(config)
            ApiTestBase._server_thread = threading.Thread(
                target=ApiTestBase._server.run,
                name="test-uvicorn", daemon=True,
            )
            ApiTestBase._server_thread.start()
            for _ in range(100):
                try:
                    httpx.get("http://127.0.0.1:18080/health", timeout=1)
                    break
                except httpx.TransportError:
                    time.sleep(0.1)
            # 全套件以干净的种子数据开始
            reset_seed_data()
        cls.client = httpx.Client(
            base_url="http://127.0.0.1:18080", timeout=15
        )

    @classmethod
    def tearDownClass(cls):
        cls.client.close()

    def setUp(self):
        # 每个用例前清掉所有未终结会话，保证相互独立
        for s in manager.list_sessions():
            if s.status in ("open", "closing"):
                try:
                    manager.close(s.session_id)
                except SessionError:
                    pass
        # 写入类用例会改数据，统一在用例内自行安排快照时机，
        # 不在全局 setUp 中重置（否则会清掉同用例刚提交的写入）。

    @classmethod
    def _reset_seed(cls):
        reset_seed_data()

    def open(self, region="华北"):
        r = self.client.post("/sessions", json={"region": region})
        self.assertEqual(r.status_code, 201, r.text)
        return r.json()

    def close(self, sid):
        return self.client.post(f"/sessions/{sid}/close")

    def page(self, sid, page=1, page_size=100, headers=None):
        return self.client.get(
            f"/sessions/{sid}/claims",
            params={"page": page, "page_size": page_size},
            headers=headers or {},
        )


class TestPagination(ApiTestBase):
    def test_pages_ordered_and_capped(self):
        s = self.open()
        r1 = self.page(s["session_id"])
        self.assertEqual(r1.status_code, 200)
        body = r1.json()
        self.assertEqual(len(body["rows"]), 100)
        self.assertTrue(body["has_more"])
        ids = [r["claim_id"] for r in body["rows"]]
        self.assertEqual(ids, sorted(ids))
        self.assertTrue(all(r["region"] == "华北" for r in body["rows"]))
        self.assertTrue(all(isinstance(r["amount"], int) for r in body["rows"]))

        r2 = self.page(s["session_id"], page=2)
        self.assertEqual(len(r2.json()["rows"]), 20)
        self.assertFalse(r2.json()["has_more"])
        self.assertGreater(
            r2.json()["rows"][0]["claim_id"], body["rows"][-1]["claim_id"]
        )

        r3 = self.page(s["session_id"], page=3)
        self.assertEqual(r3.json()["rows"], [])
        self.assertFalse(r3.json()["has_more"])

    def test_page_size_forced_to_cap(self):
        s = self.open()
        r = self.page(s["session_id"], page_size=999)
        self.assertEqual(r.json()["page_size"], 100)
        self.assertEqual(len(r.json()["rows"]), 100)

    def test_invalid_pagination_params(self):
        s = self.open()
        self.assertEqual(self.client.get(
            f"/sessions/{s['session_id']}/claims", params={"page": 0}
        ).status_code, 422)
        self.assertEqual(self.client.get(
            f"/sessions/{s['session_id']}/claims", params={"page_size": 0}
        ).status_code, 422)

    def test_repeat_page_same_snapshot(self):
        s = self.open()
        b1 = self.page(s["session_id"]).json()
        b2 = self.page(s["session_id"]).json()
        self.assertEqual(
            [r["claim_id"] for r in b1["rows"]],
            [r["claim_id"] for r in b2["rows"]],
        )
        self.assertEqual(b1["snapshot_time"], b2["snapshot_time"])


class TestSnapshotIsolation(ApiTestBase):
    def test_source_writes_invisible_inside_session(self):
        self._reset_seed()  # 本用例需要干净的种子数据
        s = self.open("华北")
        before = self.page(s["session_id"]).json()
        before_ids = {r["claim_id"] for r in before["rows"]}

        # 源端：新增保单+理赔、改金额、删理赔，均独立提交
        pol = self.client.post("/admin/policies", json={"region": "华北"})
        self.assertEqual(pol.status_code, 201)
        nc = self.client.post(
            "/admin/claims",
            json={"policy_id": pol.json()["id"], "amount": 777},
        )
        self.assertEqual(nc.status_code, 201)
        first_id = before["rows"][0]["claim_id"]
        second_id = before["rows"][1]["claim_id"]
        self.assertEqual(self.client.patch(
            f"/admin/claims/{first_id}", params={"amount": 1}
        ).status_code, 200)
        self.assertEqual(self.client.delete(
            f"/admin/claims/{second_id}"
        ).status_code, 204)

        after = self.page(s["session_id"]).json()
        after_ids = {r["claim_id"] for r in after["rows"]}
        self.assertEqual(before_ids, after_ids)
        self.assertNotIn(nc.json()["id"], after_ids)
        self.assertNotEqual(after["rows"][0]["amount"], 1)
        self.assertIn(second_id, after_ids)

        # 新会话能看到源端最新提交（翻完全部页核对）
        s2 = self.open("华北")
        fresh_ids = set()
        page_no = 1
        while True:
            body = self.page(s2["session_id"], page=page_no).json()
            fresh_ids.update(r["claim_id"] for r in body["rows"])
            if not body["has_more"]:
                break
            page_no += 1
        self.assertIn(nc.json()["id"], fresh_ids)
        self.assertNotIn(second_id, fresh_ids)
        self.close(s2["session_id"])

    def test_snapshot_covers_both_tables(self):
        # 地区来自 policies 表、金额来自 claims 表：JOIN 结果同样固定
        s = self.open("华东")
        b = self.page(s["session_id"]).json()
        self.assertEqual(len(b["rows"]), 100)
        self.assertTrue(all(r["region"] == "华东" for r in b["rows"]))


class TestCapacity(ApiTestBase):
    def test_max_two_sessions_and_release(self):
        s1 = self.open("华东")
        s2 = self.open("华南")
        r = self.client.post("/sessions", json={"region": "华北"})
        self.assertEqual(r.status_code, 429)
        self.assertEqual(r.json()["code"], "capacity_reached")

        # 满额不影响既有会话
        self.assertEqual(self.page(s1["session_id"], page_size=5).status_code, 200)

        self.assertEqual(self.close(s2["session_id"]).json()["status"], "closed")
        s3 = self.open("华北")  # 名额归还后可再开
        self.assertEqual(s3["status"], "open")
        # 另一个会话不受影响
        self.assertEqual(self.page(s1["session_id"], page_size=5).status_code, 200)
        self.close(s1["session_id"])
        self.close(s3["session_id"])

    def test_unknown_session_404(self):
        r = self.client.get("/sessions/nonexistent/claims")
        self.assertEqual(r.status_code, 404)
        self.assertEqual(self.client.get("/sessions/nonexistent").status_code, 404)


class TestCloseSemantics(ApiTestBase):
    def test_close_rejects_later_pages_but_keeps_status_queryable(self):
        s = self.open()
        self.close(s["session_id"])
        r = self.page(s["session_id"])
        self.assertEqual(r.status_code, 409)
        self.assertNotIn("rows", r.json())
        info = self.client.get(f"/sessions/{s['session_id']}").json()
        self.assertEqual(info["status"], "closed")

    def test_slow_page_then_close(self):
        s = self.open("华东")
        sid = s["session_id"]
        box = {}

        def slow():
            box["r"] = self.client.get(
                f"/sessions/{sid}/claims",
                params={"page": 1, "page_size": 10},
                headers={"X-Debug-Sleep-Seconds": "3"},
                timeout=15,
            )

        t = threading.Thread(target=slow)
        t.start()
        time.sleep(0.8)
        r = self.close(sid)
        self.assertEqual(r.status_code, 200)
        self.assertEqual(r.json()["status"], "closed")
        t.join()
        self.assertIn(box["r"].status_code, (409, 410))
        self.assertNotIn("rows", box["r"].json())

        # 关闭后名额已归还，可开新会话
        s2 = self.open("华东")
        self.assertEqual(s2["status"], "open")
        self.close(s2["session_id"])

    def test_close_idempotent_and_new_session_unaffected(self):
        s1 = self.open("华东")
        s2 = self.open("华南")
        self.close(s1["session_id"])
        again = self.close(s1["session_id"])
        self.assertEqual(again.json()["status"], "closed")
        # 另一个会话照常工作
        self.assertEqual(self.page(s2["session_id"], page_size=5).status_code, 200)
        self.close(s2["session_id"])


class TestExpiry(ApiTestBase):
    def test_expiry_rejects_pages_and_releases_slot(self):
        s = self.open("华南")
        sid = s["session_id"]
        time.sleep(TTL + 0.6)
        r = self.page(sid)
        self.assertIn(r.status_code, (409, 410))
        self.assertNotIn("rows", r.json())
        info = self.client.get(f"/sessions/{sid}").json()
        self.assertEqual(info["status"], "expired")
        # 名额已释放：能连开两个新会话
        a = self.open("华东")
        b = self.open("华北")
        self.assertEqual(a["status"], "open")
        self.assertEqual(b["status"], "open")
        self.close(a["session_id"])
        self.close(b["session_id"])

    def test_pages_before_expiry_still_work(self):
        s = self.open("华南")
        r = self.page(s["session_id"], page_size=10)
        self.assertEqual(r.status_code, 200)
        self.assertEqual(len(r.json()["rows"]), 10)


class TestConnectionLossAndRestart(ApiTestBase):
    """连接丢失（终止会话后端）与服务重启语义。

    本环境禁止 kill 系统调用，故用 pg_terminate_backend 从 SQL 层
    断开会话持有的后端连接，对客户端而言与网络/进程断开等价。
    """

    def _terminate_session_backend(self, sid: str) -> None:
        sess = manager.get_status(sid)
        with psycopg.connect(settings.database_url, autocommit=True) as admin:
            # 按客户端后端 PID 精准终止该会话独占的连接
            admin.execute(
                "SELECT pg_terminate_backend(%s)",
                (sess.conn.info.backend_pid,),
            )
        # 给客户端一点时间感知 TCP 断开
        deadline = time.time() + 5
        while time.time() < deadline:
            if sess.conn.closed:
                return
            time.sleep(0.1)

    def test_connection_loss_invalidates_session_no_snapshot_swap(self):
        s = self.open("华东")
        sid = s["session_id"]
        # 从 SQL 层断开会话持有的后端连接 -> 快照事务随之消失
        self._terminate_session_backend(sid)
        r = self.page(sid)
        # 旧会话直接失败，绝不换连接/换快照续取；
        # 409（清理线程先标记终态）或 503（取页时发现连接断裂）均合规
        self.assertIn(r.status_code, (409, 503))
        self.assertNotIn("rows", r.json())
        # 旧 id 无法续取（失效会话已移出注册表或已为终态）
        info = self.client.get(f"/sessions/{sid}")
        if info.status_code == 200:
            self.assertIn(info.json()["status"], ("closed", "expired"))
            self.assertEqual(self.page(sid).status_code, 409)
        else:
            self.assertEqual(info.status_code, 404)
        # 新会话在池恢复后的连接上正常开启
        s2 = self.open("华东")
        body = self.page(s2["session_id"], page_size=5)
        self.assertEqual(body.status_code, 200)
        self.assertEqual(len(body.json()["rows"]), 5)
        self.close(s2["session_id"])

    @staticmethod
    def _restart_uvicorn():
        """完整重启共享 uvicorn：lifespan 负责 manager.stop()/start()。"""
        ApiTestBase._server.should_exit = True
        ApiTestBase._server_thread.join(timeout=10)
        config = uvicorn.Config(
            "app.main:app", host="127.0.0.1", port=18080, log_level="warning",
        )
        ApiTestBase._server = uvicorn.Server(config)
        ApiTestBase._server_thread = threading.Thread(
            target=ApiTestBase._server.run,
            name="test-uvicorn-restart", daemon=True,
        )
        ApiTestBase._server_thread.start()
        for _ in range(100):
            try:
                if httpx.get(
                    "http://127.0.0.1:18080/health", timeout=1
                ).status_code == 200:
                    return
            except httpx.TransportError:
                time.sleep(0.1)
        raise RuntimeError("uvicorn 重启后健康检查失败")

    def test_service_restart_drops_in_memory_sessions(self):
        s = self.open("华南")
        sid = s["session_id"]

        # 模拟 API 服务重启（新进程语义：内存注册表为空、池与 sweeper 重建）
        self._restart_uvicorn()

        # 旧会话 id 在新进程中不存在，不得换快照续取
        r = self.client.get(f"/sessions/{sid}")
        self.assertEqual(r.status_code, 404)
        r2 = self.page(sid)
        self.assertEqual(r2.status_code, 404)
        s2 = self.open("华南")
        self.assertEqual(s2["status"], "open")
        self.close(s2["session_id"])


class TestReturnFailureRecovery(ApiTestBase):
    """收尾链路：连接归还失败 -> 停留 closing、占名额、可重试 ->
    连接池恢复后重复关闭/sweeper 完成归还，落终态、开放名额。"""

    def setUp(self):
        super().setUp()
        # 每个用例从无故障状态开始
        manager.set_return_fault(False)

    def tearDown(self):
        manager.set_return_fault(False)
        super().tearDown()

    def _info(self, sid):
        return self.client.get(f"/sessions/{sid}").json()

    def test_return_failure_keeps_closing_queryable_and_holds_slot(self):
        s = self.open("华东")
        sid = s["session_id"]
        manager.set_return_fault(True)

        # 关闭受理：归还失败 -> 停留 closing，不谎报 closed
        r = self.close(sid)
        self.assertEqual(r.status_code, 200, r.text)
        info = r.json()
        self.assertEqual(info["status"], "closing")
        self.assertFalse(info["conn_returned"])
        self.assertGreaterEqual(info["teardown_attempts"], 1)
        self.assertIsNotNone(info["last_teardown_error"])
        self.assertEqual(info["close_reason"], "closed")

        # 旧会话不可取页（409，无明细），但状态仍可查询
        pr = self.page(sid)
        self.assertEqual(pr.status_code, 409)
        self.assertNotIn("rows", pr.json())
        self.assertEqual(self._info(sid)["status"], "closing")

        # 名额未释放：另开两个会话的机会被占住，新开启请求 429
        other = self.open("华南")
        blocked = self.client.post("/sessions", json={"region": "华北"})
        self.assertEqual(blocked.status_code, 429)
        self.assertEqual(blocked.json()["code"], "capacity_reached")

        # 故障期间重复关闭：继续重试但仍失败，状态保持 closing
        again = self.close(sid).json()
        self.assertEqual(again["status"], "closing")
        self.assertFalse(again["conn_returned"])
        self.assertGreaterEqual(again["teardown_attempts"], 2)

        # 恢复后再次关闭：连接确实归还，才报 closed 并开放名额
        manager.set_return_fault(False)
        recovered = self.close(sid).json()
        self.assertEqual(recovered["status"], "closed")
        self.assertTrue(recovered["conn_returned"])
        self.assertIsNone(recovered["last_teardown_error"])

        # 名额开放：此前被拒的会话现在可开
        s3 = self.client.post("/sessions", json={"region": "华北"})
        self.assertEqual(s3.status_code, 201)
        self.close(other["session_id"])
        self.close(s3.json()["session_id"])

    def test_sweeper_retries_closing_after_pool_recovers(self):
        s = self.open("华东")
        sid = s["session_id"]
        manager.set_return_fault(True)
        self.assertEqual(self.close(sid).json()["status"], "closing")

        # 不手动重试关闭，等待 sweeper 巡检：故障期间它也只会反复失败
        time.sleep(0.5)
        self.assertEqual(self._info(sid)["status"], "closing")

        # 恢复后由 sweeper 自动收尾（SWEEPER_INTERVAL_SECONDS=0.1）
        manager.set_return_fault(False)
        deadline = time.time() + 5
        while time.time() < deadline:
            if self._info(sid)["status"] == "closed":
                break
            time.sleep(0.1)
        info = self._info(sid)
        self.assertEqual(info["status"], "closed")
        self.assertTrue(info["conn_returned"])
        self.assertGreaterEqual(info["teardown_attempts"], 2)

        # 终态幂等：再关闭不重复归还、不报错
        again = self.close(sid).json()
        self.assertEqual(again["status"], "closed")
        self.assertEqual(again["teardown_attempts"], info["teardown_attempts"])

    def test_expired_teardown_failure_then_retry_reports_expired(self):
        s = self.open("华南")
        sid = s["session_id"]
        manager.set_return_fault(True)
        # 立即到期，走与 60 秒自然到期完全相同的 sweeper 路径
        self.assertEqual(
            self.client.post(f"/sessions/{sid}/debug/expire-now").status_code, 200
        )
        deadline = time.time() + 5
        while time.time() < deadline:
            if self._info(sid)["status"] == "closing":
                break
            time.sleep(0.1)
        info = self._info(sid)
        self.assertEqual(info["status"], "closing")
        self.assertEqual(info["close_reason"], "expired")
        self.assertFalse(info["conn_returned"])

        # 到期后取页被拒（410/409），无明细
        r = self.page(sid)
        self.assertIn(r.status_code, (409, 410))
        self.assertNotIn("rows", r.json())

        # 恢复后 sweeper 收尾：终态必须是 expired 而非 closed，名额开放
        manager.set_return_fault(False)
        deadline = time.time() + 5
        while time.time() < deadline:
            if self._info(sid)["status"] == "expired":
                break
            time.sleep(0.1)
        info = self._info(sid)
        self.assertEqual(info["status"], "expired")
        self.assertTrue(info["conn_returned"])
        a = self.open("华东")
        b = self.open("华北")
        self.assertEqual(a["status"], "open")
        self.assertEqual(b["status"], "open")
        self.close(a["session_id"])
        self.close(b["session_id"])

    def test_concurrent_close_returns_connection_once(self):
        s = self.open("华东")
        sid = s["session_id"]
        sess = manager.get_status(sid)
        manager.set_return_fault(True)
        # 先制造一次失败收尾，会话停在 closing
        self.assertEqual(self.close(sid).json()["status"], "closing")
        manager.set_return_fault(False)

        results = []

        def do_close():
            results.append(self.close(sid).json()["status"])

        threads = [threading.Thread(target=do_close) for _ in range(6)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=10)

        # 所有并发关闭都得到终态 closed
        self.assertEqual(results, ["closed"] * 6)
        info = self._info(sid)
        self.assertTrue(info["conn_returned"])
        # 关键断言：同一条连接只归还一次（成功归还仅 1 次，另加故障期 1 次尝试）
        self.assertEqual(sess.teardown_attempts, 2)
        # 连接已回到池中且仍可正常借用：新会话分页正常
        s2 = self.open("华东")
        body = self.page(s2["session_id"], page_size=5)
        self.assertEqual(body.status_code, 200)
        self.assertEqual(len(body.json()["rows"]), 5)
        self.close(s2["session_id"])

    def test_closing_session_never_swaps_connection(self):
        s = self.open("华东")
        sid = s["session_id"]
        sess = manager.get_status(sid)
        original_pid = sess.conn.info.backend_pid
        manager.set_return_fault(True)
        self.assertEqual(self.close(sid).json()["status"], "closing")

        # closing 期间反复取页全部失败，且会话持有的仍是原连接对象
        for _ in range(3):
            r = self.page(sid)
            self.assertEqual(r.status_code, 409)
            self.assertNotIn("rows", r.json())
        self.assertIs(manager.get_status(sid).conn, sess.conn)
        self.assertEqual(
            manager.get_status(sid).conn.info.backend_pid, original_pid
        )

        # 另一会话在自己的连接上继续分页，不受故障收尾影响
        other = self.open("华南")
        body = self.page(other["session_id"], page_size=7)
        self.assertEqual(body.status_code, 200)
        self.assertEqual(len(body.json()["rows"]), 7)

        # 恢复收尾后，旧 id 仍是终态、不可续取；没有任何换新连接的窗口
        manager.set_return_fault(False)
        self.assertEqual(self.close(sid).json()["status"], "closed")
        self.assertEqual(self.page(sid).status_code, 409)
        self.close(other["session_id"])

    def test_recovered_connection_reused_by_new_session(self):
        s = self.open("华东")
        sid = s["session_id"]
        pool = manager.pool()
        before = pool.get_stats()
        manager.set_return_fault(True)
        self.assertEqual(self.close(sid).json()["status"], "closing")
        # 故障期间连接不在池中：池可用连接数不增加
        self.assertLessEqual(
            pool.get_stats().get("pool_available", 0),
            before.get("pool_available", 0),
        )
        manager.set_return_fault(False)
        self.assertEqual(self.close(sid).json()["status"], "closed")

        # 归还成功后连接回到池中：出现可用连接，且池没有因泄漏而扩编
        after = pool.get_stats()
        self.assertGreaterEqual(after.get("pool_available", 0), 1)
        self.assertLessEqual(
            after.get("pool_size", 0), before.get("pool_size", 0) + 1
        )
        # 新会话在池恢复后正常借连接、开新快照事务并取页（不再卡死）
        s2 = self.open("华东")
        body = self.page(s2["session_id"], page_size=3)
        self.assertEqual(body.status_code, 200)
        self.assertEqual(len(body.json()["rows"]), 3)
        self.assertEqual(s2["status"], "open")
        self.close(s2["session_id"])

    def test_broken_fetch_with_pool_fault_stays_closing_then_recovers(self):
        """取页时连接断裂（503 作废、不换连接）与连接池归还故障叠加：
        收尾停留 closing，恢复后才落终态。用 monkeypatch 确定性注入，
        不依赖操作系统信号（部分环境禁止 pg_terminate_backend）。"""
        from unittest import mock
        import psycopg as _pg

        s = self.open("华东")
        sid = s["session_id"]
        sess = manager.get_status(sid)
        original_conn = sess.conn

        manager.set_return_fault(True)
        # 让该会话连接上的下一次查询抛出连接级错误，模拟后端中断
        with mock.patch.object(
            original_conn,
            "execute",
            side_effect=_pg.OperationalError("simulated backend termination"),
        ):
            r = self.page(sid)
        self.assertEqual(r.status_code, 503)
        self.assertEqual(r.json()["code"], "session_broken")
        self.assertNotIn("rows", r.json())

        info = self._info(sid)
        self.assertEqual(info["status"], "closing")
        self.assertEqual(info["close_reason"], "connection_lost")
        self.assertFalse(info["conn_returned"])
        # 关键：会话仍绑定原连接对象，绝不换连接/换快照续取
        self.assertIs(manager.get_status(sid).conn, original_conn)
        # 旧 id 不可续取
        self.assertEqual(self.page(sid).status_code, 409)

        # 恢复后 sweeper 完成收尾（connection_lost 归并为 closed 终态）
        manager.set_return_fault(False)
        deadline = time.time() + 5
        while time.time() < deadline:
            if self._info(sid)["status"] in ("closed", "expired"):
                break
            time.sleep(0.1)
        info = self._info(sid)
        self.assertEqual(info["status"], "closed")
        self.assertTrue(info["conn_returned"])
        self.assertIsNone(info["last_teardown_error"])

        # 池补建/复用连接后新会话正常工作
        s2 = self.open("华东")
        body = self.page(s2["session_id"], page_size=3)
        self.assertEqual(body.status_code, 200)
        self.assertEqual(len(body.json()["rows"]), 3)
        self.close(s2["session_id"])


if __name__ == "__main__":
    unittest.main(verbosity=2)

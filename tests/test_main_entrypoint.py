import importlib
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import Mock, patch

from miniqmt_follower import app
from miniqmt_follower.models import Action, ExecutionResult, ExecutionStatus, TradeSignal
from miniqmt_follower.opening import OpeningSellBarrier
from miniqmt_follower.redis_stream import StreamMessage


def make_signal(action: Action, created_at: str, signal_id: str) -> TradeSignal:
    return TradeSignal(
        signal_id=signal_id,
        strategy_id="harvester",
        action=action,
        code="000001.XSHE",
        amount=1000,
        reference_price=10.0,
        created_at=created_at,
    )


class BlockingProbeEngine:
    def __init__(self):
        self.started = threading.Event()

    def execute(self, signal):
        self.started.set()
        return ExecutionResult(
            signal_id=signal.signal_id,
            status=ExecutionStatus.FILLED,
            requested_qty=signal.amount,
            filled_qty=signal.amount,
            attempts=1,
            message="filled",
        )


class FailingProbeEngine:
    def execute(self, _signal):
        raise RuntimeError("boom")


class MainEntrypointTests(unittest.TestCase):
    def setUp(self):
        patcher = patch.object(app, "seconds_until_market_open", return_value=0.0)
        patcher.start()
        self.addCleanup(patcher.stop)

    def test_preopen_buy_waits_for_market_open_even_without_registered_sells(self):
        barrier = OpeningSellBarrier()
        buy = make_signal(Action.BUY, "2026-08-03 09:29:00", "buy-preopen")
        engine = BlockingProbeEngine()

        with (
            patch.object(
                app, "seconds_until_market_open", return_value=0.05, create=True,
            ),
            ThreadPoolExecutor(max_workers=1) as pool,
        ):
            future = pool.submit(app._execute_safe, engine, buy, barrier)
            self.assertFalse(engine.started.wait(0.02))
            self.assertTrue(engine.started.wait(0.2))
            future.result(timeout=1)

    def test_buy_waits_until_preopen_sell_finishes(self):
        barrier = OpeningSellBarrier()
        sell = make_signal(Action.SELL, "2026-08-03 09:27:00", "sell-1")
        buy = make_signal(Action.BUY, "2026-08-03 09:30:00", "buy-1")
        barrier.register(sell)
        engine = BlockingProbeEngine()
        with ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(app._execute_safe, engine, buy, barrier)
            self.assertFalse(engine.started.wait(0.05))
            barrier.release("sell-1")
            self.assertTrue(engine.started.wait(0.5))
            future.result(timeout=1)

    def test_preopen_sell_releases_barrier_even_when_engine_returns_failure(self):
        barrier = OpeningSellBarrier()
        sell = make_signal(Action.SELL, "2026-08-03 09:27:00", "sell-1")
        barrier.register(sell)

        result = app._execute_safe(FailingProbeEngine(), sell, barrier)

        self.assertEqual(result.status, ExecutionStatus.FAILED_BROKER)
        self.assertEqual(barrier.pending_count(), 0)

    def test_trade_workers_default_covers_strategy_target_count(self):
        # 开盘时段会并行处理多只卖单，屏障放行后也需并发处理多只买单。
        # 默认 8: 可同时容纳 harvester 的 5 只涨停目标股排队，
        # 并给普通买单保留 3 个 worker；买卖方向各自独立线程池。
        self.assertEqual(app._parse_args([]).workers, 8)

    def test_trade_workers_accept_custom_count(self):
        self.assertEqual(app._parse_args(["--workers", "8"]).workers, 8)

    def test_zero_trade_workers_are_rejected(self):
        with self.assertRaises(SystemExit):
            app._parse_args(["--workers", "0"])

    def test_empty_allowlist_accepts_any_strategy(self):
        self.assertTrue(app._strategy_allowed("harvester", set()))
        self.assertTrue(app._strategy_allowed("", set()))

    def test_allowlist_filters_unknown_strategies(self):
        allowed = {"harvester"}
        self.assertTrue(app._strategy_allowed("harvester", allowed))
        self.assertFalse(app._strategy_allowed("hunter", allowed))
        self.assertFalse(app._strategy_allowed("", allowed))

    def test_main_py_delegates_to_runtime_app(self):
        main_module = importlib.import_module("main")

        with patch("miniqmt_follower.app.main") as app_main:
            main_module.main()

        app_main.assert_called_once_with()

    def test_legacy_execute_at_is_ignored_and_trade_submits_immediately(self):
        buy_pool = Mock()
        future = object()
        buy_pool.submit.return_value = future
        pools = {Action.BUY: buy_pool, Action.SELL: Mock()}
        pending = {}
        engine = Mock()
        barrier = OpeningSellBarrier()
        message = self._trade_message("legacy", "2099-01-01 09:30:00")

        app._submit_trade(message, pending, pools, engine, barrier)

        buy_pool.submit.assert_called_once_with(
            app._execute_safe, engine, message.signal, barrier
        )
        self.assertIs(pending[future], message)
        self.assertFalse(hasattr(message.signal, "execute_at"))

    def test_scheduling_helpers_are_removed(self):
        self.assertFalse(hasattr(app, "_queue_or_submit_trade"))
        self.assertFalse(hasattr(app, "_submit_due_scheduled"))
        self.assertFalse(hasattr(app, "_is_opening_handoff"))

    def test_buy_and_sell_are_routed_to_independent_worker_pools(self):
        buy_pool = Mock()
        sell_pool = Mock()
        buy_future = object()
        sell_future = object()
        buy_pool.submit.return_value = buy_future
        sell_pool.submit.return_value = sell_future
        pools = {Action.BUY: buy_pool, Action.SELL: sell_pool}
        pending = {}
        engine = Mock()
        barrier = OpeningSellBarrier()
        sell = self._trade_message("sell", action="sell")
        buy = self._trade_message("buy", action="buy")

        app._submit_trade(sell, pending, pools, engine, barrier)
        app._submit_trade(buy, pending, pools, engine, barrier)

        self.assertEqual(barrier.pending_count(), 1)
        sell_pool.submit.assert_called_once_with(
            app._execute_safe, engine, sell.signal, barrier
        )
        buy_pool.submit.assert_called_once_with(
            app._execute_safe, engine, buy.signal, barrier
        )
        self.assertIs(pending[sell_future], sell)
        self.assertIs(pending[buy_future], buy)

    @staticmethod
    def _trade_message(signal_id, execute_at=None, action="buy"):
        raw = {
            "signal_id": signal_id,
            "strategy_id": "stable_discount_hunter",
            "action": action,
            "code": "510300.XSHG",
            "amount": 1000,
            "reference_price": 4.0,
            "created_at": "2026-07-13 09:25:00",
        }
        if execute_at is not None:
            raw["execute_at"] = execute_at
        return StreamMessage(message_id=signal_id + "-0", signal=TradeSignal.from_dict(raw))


if __name__ == "__main__":
    unittest.main()

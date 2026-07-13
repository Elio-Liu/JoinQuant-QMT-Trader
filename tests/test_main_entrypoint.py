import datetime as dt
import importlib
import unittest
from unittest.mock import Mock, patch

from qmt_follower import app
from qmt_follower.models import TradeSignal
from qmt_follower.redis_stream import StreamMessage


class MainEntrypointTests(unittest.TestCase):
    def test_trade_worker_defaults_to_one(self):
        self.assertEqual(app._parse_args([]).workers, 1)

    def test_parallel_trade_workers_are_rejected(self):
        with self.assertRaises(SystemExit):
            app._parse_args(["--workers", "2"])

    def test_main_py_delegates_to_runtime_app(self):
        main_module = importlib.import_module("main")

        with patch("qmt_follower.app.main") as app_main:
            main_module.main()

        app_main.assert_called_once_with()

    def test_future_trade_waits_until_execute_at(self):
        pool = Mock()
        future = object()
        pool.submit.return_value = future
        scheduled = []
        pending = {}
        message = self._trade_message("scheduled", "2026-07-13 09:30:00")

        app._queue_or_submit_trade(
            message, scheduled, pending, pool, Mock(), dt.datetime(2026, 7, 13, 9, 25)
        )

        pool.submit.assert_not_called()
        self.assertEqual(len(scheduled), 1)
        self.assertEqual(pending, {})

        app._submit_due_scheduled(
            scheduled, pending, pool, Mock(), dt.datetime(2026, 7, 13, 9, 30)
        )

        pool.submit.assert_called_once()
        self.assertEqual(scheduled, [])
        self.assertIs(pending[future], message)

    def test_waiting_trade_does_not_block_immediate_trade(self):
        pool = Mock()
        future = object()
        pool.submit.return_value = future
        scheduled = []
        pending = {}
        engine = Mock()
        now = dt.datetime(2026, 7, 13, 9, 26)

        app._queue_or_submit_trade(
            self._trade_message("scheduled", "2026-07-13 09:30:00"),
            scheduled,
            pending,
            pool,
            engine,
            now,
        )
        immediate = self._trade_message("immediate")
        app._queue_or_submit_trade(immediate, scheduled, pending, pool, engine, now)

        pool.submit.assert_called_once_with(app._execute_safe, engine, immediate.signal)
        self.assertEqual(len(scheduled), 1)
        self.assertIs(pending[future], immediate)

    def test_invalid_execute_at_is_rejected(self):
        with self.assertRaises(ValueError):
            app._queue_or_submit_trade(
                self._trade_message("invalid", "09:30"),
                [],
                {},
                Mock(),
                Mock(),
                dt.datetime(2026, 7, 13, 9, 25),
            )

    @staticmethod
    def _trade_message(signal_id, execute_at=None):
        raw = {
            "signal_id": signal_id,
            "strategy_id": "stable_discount_hunter",
            "action": "buy",
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

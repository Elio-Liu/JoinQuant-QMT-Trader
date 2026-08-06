import logging
import sys
import tempfile
import unittest
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path

import miniqmt_follower.executor as executor_module
from miniqmt_follower.app import _execute_safe
from miniqmt_follower.executor import OrderExecutionEngine
from miniqmt_follower.logging_config import _ColoredFormatter, _FileFormatter, setup_logging
from miniqmt_follower.models import (
    Action,
    BrokerOrderStatus,
    ExecutionConfig,
    ExecutionStatus,
    OrderSnapshot,
    TradeSignal,
)
from miniqmt_follower.opening import OpeningSellBarrier
from miniqmt_follower.store import SQLiteExecutionStore
from tests.test_executor import FakeBroker, FakeMarketData


class TerminalLoggingTests(unittest.TestCase):
    def test_worker_exception_console_message_includes_root_cause(self):
        class ExplodingEngine:
            @staticmethod
            def execute(_signal):
                raise RuntimeError("database is locked")

        signal = self._signal()

        with self.assertLogs("miniqmt_follower.app", level=logging.ERROR) as captured:
            result = _execute_safe(ExplodingEngine(), signal, OpeningSellBarrier())

        self.assertEqual(result.status, ExecutionStatus.FAILED_BROKER)
        self.assertIn("【卖单】❌", captured.records[0].getMessage())
        self.assertIn("database is locked", captured.records[0].getMessage())

    def test_signal_console_prefix_has_single_trade_tag_emoji_and_stable_id(self):
        signal = self._signal()

        self.assertTrue(hasattr(signal, "console_prefix"), "TradeSignal lacks console_prefix")
        self.assertRegex(signal.console_prefix, r"^【卖单】📉 信号#[0-9A-F]{4}$")
        self.assertEqual(signal.console_prefix, self._signal().console_prefix)
        self.assertNotIn("【信号】", signal.console_prefix)

    def test_display_code_shows_name_when_available_and_falls_back_to_code(self):
        named = self._signal().with_stock_name("央企创新ETF")
        self.assertEqual(named.display_code, "央企创新ETF(517110)")
        self.assertEqual(self._signal().display_code, "517110")

    def test_trade_events_use_one_semantic_emoji_without_stacked_event_tags(self):
        signal = self._signal()

        self.assertRegex(signal.console_event("重试"), r"^【卖单】🔁 信号#[0-9A-F]{4}$")
        self.assertRegex(signal.console_event("成交"), r"^【卖单】✅ 信号#[0-9A-F]{4}$")
        self.assertRegex(signal.console_event("风控"), r"^【卖单】⚠️ 信号#[0-9A-F]{4}$")
        self.assertNotIn("【重试】", signal.console_event("重试"))

    def test_console_formatter_keeps_only_time_and_structured_message(self):
        record = logging.LogRecord(
            name="miniqmt_follower.executor",
            level=logging.INFO,
            pathname=__file__,
            lineno=1,
            msg="【卖单】📉 信号#A3F2 | 央企创新ETF(517110) | 13700股",
            args=(),
            exc_info=None,
        )

        formatted = _ColoredFormatter().format(record)

        self.assertRegex(formatted, r"^\d{2}:\d{2}:\d{2} 【卖单】📉")
        self.assertNotIn("[信息]", formatted)
        self.assertNotIn("[executor]", formatted)

    def test_console_formatter_adds_level_emoji_to_unstructured_messages(self):
        record = logging.LogRecord(
            name="miniqmt_follower.test",
            level=logging.WARNING,
            pathname=__file__,
            lineno=1,
            msg="fallback warning",
            args=(),
            exc_info=None,
        )

        formatted = _ColoredFormatter().format(record)

        self.assertRegex(formatted, r"^\d{2}:\d{2}:\d{2} 【警告】⚠️ fallback warning$")

    def test_file_formatter_preserves_exception_traceback(self):
        try:
            raise RuntimeError("boom")
        except RuntimeError:
            record = logging.LogRecord(
                name="miniqmt_follower.app",
                level=logging.ERROR,
                pathname=__file__,
                lineno=1,
                msg="worker failed",
                args=(),
                exc_info=sys.exc_info(),
            )

        formatted = _FileFormatter().format(record)

        self.assertIn("Traceback (most recent call last)", formatted)
        self.assertIn("RuntimeError: boom", formatted)

    def test_setup_logging_keeps_debug_details_in_file_when_console_is_info(self):
        root = logging.getLogger()
        original_handlers = root.handlers[:]
        original_level = root.level
        with tempfile.TemporaryDirectory() as tmpdir:
            try:
                setup_logging(level="INFO", log_dir=tmpdir)
                console = next(handler for handler in root.handlers if type(handler) is logging.StreamHandler)
                file_handler = next(
                    handler for handler in root.handlers if isinstance(handler, TimedRotatingFileHandler)
                )

                self.assertEqual(root.level, logging.DEBUG)
                self.assertEqual(console.level, logging.INFO)
                self.assertEqual(file_handler.level, logging.DEBUG)

                logging.getLogger("miniqmt_follower.test").debug("debug-detail-for-file")
                file_handler.flush()
                text = (Path(tmpdir) / "miniqmt_follower.log").read_text(encoding="utf-8")
                self.assertIn("debug-detail-for-file", text)
            finally:
                for handler in root.handlers:
                    handler.close()
                root.handlers.clear()
                root.handlers.extend(original_handlers)
                root.setLevel(original_level)

    def test_each_unfilled_attempt_is_one_structured_retry_line(self):
        original_auction_clock = executor_module._seconds_until_market_open
        executor_module._seconds_until_market_open = lambda: 0.0
        try:
            with tempfile.TemporaryDirectory() as tmpdir:
                store = SQLiteExecutionStore(Path(tmpdir) / "state.db")
                signal = self._signal()
                config = ExecutionConfig(
                    sell_slippage_pct=0.003,
                    order_timeout_sec=0,
                    max_attempts=2,
                    poll_interval_sec=0,
                )
                broker = FakeBroker(
                    {
                        "order-1": [OrderSnapshot("order-1", BrokerOrderStatus.OPEN, filled_qty=0)],
                        "order-2": [OrderSnapshot("order-2", BrokerOrderStatus.FILLED, filled_qty=13700)],
                    },
                    available_positions={"517110.XSHG": 13700},
                )
                engine = OrderExecutionEngine(
                    store,
                    FakeMarketData(
                        [
                            executor_module.Quote(last_price=0.710, ask1=0.711, bid1=0.710),
                            executor_module.Quote(last_price=0.710, ask1=0.711, bid1=0.710),
                        ],
                        names={"517110.XSHG": "央企创新ETF"},
                    ),
                    broker,
                    config,
                )

                with self.assertLogs("miniqmt_follower.executor", level=logging.INFO) as captured:
                    engine.execute(signal)

                messages = [record.getMessage() for record in captured.records]
                retries = [message for message in messages if "🔁" in message]
                completions = [message for message in messages if "✅" in message]

                self.assertEqual(len(retries), 1)
                self.assertIn("【卖单】🔁", retries[0])
                self.assertIn("央企创新ETF(517110)", retries[0])
                self.assertIn("第01次", retries[0])
                self.assertIn("行情 0.710", retries[0])
                self.assertIn("买/卖 0.710/0.711", retries[0])
                self.assertIn("挂 0.708×13700", retries[0])
                self.assertIn("累计 0/13700", retries[0])
                self.assertIn("未成→已撤", retries[0])
                self.assertEqual(len(completions), 1)
                self.assertIn("【卖单】✅", completions[0])
                self.assertIn("央企创新ETF(517110)", completions[0])
                self.assertIn("第02次", completions[0])
                self.assertIn("全成 13700/13700", completions[0])
                self.assertFalse(any("第1次定价" in message for message in messages))
        finally:
            executor_module._seconds_until_market_open = original_auction_clock

    @staticmethod
    def _signal():
        return TradeSignal(
            signal_id="stress-manual-20260710-01-517110XSHG-sell-13700",
            strategy_id="manual_stress",
            action=Action.SELL,
            code="517110.XSHG",
            amount=13700,
            reference_price=0.710,
            created_at="2026-07-10 09:30:11",
        )


if __name__ == "__main__":
    unittest.main()

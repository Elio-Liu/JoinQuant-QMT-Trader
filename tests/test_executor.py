import tempfile
import unittest
from pathlib import Path

import qmt_follower.executor as executor_module
from qmt_follower.executor import OrderExecutionEngine
from qmt_follower.models import (
    Action,
    BrokerOrderStatus,
    ExecutionConfig,
    ExecutionStatus,
    OrderSnapshot,
    Quote,
    TradeSignal,
)
from qmt_follower.store import SQLiteExecutionStore


def setUpModule():
    # 测试与真实时钟解耦: 若测试恰好在 9:15~9:30 运行, 竞价保护会顺延超时,
    # 破坏 order_timeout_sec=0 的立即超时假设。默认关闭, 竞价用例单独覆盖。
    executor_module._seconds_until_market_open = lambda: 0.0


class FakeMarketData:
    """假行情源。prices 里的元素可以是 float(只有最新价)或 Quote(带盘口)。"""

    def __init__(self, prices):
        self.prices = list(prices)
        self.queries = []

    def latest_quote(self, code):
        self.queries.append(code)
        item = self.prices.pop(0)
        if isinstance(item, Quote):
            return item
        return Quote(last_price=item)


class FakeBroker:
    def __init__(
        self,
        snapshots_by_order,
        *,
        available_cash=1_000_000.0,
        available_positions=None,
        cancel_snapshots_by_order=None,
    ):
        self.snapshots_by_order = snapshots_by_order
        self.cancel_snapshots_by_order = cancel_snapshots_by_order or {}
        self.submitted = []
        self.canceled = []
        self.available_cash = available_cash
        self.available_positions = available_positions or {}
        self.cash_queries = 0
        self.position_queries = []
        self._cancel_requested = set()
        self._last_snapshots = {}

    def query_available_cash(self) -> float:
        self.cash_queries += 1
        return self.available_cash

    def query_available_position(self, code: str) -> int:
        self.position_queries.append(code)
        return self.available_positions.get(code, 10_000)

    def submit_order(self, signal, quantity, price):
        order_id = f"order-{len(self.submitted) + 1}"
        self.submitted.append((order_id, quantity, price))
        return order_id

    def get_order_snapshot(self, order_id):
        if order_id in self._cancel_requested:
            snapshots = self.cancel_snapshots_by_order.get(order_id)
            if snapshots is None:
                last = self._last_snapshots[order_id]
                snapshot = OrderSnapshot(
                    order_id, BrokerOrderStatus.CANCELED, filled_qty=last.filled_qty,
                )
                self._last_snapshots[order_id] = snapshot
                return snapshot
        else:
            snapshots = self.snapshots_by_order[order_id]
        if len(snapshots) > 1:
            snapshot = snapshots.pop(0)
        else:
            snapshot = snapshots[0]
        self._last_snapshots[order_id] = snapshot
        return snapshot

    def cancel_order(self, order_id):
        self.canceled.append(order_id)
        self._cancel_requested.add(order_id)


class ExecutorTests(unittest.TestCase):
    # ---- 原有测试 ----

    def test_partial_fill_reorders_remaining_quantity_with_latest_slippage_price(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            store = SQLiteExecutionStore(Path(tmpdir) / "state.db")
            signal = TradeSignal(
                signal_id="sig-1",
                strategy_id="hunter",
                action=Action.BUY,
                code="000001.XSHE",
                amount=1000,
                reference_price=10.0,
                created_at="2026-06-08 09:30:00",
            )
            config = ExecutionConfig(
                buy_slippage_pct=0.003,
                order_timeout_sec=0,
                max_attempts=2,
                poll_interval_sec=0,
            )
            market_data = FakeMarketData([10.0, 10.1])
            broker = FakeBroker(
                {
                    "order-1": [OrderSnapshot("order-1", BrokerOrderStatus.PARTIALLY_FILLED, filled_qty=400)],
                    "order-2": [OrderSnapshot("order-2", BrokerOrderStatus.FILLED, filled_qty=600)],
                }
            )
            engine = OrderExecutionEngine(store, market_data, broker, config)

            result = engine.execute(signal)

            self.assertEqual(result.status, ExecutionStatus.FILLED)
            self.assertEqual(result.filled_qty, 1000)
            self.assertEqual(broker.canceled, ["order-1"])
            self.assertEqual(broker.submitted, [("order-1", 1000, 10.03), ("order-2", 600, 10.13)])

    def test_sell_retry_refreshes_quote_and_position_before_each_submission(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            store = SQLiteExecutionStore(Path(tmpdir) / "state.db")
            signal = TradeSignal(
                signal_id="sig-sell-refresh",
                strategy_id="hunter",
                action=Action.SELL,
                code="517110.XSHG",
                amount=1000,
                reference_price=0.710,
                created_at="2026-07-10 09:30:00",
            )
            market_data = FakeMarketData([0.710, 0.711])
            broker = FakeBroker(
                {
                    "order-1": [OrderSnapshot("order-1", BrokerOrderStatus.OPEN, filled_qty=0)],
                    "order-2": [OrderSnapshot("order-2", BrokerOrderStatus.FILLED, filled_qty=1000)],
                },
                available_positions={"517110.XSHG": 1000},
            )
            engine = OrderExecutionEngine(
                store,
                market_data,
                broker,
                ExecutionConfig(order_timeout_sec=0, max_attempts=2, poll_interval_sec=0),
            )

            result = engine.execute(signal)

            self.assertEqual(result.status, ExecutionStatus.FILLED)
            self.assertEqual(market_data.queries, [signal.code, signal.code])
            self.assertEqual(broker.position_queries, [signal.code, signal.code])

    def test_buy_cap_uses_current_order_price_and_round_lot(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            store = SQLiteExecutionStore(Path(tmpdir) / "state.db")
            signal = TradeSignal(
                signal_id="sig-buy-current-price",
                strategy_id="hunter",
                action=Action.BUY,
                code="159309.XSHE",
                amount=1000,
                reference_price=1.0,
                created_at="2026-07-10 09:30:00",
            )
            broker = FakeBroker(
                {"order-1": [OrderSnapshot("order-1", BrokerOrderStatus.FILLED, filled_qty=900)]},
                available_cash=1150.0,
            )
            engine = OrderExecutionEngine(
                store,
                FakeMarketData([Quote(last_price=1.0, ask1=1.2, bid1=1.199)]),
                broker,
                ExecutionConfig(
                    pricing_mode="book",
                    book_tick_offset=0,
                    order_timeout_sec=0,
                    max_attempts=1,
                    poll_interval_sec=0,
                    max_deviation_from_signal_price_pct=0.25,
                ),
            )

            result = engine.execute(signal)

            self.assertEqual(result.status, ExecutionStatus.FILLED)
            self.assertEqual(broker.submitted, [("order-1", 900, 1.2)])

    def test_cancel_confirmation_reconciles_late_fill_before_reorder(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            store = SQLiteExecutionStore(Path(tmpdir) / "state.db")
            signal = TradeSignal(
                signal_id="sig-cancel-late-fill",
                strategy_id="hunter",
                action=Action.SELL,
                code="517110.XSHG",
                amount=1000,
                reference_price=0.710,
                created_at="2026-07-10 09:30:00",
            )
            broker = FakeBroker(
                {
                    "order-1": [
                        OrderSnapshot("order-1", BrokerOrderStatus.PARTIALLY_FILLED, filled_qty=400),
                    ],
                    "order-2": [OrderSnapshot("order-2", BrokerOrderStatus.FILLED, filled_qty=400)],
                },
                available_positions={"517110.XSHG": 1000},
                cancel_snapshots_by_order={
                    "order-1": [
                        OrderSnapshot("order-1", BrokerOrderStatus.CANCELED, filled_qty=600),
                    ],
                },
            )
            engine = OrderExecutionEngine(
                store,
                FakeMarketData([0.710, 0.711]),
                broker,
                ExecutionConfig(order_timeout_sec=0, max_attempts=2, poll_interval_sec=0),
            )

            result = engine.execute(signal)

            self.assertEqual(result.status, ExecutionStatus.FILLED)
            self.assertEqual(result.filled_qty, 1000)
            self.assertEqual(broker.submitted[1][1], 400)

    def test_resource_query_failure_does_not_submit_order(self):
        class FailingPositionBroker(FakeBroker):
            def query_available_position(self, code):
                raise RuntimeError("position query unavailable")

        with tempfile.TemporaryDirectory() as tmpdir:
            store = SQLiteExecutionStore(Path(tmpdir) / "state.db")
            signal = TradeSignal(
                signal_id="sig-position-query-failed",
                strategy_id="hunter",
                action=Action.SELL,
                code="517110.XSHG",
                amount=1000,
                reference_price=0.710,
                created_at="2026-07-10 09:30:00",
            )
            broker = FailingPositionBroker(
                {"order-1": [OrderSnapshot("order-1", BrokerOrderStatus.FILLED, filled_qty=1000)]},
            )
            engine = OrderExecutionEngine(
                store,
                FakeMarketData([0.710]),
                broker,
                ExecutionConfig(order_timeout_sec=0, max_attempts=1, poll_interval_sec=0),
            )

            result = engine.execute(signal)

            self.assertEqual(result.status, ExecutionStatus.FAILED_BROKER)
            self.assertEqual(broker.submitted, [])

    def test_unconfirmed_cancel_halts_later_trade_submissions(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            store = SQLiteExecutionStore(Path(tmpdir) / "state.db")
            first_signal = TradeSignal(
                signal_id="sig-cancel-unconfirmed",
                strategy_id="hunter",
                action=Action.SELL,
                code="517110.XSHG",
                amount=1000,
                reference_price=0.710,
                created_at="2026-07-10 09:30:00",
            )
            second_signal = TradeSignal(
                signal_id="sig-after-uncertain-order",
                strategy_id="hunter",
                action=Action.BUY,
                code="159309.XSHE",
                amount=100,
                reference_price=1.260,
                created_at="2026-07-10 09:30:01",
            )
            broker = FakeBroker(
                {"order-1": [OrderSnapshot("order-1", BrokerOrderStatus.OPEN, filled_qty=0)]},
                available_positions={"517110.XSHG": 1000},
                cancel_snapshots_by_order={
                    "order-1": [OrderSnapshot("order-1", BrokerOrderStatus.OPEN, filled_qty=0)],
                },
            )
            engine = OrderExecutionEngine(
                store,
                FakeMarketData([0.710, 1.260]),
                broker,
                ExecutionConfig(
                    order_timeout_sec=0,
                    max_attempts=2,
                    max_total_duration_sec=0.01,
                    poll_interval_sec=0.001,
                ),
            )

            first_result = engine.execute(first_signal)
            second_result = engine.execute(second_signal)

            self.assertEqual(first_result.status, ExecutionStatus.FAILED_BROKER)
            self.assertEqual(second_result.status, ExecutionStatus.FAILED_BROKER)
            self.assertEqual(len(broker.submitted), 1)

    def test_stops_after_max_attempts_with_partial_timeout(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            store = SQLiteExecutionStore(Path(tmpdir) / "state.db")
            signal = TradeSignal(
                signal_id="sig-2",
                strategy_id="hunter",
                action=Action.SELL,
                code="000001.XSHE",
                amount=1000,
                reference_price=10.0,
                created_at="2026-06-08 09:30:00",
            )
            config = ExecutionConfig(
                sell_slippage_pct=0.003,
                order_timeout_sec=0,
                max_attempts=2,
                poll_interval_sec=0,
            )
            market_data = FakeMarketData([10.0, 9.9])
            broker = FakeBroker(
                {
                    "order-1": [OrderSnapshot("order-1", BrokerOrderStatus.PARTIALLY_FILLED, filled_qty=300)],
                    "order-2": [OrderSnapshot("order-2", BrokerOrderStatus.OPEN, filled_qty=0)],
                }
            )
            engine = OrderExecutionEngine(store, market_data, broker, config)

            result = engine.execute(signal)

            self.assertEqual(result.status, ExecutionStatus.PARTIALLY_FILLED_TIMEOUT)
            self.assertEqual(result.filled_qty, 300)
            self.assertEqual(broker.canceled, ["order-1", "order-2"])
            self.assertEqual(broker.submitted, [("order-1", 1000, 9.97), ("order-2", 700, 9.87)])

    # ---- 竞价时段保护测试 ----

    def test_auction_protection_extends_poll_deadline_until_open(self):
        """竞价时段收到信号: order_timeout_sec=0 但保护顺延 deadline,
        委托不会被立即判超时撤单, 而是持续轮询到(模拟的)开盘后成交。"""
        original = executor_module._seconds_until_market_open
        executor_module._seconds_until_market_open = lambda: 0.5  # 模拟距开盘 0.5 秒
        try:
            with tempfile.TemporaryDirectory() as tmpdir:
                store = SQLiteExecutionStore(Path(tmpdir) / "state.db")
                signal = TradeSignal(
                    signal_id="sig-auction",
                    strategy_id="hunter",
                    action=Action.SELL,
                    code="000001.XSHE",
                    amount=1000,
                    reference_price=10.0,
                    created_at="2026-06-08 09:27:00",
                )
                config = ExecutionConfig(
                    order_timeout_sec=0,
                    max_attempts=1,
                    poll_interval_sec=0.01,
                )
                market_data = FakeMarketData([10.0])
                broker = FakeBroker(
                    {
                        "order-1": [
                            OrderSnapshot("order-1", BrokerOrderStatus.OPEN, filled_qty=0),
                            OrderSnapshot("order-1", BrokerOrderStatus.OPEN, filled_qty=0),
                            OrderSnapshot("order-1", BrokerOrderStatus.FILLED, filled_qty=1000),
                        ]
                    }
                )
                engine = OrderExecutionEngine(store, market_data, broker, config)

                result = engine.execute(signal)

                self.assertEqual(result.status, ExecutionStatus.FILLED)
                self.assertEqual(broker.canceled, [])
        finally:
            executor_module._seconds_until_market_open = original

    # ---- 盘口价定价模式测试 ----

    def test_book_pricing_buys_at_ask_plus_tick_offset(self):
        """book 模式买入: 委托价 = 卖一价 + book_tick_offset 个 tick, 而不是最新价加滑点。"""
        with tempfile.TemporaryDirectory() as tmpdir:
            store = SQLiteExecutionStore(Path(tmpdir) / "state.db")
            signal = TradeSignal(
                signal_id="sig-book-buy",
                strategy_id="hunter",
                action=Action.BUY,
                code="000001.XSHE",
                amount=1000,
                reference_price=10.0,
                created_at="2026-06-08 09:30:00",
            )
            config = ExecutionConfig(
                pricing_mode="book",
                book_tick_offset=2,
                order_timeout_sec=0,
                max_attempts=1,
                poll_interval_sec=0,
            )
            market_data = FakeMarketData([Quote(last_price=10.0, ask1=10.02, bid1=10.01)])
            broker = FakeBroker(
                {"order-1": [OrderSnapshot("order-1", BrokerOrderStatus.FILLED, filled_qty=1000)]}
            )
            engine = OrderExecutionEngine(store, market_data, broker, config)

            result = engine.execute(signal)

            self.assertEqual(result.status, ExecutionStatus.FILLED)
            self.assertEqual(broker.submitted, [("order-1", 1000, 10.04)])

    def test_book_pricing_falls_back_to_slippage_when_ask_missing(self):
        """涨停等场景卖一缺失 → 回退最新价滑点定价, 不因盘口缺档下不了单。"""
        with tempfile.TemporaryDirectory() as tmpdir:
            store = SQLiteExecutionStore(Path(tmpdir) / "state.db")
            signal = TradeSignal(
                signal_id="sig-book-fallback",
                strategy_id="hunter",
                action=Action.BUY,
                code="000001.XSHE",
                amount=1000,
                reference_price=10.0,
                created_at="2026-06-08 09:30:00",
            )
            config = ExecutionConfig(
                pricing_mode="book",
                buy_slippage_pct=0.003,
                order_timeout_sec=0,
                max_attempts=1,
                poll_interval_sec=0,
            )
            market_data = FakeMarketData([Quote(last_price=10.0, ask1=None, bid1=10.0)])
            broker = FakeBroker(
                {"order-1": [OrderSnapshot("order-1", BrokerOrderStatus.FILLED, filled_qty=1000)]}
            )
            engine = OrderExecutionEngine(store, market_data, broker, config)

            result = engine.execute(signal)

            self.assertEqual(result.status, ExecutionStatus.FILLED)
            self.assertEqual(broker.submitted, [("order-1", 1000, 10.03)])

    # ---- 数量上限适配测试 ----

    def test_buy_capped_to_available_cash(self):
        """信号要买 2000 股, 但资金只够买约 1500 股 → 数量被上限到 1500 股。"""
        with tempfile.TemporaryDirectory() as tmpdir:
            store = SQLiteExecutionStore(Path(tmpdir) / "state.db")
            signal = TradeSignal(
                signal_id="sig-buy-cap",
                strategy_id="hunter",
                action=Action.BUY,
                code="000001.XSHE",
                amount=2000,
                reference_price=10.0,
                created_at="2026-06-08 09:30:00",
            )
            config = ExecutionConfig(
                buy_slippage_pct=0.003,
                order_timeout_sec=0,
                max_attempts=1,
                poll_interval_sec=0,
            )
            # 资金只够买 1500 股: 10.03 * 1500 = 15045
            available_cash = 10.03 * 1500
            market_data = FakeMarketData([10.0])
            broker = FakeBroker(
                {
                    "order-1": [OrderSnapshot("order-1", BrokerOrderStatus.FILLED, filled_qty=1500)],
                },
                available_cash=available_cash,
            )
            engine = OrderExecutionEngine(store, market_data, broker, config)

            result = engine.execute(signal)

            self.assertEqual(result.status, ExecutionStatus.FILLED)
            self.assertEqual(result.filled_qty, 1500)
            self.assertEqual(result.requested_qty, 2000)  # 原始请求量保留
            self.assertEqual(len(broker.submitted), 1)
            self.assertEqual(broker.submitted[0][1], 1500)  # 实际下单 1500 股

    def test_sell_capped_to_available_position(self):
        """信号要卖 2000 股, 但持仓只有 1800 股 → 数量被上限到 1800 股。"""
        with tempfile.TemporaryDirectory() as tmpdir:
            store = SQLiteExecutionStore(Path(tmpdir) / "state.db")
            signal = TradeSignal(
                signal_id="sig-sell-cap",
                strategy_id="hunter",
                action=Action.SELL,
                code="000001.XSHE",
                amount=2000,
                reference_price=10.0,
                created_at="2026-06-08 09:30:00",
            )
            config = ExecutionConfig(
                sell_slippage_pct=0.003,
                order_timeout_sec=0,
                max_attempts=1,
                poll_interval_sec=0,
            )
            market_data = FakeMarketData([10.0])
            broker = FakeBroker(
                {
                    "order-1": [OrderSnapshot("order-1", BrokerOrderStatus.FILLED, filled_qty=1800)],
                },
                available_positions={"000001.XSHE": 1800},
            )
            engine = OrderExecutionEngine(store, market_data, broker, config)

            result = engine.execute(signal)

            self.assertEqual(result.status, ExecutionStatus.FILLED)
            self.assertEqual(result.filled_qty, 1800)
            self.assertEqual(result.requested_qty, 2000)
            self.assertEqual(len(broker.submitted), 1)
            self.assertEqual(broker.submitted[0][1], 1800)

    def test_buy_insufficient_cash_rejected(self):
        """资金连 1 股都买不起 → 风控拒绝。"""
        with tempfile.TemporaryDirectory() as tmpdir:
            store = SQLiteExecutionStore(Path(tmpdir) / "state.db")
            signal = TradeSignal(
                signal_id="sig-no-cash",
                strategy_id="hunter",
                action=Action.BUY,
                code="000001.XSHE",
                amount=1000,
                reference_price=10.0,
                created_at="2026-06-08 09:30:00",
            )
            config = ExecutionConfig(buy_slippage_pct=0.003)
            market_data = FakeMarketData([10.0])
            broker = FakeBroker(
                {},
                available_cash=0.0,  # 一分钱都没有
            )
            engine = OrderExecutionEngine(store, market_data, broker, config)

            result = engine.execute(signal)

            self.assertEqual(result.status, ExecutionStatus.FAILED_RISK)
            self.assertEqual(result.filled_qty, 0)
            self.assertEqual(len(broker.submitted), 0)

    def test_sell_no_position_rejected(self):
        """没有该股票持仓 → 风控拒绝。"""
        with tempfile.TemporaryDirectory() as tmpdir:
            store = SQLiteExecutionStore(Path(tmpdir) / "state.db")
            signal = TradeSignal(
                signal_id="sig-no-pos",
                strategy_id="hunter",
                action=Action.SELL,
                code="000001.XSHE",
                amount=1000,
                reference_price=10.0,
                created_at="2026-06-08 09:30:00",
            )
            config = ExecutionConfig(sell_slippage_pct=0.003)
            market_data = FakeMarketData([10.0])
            broker = FakeBroker(
                {},
                available_positions={"000001.XSHE": 0},
            )
            engine = OrderExecutionEngine(store, market_data, broker, config)

            result = engine.execute(signal)

            self.assertEqual(result.status, ExecutionStatus.FAILED_RISK)
            self.assertEqual(result.filled_qty, 0)
            self.assertEqual(len(broker.submitted), 0)

    def test_expired_signal_is_recorded_without_market_or_broker_access(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            store = SQLiteExecutionStore(Path(tmpdir) / "state.db")
            signal = TradeSignal(
                signal_id="sig-expired",
                strategy_id="hunter",
                action=Action.BUY,
                code="000001.XSHE",
                amount=1000,
                reference_price=10.0,
                created_at="2000-01-01 09:30:00",
                expire_at="2000-01-01 09:30:20",
            )
            market_data = FakeMarketData([])
            broker = FakeBroker({})
            engine = OrderExecutionEngine(store, market_data, broker, ExecutionConfig())

            result = engine.execute(signal)

            self.assertEqual(result.status, ExecutionStatus.EXPIRED)
            self.assertEqual(result.filled_qty, 0)
            self.assertEqual(result.attempts, 0)
            self.assertEqual(store.get_signal(signal.signal_id).status, ExecutionStatus.EXPIRED)
            self.assertEqual(market_data.queries, [])
            self.assertEqual(broker.submitted, [])
            self.assertEqual(broker.cash_queries, 0)

    def test_invalid_expire_at_fails_closed_without_market_or_broker_access(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            store = SQLiteExecutionStore(Path(tmpdir) / "state.db")
            signal = TradeSignal(
                signal_id="sig-invalid-expiry",
                strategy_id="hunter",
                action=Action.BUY,
                code="000001.XSHE",
                amount=1000,
                reference_price=10.0,
                created_at="2026-07-14 09:30:00",
                expire_at="09:30:20",
            )
            market_data = FakeMarketData([])
            broker = FakeBroker({})
            engine = OrderExecutionEngine(store, market_data, broker, ExecutionConfig())

            result = engine.execute(signal)

            self.assertEqual(result.status, ExecutionStatus.FAILED_RISK)
            self.assertIn("invalid expire_at", result.message)
            self.assertEqual(market_data.queries, [])
            self.assertEqual(broker.submitted, [])
            self.assertEqual(broker.cash_queries, 0)


if __name__ == "__main__":
    unittest.main()

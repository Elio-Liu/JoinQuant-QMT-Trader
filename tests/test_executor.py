import threading
import tempfile
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest import mock

import miniqmt_follower.executor as executor_module
from miniqmt_follower.executor import OrderExecutionEngine
from miniqmt_follower.models import (
    Action,
    BrokerOrderRejected,
    BrokerOrderStatus,
    BrokerRejectionKind,
    BrokerSubmissionUncertain,
    ExecutionConfig,
    ExecutionStatus,
    OrderSnapshot,
    Quote,
    TradeSignal,
)
from miniqmt_follower.store import SQLiteExecutionStore


def setUpModule():
    # 测试与真实时钟解耦: 若测试恰好在 9:15~9:30 运行, 竞价保护会顺延超时,
    # 破坏 order_timeout_sec=0 的立即超时假设。默认关闭, 竞价用例单独覆盖。
    executor_module._seconds_until_market_open = lambda: 0.0


class FakeMarketData:
    """假行情源。prices 里的元素可以是 float(只有最新价)或 Quote(带盘口)。"""

    def __init__(self, prices, names=None):
        self.prices = list(prices)
        self.names = names or {}
        self.queries = []

    def latest_quote(self, code):
        self.queries.append(code)
        item = self.prices.pop(0)
        if isinstance(item, Quote):
            return item
        return Quote(last_price=item)

    def instrument_name(self, code):
        return self.names.get(code)


class FakeBroker:
    def __init__(
        self,
        snapshots_by_order,
        *,
        available_cash=1_000_000.0,
        available_cash_sequence=None,
        available_positions=None,
        cancel_snapshots_by_order=None,
        available_total_assets=None,
        total_positions=None,
    ):
        self.snapshots_by_order = snapshots_by_order
        self.cancel_snapshots_by_order = cancel_snapshots_by_order or {}
        self.submitted = []
        self.canceled = []
        self.available_cash = available_cash
        self.available_cash_sequence = list(available_cash_sequence or [])
        self.available_positions = available_positions or {}
        self.available_total_assets = (
            available_total_assets
            if available_total_assets is not None
            else available_cash
        )
        self.total_positions = total_positions or {}
        self.cash_queries = 0
        self.position_queries = []
        self._cancel_requested = set()
        self._last_snapshots = {}

    def query_available_cash(self) -> float:
        self.cash_queries += 1
        if self.available_cash_sequence:
            if len(self.available_cash_sequence) > 1:
                return self.available_cash_sequence.pop(0)
            return self.available_cash_sequence[0]
        return self.available_cash

    def query_available_position(self, code: str) -> int:
        self.position_queries.append(code)
        return self.available_positions.get(code, 10_000)

    def query_total_assets(self) -> float:
        return self.available_total_assets

    def query_position(self, code: str) -> int:
        return self.total_positions.get(
            code, self.available_positions.get(code, 0)
        )

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


class FakeDirectRejectBroker(FakeBroker):
    def __init__(self, snapshots_by_order, submit_outcomes, **kwargs):
        super().__init__(snapshots_by_order, **kwargs)
        self.submit_outcomes = list(submit_outcomes)
        self.submit_calls = []

    def submit_order(self, signal, quantity, price):
        self.submit_calls.append((quantity, price))
        if self.submit_outcomes:
            outcome = self.submit_outcomes.pop(0)
            if isinstance(outcome, Exception):
                raise outcome
        return super().submit_order(signal, quantity, price)


class FakeSnapshotErrorBroker(FakeBroker):
    def get_order_snapshot(self, order_id):
        raise ConnectionError(f"order query unavailable for {order_id}")


class CashRaceBroker(FakeBroker):
    def __init__(self, available_cash):
        super().__init__({}, available_cash=available_cash)
        self._cash_lock = threading.Lock()
        self._filled_by_order = {}

    def query_available_cash(self):
        with self._cash_lock:
            cash = self.available_cash
        time.sleep(0.05)
        return cash

    def submit_order(self, signal, quantity, price):
        with self._cash_lock:
            order_id = f"order-{len(self.submitted) + 1}"
            self.available_cash -= quantity * price
            self.submitted.append((order_id, quantity, price))
            self._filled_by_order[order_id] = quantity
        return order_id

    def get_order_snapshot(self, order_id):
        return OrderSnapshot(
            order_id,
            BrokerOrderStatus.FILLED,
            self._filled_by_order[order_id],
        )


class HaltRaceBroker:
    def __init__(self):
        self.cash_query_started = threading.Event()
        self.allow_cash_return = threading.Event()
        self.submitted = []

    def query_available_cash(self):
        self.cash_query_started.set()
        if not self.allow_cash_return.wait(1):
            raise TimeoutError("test did not release cash query")
        return 1_000_000.0

    def query_available_position(self, _code):
        return 1000

    def submit_order(self, signal, quantity, price):
        order_id = "sell-order" if signal.action == Action.SELL else "buy-order"
        self.submitted.append((order_id, quantity, price))
        return order_id

    def get_order_snapshot(self, order_id):
        if order_id == "sell-order":
            if not self.cash_query_started.wait(1):
                raise TimeoutError("BUY did not reach cash query")
            raise ConnectionError("limit-down queue state unavailable")
        return OrderSnapshot(order_id, BrokerOrderStatus.FILLED, 1000)

    def cancel_order(self, _order_id):
        raise AssertionError("cancel should not be called")


class BuyCashConcurrencyTests(unittest.TestCase):
    def _signals(self):
        return [
            TradeSignal(
                signal_id=signal_id,
                strategy_id="harvester",
                action=Action.BUY,
                code="000001.XSHE",
                amount=1000,
                reference_price=10.0,
                created_at="2026-08-03 09:30:00",
            )
            for signal_id in ("buy-1", "buy-2")
        ]

    @staticmethod
    def _execute_and_close(store, engine, signal):
        try:
            return engine.execute(signal)
        finally:
            store.close()

    def test_parallel_buys_serialize_cash_query_and_submission(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            broker = CashRaceBroker(available_cash=15045.0)
            store = SQLiteExecutionStore(Path(tmpdir) / "state.db")
            engine = OrderExecutionEngine(
                store,
                FakeMarketData([10.0, 10.0]),
                broker,
                ExecutionConfig(),
            )

            with ThreadPoolExecutor(max_workers=2) as pool:
                results = list(
                    pool.map(
                        lambda signal: self._execute_and_close(store, engine, signal),
                        self._signals(),
                    )
                )

            self.assertEqual(
                sorted(quantity for _, quantity, _ in broker.submitted),
                [500, 1000],
            )
            self.assertEqual(sum(result.filled_qty for result in results), 1500)
            store.close()

    def test_parallel_limit_up_buys_use_the_same_cash_lock(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            broker = CashRaceBroker(available_cash=15045.0)
            locked_quote = Quote(
                last_price=10.03,
                ask1=None,
                bid1=10.03,
                high_limit=10.03,
                low_limit=9.0,
            )
            store = SQLiteExecutionStore(Path(tmpdir) / "state.db")
            engine = OrderExecutionEngine(
                store,
                FakeMarketData([locked_quote, locked_quote]),
                broker,
                ExecutionConfig(limit_up_buy_mode="queue"),
            )

            with (
                mock.patch.object(
                    executor_module,
                    "_seconds_until_queue_buy_deadline",
                    return_value=60.0,
                ),
                ThreadPoolExecutor(max_workers=2) as pool,
            ):
                results = list(
                    pool.map(
                        lambda signal: self._execute_and_close(store, engine, signal),
                        self._signals(),
                    )
                )

            self.assertEqual(
                sorted(quantity for _, quantity, _ in broker.submitted),
                [500, 1000],
            )
            self.assertEqual(sum(result.filled_qty for result in results), 1500)
            store.close()


class OpeningSellPriorityTests(unittest.TestCase):
    def test_halt_after_barrier_release_blocks_running_buy_submission(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            queue_released = threading.Event()
            store = SQLiteExecutionStore(Path(tmpdir) / "state.db")
            broker = HaltRaceBroker()
            market = FakeMarketData([
                Quote(
                    last_price=9.0,
                    ask1=9.01,
                    bid1=None,
                    high_limit=11.0,
                    low_limit=9.0,
                ),
                Quote(last_price=10.0, ask1=10.0, bid1=9.99),
            ])
            engine = OrderExecutionEngine(
                store,
                market,
                broker,
                ExecutionConfig(limit_down_sell_mode="queue"),
                on_limit_down_queued=lambda _signal_id: queue_released.set(),
            )
            sell = TradeSignal(
                signal_id="halt-race-sell",
                strategy_id="harvester",
                action=Action.SELL,
                code="000001.XSHE",
                amount=1000,
                reference_price=9.0,
                created_at="2026-08-03 09:27:00",
            )
            buy = TradeSignal(
                signal_id="halt-race-buy",
                strategy_id="harvester",
                action=Action.BUY,
                code="000002.XSHE",
                amount=1000,
                reference_price=10.0,
                created_at="2026-08-03 09:30:00",
            )

            with (
                mock.patch.object(
                    executor_module,
                    "_seconds_until_queue_sell_deadline",
                    return_value=60.0,
                ),
                ThreadPoolExecutor(max_workers=2) as pool,
            ):
                sell_future = pool.submit(engine.execute, sell)
                self.assertTrue(queue_released.wait(0.5))
                buy_future = pool.submit(engine.execute, buy)
                self.assertTrue(broker.cash_query_started.wait(0.5))
                self.assertEqual(
                    sell_future.result(timeout=1).status,
                    ExecutionStatus.FAILED_BROKER,
                )
                broker.allow_cash_return.set()
                self.assertEqual(
                    buy_future.result(timeout=1).status,
                    ExecutionStatus.FAILED_BROKER,
                )

            self.assertEqual(
                broker.submitted,
                [("sell-order", 1000, 9.0)],
            )
            store.close()

    def test_preopen_sell_cancels_half_second_after_open_and_reprices_remaining(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            signal = TradeSignal(
                signal_id="preopen-sell",
                strategy_id="harvester",
                action=Action.SELL,
                code="000001.XSHE",
                amount=1000,
                reference_price=10.0,
                created_at="2026-08-03 09:27:00",
            )
            market = FakeMarketData([
                Quote(last_price=10.00, bid1=9.99, ask1=10.00, low_limit=9.00),
                Quote(last_price=9.80, bid1=9.79, ask1=9.80, low_limit=9.00),
            ])
            broker = FakeBroker({
                "order-1": [OrderSnapshot("order-1", BrokerOrderStatus.OPEN, 0)],
                "order-2": [OrderSnapshot("order-2", BrokerOrderStatus.FILLED, 1000)],
            })
            store = SQLiteExecutionStore(Path(tmpdir) / "state.db")
            engine = OrderExecutionEngine(
                store,
                market,
                broker,
                ExecutionConfig(
                    order_timeout_sec=3,
                    max_attempts=2,
                    poll_interval_sec=0.01,
                ),
            )
            started = time.monotonic()
            with mock.patch.object(
                executor_module, "_seconds_until_market_open", return_value=0.01,
            ):
                result = engine.execute(signal)
            elapsed = time.monotonic() - started

            self.assertEqual(result.status, ExecutionStatus.FILLED)
            self.assertEqual(broker.canceled, ["order-1"])
            self.assertEqual(
                broker.submitted,
                [("order-1", 1000, 9.97), ("order-2", 1000, 9.77)],
            )
            self.assertLess(elapsed, 1.0)
            store.close()

    def test_preopen_partial_fill_reorders_only_confirmed_remainder(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            signal = TradeSignal(
                signal_id="preopen-partial",
                strategy_id="harvester",
                action=Action.SELL,
                code="000001.XSHE",
                amount=1000,
                reference_price=10.0,
                created_at="2026-08-03 09:27:00",
            )
            broker = FakeBroker(
                {
                    "order-1": [OrderSnapshot("order-1", BrokerOrderStatus.OPEN, 300)],
                    "order-2": [OrderSnapshot("order-2", BrokerOrderStatus.FILLED, 600)],
                },
                cancel_snapshots_by_order={
                    "order-1": [
                        OrderSnapshot("order-1", BrokerOrderStatus.CANCELED, 400)
                    ],
                },
            )
            store = SQLiteExecutionStore(Path(tmpdir) / "state.db")
            engine = OrderExecutionEngine(
                store,
                FakeMarketData([10.0, 9.8]),
                broker,
                ExecutionConfig(max_attempts=2, poll_interval_sec=0.01),
            )
            with mock.patch.object(
                executor_module, "_seconds_until_market_open", return_value=0.01,
            ):
                result = engine.execute(signal)

            self.assertEqual(result.filled_qty, 1000)
            self.assertEqual(
                [quantity for _, quantity, _ in broker.submitted], [1000, 600]
            )
            store.close()

    def test_limit_down_queue_releases_opening_barrier_without_canceling_order(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            released = threading.Event()
            signal = TradeSignal(
                signal_id="preopen-limit-down",
                strategy_id="harvester",
                action=Action.SELL,
                code="000001.XSHE",
                amount=1000,
                reference_price=9.0,
                created_at="2026-08-03 09:27:00",
            )
            broker = FakeBroker({
                "order-1": [
                    OrderSnapshot("order-1", BrokerOrderStatus.OPEN, 0),
                    OrderSnapshot("order-1", BrokerOrderStatus.FILLED, 1000),
                ],
            })
            store = SQLiteExecutionStore(Path(tmpdir) / "state.db")
            engine = OrderExecutionEngine(
                store,
                FakeMarketData([
                    Quote(
                        last_price=9.0,
                        ask1=9.01,
                        bid1=None,
                        high_limit=11.0,
                        low_limit=9.0,
                    ),
                ]),
                broker,
                ExecutionConfig(
                    limit_down_sell_mode="queue",
                    queue_sell_poll_interval_sec=0.2,
                ),
                on_limit_down_queued=lambda _signal_id: released.set(),
            )
            with (
                mock.patch.object(
                    executor_module,
                    "_seconds_until_queue_sell_deadline",
                    return_value=60.0,
                ),
                ThreadPoolExecutor(max_workers=1) as pool,
            ):
                future = pool.submit(engine.execute, signal)
                self.assertTrue(released.wait(0.5))
                self.assertFalse(future.done())
                self.assertEqual(broker.submitted, [("order-1", 1000, 9.0)])
                self.assertEqual(broker.canceled, [])
                self.assertEqual(
                    future.result(timeout=1).status, ExecutionStatus.FILLED
                )
            store.close()


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
                    # 券商始终不回终态: 熬过独立的撤单确认宽限后才认定终态不明。
                    cancel_confirm_timeout_sec=0.01,
                ),
            )

            first_result = engine.execute(first_signal)
            second_result = engine.execute(second_signal)

            self.assertEqual(first_result.status, ExecutionStatus.FAILED_BROKER)
            self.assertEqual(second_result.status, ExecutionStatus.FAILED_BROKER)
            self.assertEqual(len(broker.submitted), 1)

    def test_exhausted_duration_budget_still_confirms_cancel_without_halting(self):
        """总预算耗尽时的普通未成交撤单不能触发停机 —— 回归 bug。

        重挂是背靠背的, 最后一次尝试必然贴着 max_total_duration_sec 边界, 撤单确认
        必然发生在预算之后。旧实现把总预算当撤单宽限, 于是每条走到超时的信号都会
        把当天整条交易通道停掉 (含后续所有止损单)。
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            store = SQLiteExecutionStore(Path(tmpdir) / "state.db")
            timeout_signal = TradeSignal(
                signal_id="sig-budget-exhausted",
                strategy_id="hunter",
                action=Action.BUY,
                code="000001.XSHE",
                amount=1000,
                reference_price=10.0,
                created_at="2026-06-08 09:30:00",
            )
            next_signal = TradeSignal(
                signal_id="sig-stop-loss-after-timeout",
                strategy_id="hunter",
                action=Action.SELL,
                code="000002.XSHE",
                amount=500,
                reference_price=20.0,
                created_at="2026-06-08 09:30:01",
            )
            broker = FakeBroker(
                {
                    "order-1": [OrderSnapshot("order-1", BrokerOrderStatus.OPEN, filled_qty=0)],
                    "order-2": [OrderSnapshot("order-2", BrokerOrderStatus.FILLED, filled_qty=500)],
                },
                available_positions={"000002.XSHE": 500},
                cancel_snapshots_by_order={
                    # 撤单确认要多轮询一次才落终态, 真实券商就是这个节奏。
                    "order-1": [
                        OrderSnapshot("order-1", BrokerOrderStatus.OPEN, filled_qty=0),
                        OrderSnapshot("order-1", BrokerOrderStatus.CANCELED, filled_qty=0),
                    ],
                },
            )
            engine = OrderExecutionEngine(
                store,
                FakeMarketData([10.0, 20.0]),
                broker,
                ExecutionConfig(
                    # 委托轮询(0.05s)必然跑过总预算(0.01s), 于是撤单发生在预算之后 ——
                    # 正是实盘里每条超时信号的必经路径。
                    order_timeout_sec=0.05,
                    max_attempts=1,
                    max_total_duration_sec=0.01,
                    poll_interval_sec=0,
                    cancel_confirm_timeout_sec=30.0,
                ),
            )

            timeout_result = engine.execute(timeout_signal)
            next_result = engine.execute(next_signal)

            # 前置条件: 确实提交并撤了单, 否则本用例根本没覆盖到撤单确认路径。
            self.assertEqual(broker.canceled, ["order-1"])
            # 这条信号本身超时未成交是正常结果, 不该被记成"券商失败"。
            self.assertEqual(timeout_result.status, ExecutionStatus.FAILED_TIMEOUT)
            self.assertIsNone(engine._trading_halt_reason)
            # 交易通道必须还活着: 后面的止损单照常成交。
            self.assertEqual(next_result.status, ExecutionStatus.FILLED)
            self.assertEqual(next_result.filled_qty, 500)

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

    def test_sell_without_live_position_is_skipped_without_submission(self):
        """聚宽发出卖出信号但实盘没有该持仓 → 明确跳过。"""
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

            self.assertEqual(result.status, ExecutionStatus.SKIPPED_NO_POSITION)
            self.assertEqual(result.filled_qty, 0)
            self.assertEqual(result.attempts, 0)
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
            store.close()

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
            store.close()


class RejectedOrderRecoveryTests(unittest.TestCase):
    @staticmethod
    def _signal(signal_id="sig-rejected", amount=1000):
        return TradeSignal(
            signal_id=signal_id,
            strategy_id="harvester",
            action=Action.BUY,
            code="000001.XSHE",
            amount=amount,
            reference_price=10.0,
            created_at="2026-08-03 09:29:22",
        )

    @staticmethod
    def _config(max_attempts=2):
        return ExecutionConfig(
            pricing_mode="book",
            book_tick_offset=2,
            order_timeout_sec=0,
            max_attempts=max_attempts,
            max_total_duration_sec=15,
            poll_interval_sec=0,
        )

    def _execute(self, signal, market_data, broker, config=None):
        with tempfile.TemporaryDirectory() as tmpdir:
            store = SQLiteExecutionStore(Path(tmpdir) / "state.db")
            engine = OrderExecutionEngine(
                store, market_data, broker, config or self._config(),
            )
            return engine.execute(signal)

    def test_price_rejection_refreshes_quote_and_fills_on_second_order(self):
        market_data = FakeMarketData(
            [
                Quote(last_price=10.0, ask1=10.0, bid1=9.99),
                Quote(last_price=10.1, ask1=10.1, bid1=10.09),
            ]
        )
        broker = FakeBroker(
            {
                "order-1": [
                    OrderSnapshot(
                        "order-1",
                        BrokerOrderStatus.REJECTED,
                        rejection_reason="委托价不正确",
                        rejection_kind=BrokerRejectionKind.PRICE,
                    )
                ],
                "order-2": [OrderSnapshot("order-2", BrokerOrderStatus.FILLED, 1000)],
            }
        )

        result = self._execute(self._signal(), market_data, broker)

        self.assertEqual(result.status, ExecutionStatus.FILLED)
        self.assertEqual(result.attempts, 2)
        self.assertEqual(len(market_data.queries), 2)
        self.assertEqual(broker.cash_queries, 2)
        self.assertEqual(broker.canceled, [])
        self.assertEqual(
            broker.submitted,
            [("order-1", 1000, 10.02), ("order-2", 1000, 10.12)],
        )

    def test_unknown_confirmed_rejection_defaults_to_refresh_and_retry(self):
        market_data = FakeMarketData([10.0, 10.1])
        broker = FakeBroker(
            {
                "order-1": [
                    OrderSnapshot(
                        "order-1",
                        BrokerOrderStatus.REJECTED,
                        rejection_reason="未识别柜台废单",
                        rejection_kind=BrokerRejectionKind.UNKNOWN,
                    )
                ],
                "order-2": [OrderSnapshot("order-2", BrokerOrderStatus.FILLED, 1000)],
            }
        )

        result = self._execute(self._signal("sig-unknown"), market_data, broker)

        self.assertEqual(result.status, ExecutionStatus.FILLED)
        self.assertEqual(result.attempts, 2)

    def test_hard_rejection_stops_without_second_submission(self):
        market_data = FakeMarketData([10.0, 10.1])
        broker = FakeBroker(
            {
                "order-1": [
                    OrderSnapshot(
                        "order-1",
                        BrokerOrderStatus.REJECTED,
                        rejection_reason="证券停牌",
                        rejection_kind=BrokerRejectionKind.HARD_STOP,
                    )
                ]
            }
        )

        result = self._execute(self._signal("sig-hard-stop"), market_data, broker)

        self.assertEqual(result.status, ExecutionStatus.FAILED_BROKER)
        self.assertEqual(result.attempts, 1)
        self.assertIn("证券停牌", result.message)
        self.assertEqual(len(broker.submitted), 1)
        self.assertEqual(len(market_data.queries), 1)

    def test_partial_fill_on_rejected_order_retries_only_remaining_quantity(self):
        market_data = FakeMarketData([10.0, 10.1])
        broker = FakeBroker(
            {
                "order-1": [
                    OrderSnapshot(
                        "order-1",
                        BrokerOrderStatus.REJECTED,
                        filled_qty=400,
                        rejection_reason="订单价格超出范围",
                        rejection_kind=BrokerRejectionKind.PRICE,
                    )
                ],
                "order-2": [OrderSnapshot("order-2", BrokerOrderStatus.FILLED, 600)],
            }
        )

        result = self._execute(self._signal("sig-rejected-partial"), market_data, broker)

        self.assertEqual(result.status, ExecutionStatus.FILLED)
        self.assertEqual(result.filled_qty, 1000)
        self.assertEqual(broker.submitted[1][1], 600)

    def test_resource_rejection_refreshes_cash_and_resizes_each_submission(self):
        market_data = FakeMarketData([10.0, 10.1, 10.2])
        broker = FakeBroker(
            {
                "order-1": [
                    OrderSnapshot(
                        "order-1",
                        BrokerOrderStatus.REJECTED,
                        rejection_reason="可用资金不足",
                        rejection_kind=BrokerRejectionKind.RESOURCE,
                    )
                ],
                "order-2": [OrderSnapshot("order-2", BrokerOrderStatus.FILLED, 500)],
                "order-3": [OrderSnapshot("order-3", BrokerOrderStatus.FILLED, 500)],
            },
            available_cash_sequence=[1_000_000.0, 5_065.0, 1_000_000.0],
        )

        result = self._execute(
            self._signal("sig-resource-reject"),
            market_data,
            broker,
            self._config(max_attempts=3),
        )

        self.assertEqual(result.status, ExecutionStatus.FILLED)
        self.assertEqual(result.attempts, 3)
        self.assertEqual([quantity for _order_id, quantity, _price in broker.submitted], [1000, 500, 500])
        self.assertEqual(broker.cash_queries, 3)

    def test_direct_confirmed_rejection_retries_without_fake_order_id(self):
        market_data = FakeMarketData([10.0, 10.1])
        broker = FakeDirectRejectBroker(
            {"order-1": [OrderSnapshot("order-1", BrokerOrderStatus.FILLED, 1000)]},
            [
                BrokerOrderRejected(
                    "订单价格超出范围",
                    error_code="110001",
                    kind=BrokerRejectionKind.PRICE,
                ),
                None,
            ],
        )

        result = self._execute(self._signal("sig-direct-reject"), market_data, broker)

        self.assertEqual(result.status, ExecutionStatus.FILLED)
        self.assertEqual(result.attempts, 2)
        self.assertEqual(len(broker.submit_calls), 2)
        self.assertEqual(len(broker.submitted), 1)

    def test_submission_uncertain_halts_this_engine_and_later_signals(self):
        market_data = FakeMarketData([10.0, 10.1])
        broker = FakeDirectRejectBroker(
            {},
            [BrokerSubmissionUncertain("QMT submission state uncertain")],
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            store = SQLiteExecutionStore(Path(tmpdir) / "state.db")
            engine = OrderExecutionEngine(store, market_data, broker, self._config())

            first = engine.execute(self._signal("sig-uncertain-1"))
            second = engine.execute(self._signal("sig-uncertain-2"))

        self.assertEqual(first.status, ExecutionStatus.FAILED_BROKER)
        self.assertEqual(first.attempts, 1)
        self.assertIn("manual MiniQMT reconciliation", first.message)
        self.assertEqual(second.status, ExecutionStatus.FAILED_BROKER)
        self.assertEqual(len(broker.submit_calls), 1)
        self.assertEqual(len(market_data.queries), 1)

    def test_order_query_error_after_submission_halts_later_signals(self):
        market_data = FakeMarketData([10.0, 10.1])
        broker = FakeSnapshotErrorBroker({})
        with tempfile.TemporaryDirectory() as tmpdir:
            store = SQLiteExecutionStore(Path(tmpdir) / "state.db")
            engine = OrderExecutionEngine(store, market_data, broker, self._config())

            first = engine.execute(self._signal("sig-query-uncertain-1"))
            second = engine.execute(self._signal("sig-query-uncertain-2"))

        self.assertEqual(first.status, ExecutionStatus.FAILED_BROKER)
        self.assertEqual(first.attempts, 1)
        self.assertIn("state is uncertain", first.message)
        self.assertIn("manual MiniQMT reconciliation", first.message)
        self.assertEqual(second.status, ExecutionStatus.FAILED_BROKER)
        self.assertEqual(len(broker.submitted), 1)
        self.assertEqual(len(market_data.queries), 1)

    def test_rejections_consume_attempt_budget(self):
        market_data = FakeMarketData([10.0, 10.1])
        broker = FakeBroker(
            {
                "order-1": [
                    OrderSnapshot(
                        "order-1", BrokerOrderStatus.REJECTED,
                        rejection_kind=BrokerRejectionKind.UNKNOWN,
                    )
                ],
                "order-2": [
                    OrderSnapshot(
                        "order-2", BrokerOrderStatus.REJECTED,
                        rejection_kind=BrokerRejectionKind.UNKNOWN,
                    )
                ],
            }
        )

        result = self._execute(
            self._signal("sig-reject-budget"), market_data, broker, self._config(max_attempts=2),
        )

        self.assertEqual(result.status, ExecutionStatus.FAILED_TIMEOUT)
        self.assertEqual(result.attempts, 2)
        self.assertEqual(len(broker.submitted), 2)


class LockedBookSkipTests(unittest.TestCase):
    """涨跌停无对手盘快速跳过: 防止排队委托占满同方向 worker。"""

    def _make_signal(self, action: Action) -> TradeSignal:
        return TradeSignal(
            signal_id=f"sig-locked-{action.value}",
            strategy_id="hunter",
            action=action,
            code="000001.XSHE",
            amount=1000,
            reference_price=10.0,
            created_at="2026-07-16 09:30:00",
        )

    def test_sell_with_empty_bid_side_skips_without_order(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            store = SQLiteExecutionStore(Path(tmpdir) / "state.db")
            # 跌停封死: 买一无档 (bid1=None)
            market_data = FakeMarketData([Quote(last_price=9.0, ask1=9.0, bid1=None)])
            broker = FakeBroker({}, available_positions={"000001.XSHE": 1000})
            engine = OrderExecutionEngine(
                store, market_data, broker,
                ExecutionConfig(
                    order_timeout_sec=0, poll_interval_sec=0,
                    skip_sell_when_limit_down=True,
                ),
            )

            result = engine.execute(self._make_signal(Action.SELL))

            self.assertEqual(result.status, ExecutionStatus.SKIPPED_LIMIT_DOWN)
            self.assertEqual(result.filled_qty, 0)
            self.assertEqual(broker.submitted, [])
            self.assertEqual(broker.canceled, [])
            stored = store.get_signal(result.signal_id)
            self.assertEqual(stored.status, ExecutionStatus.SKIPPED_LIMIT_DOWN)
            store.close()

    def test_buy_with_empty_ask_side_skips_without_order(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            store = SQLiteExecutionStore(Path(tmpdir) / "state.db")
            # 涨停封死: 卖一无档 (ask1=None)
            market_data = FakeMarketData([Quote(last_price=11.0, ask1=None, bid1=11.0)])
            broker = FakeBroker({})
            engine = OrderExecutionEngine(
                store, market_data, broker,
                ExecutionConfig(
                    order_timeout_sec=0, poll_interval_sec=0,
                    skip_buy_when_limit_up=True,
                ),
            )

            result = engine.execute(self._make_signal(Action.BUY))

            self.assertEqual(result.status, ExecutionStatus.SKIPPED_LIMIT_UP)
            self.assertEqual(broker.submitted, [])
            store.close()

    def test_sell_skip_disabled_falls_back_to_slippage_pricing(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            store = SQLiteExecutionStore(Path(tmpdir) / "state.db")
            # 开关关闭(默认): 无买盘也照原逻辑回退滑点定价下单
            market_data = FakeMarketData([Quote(last_price=10.0, ask1=10.0, bid1=None)])
            broker = FakeBroker(
                {"order-1": [OrderSnapshot("order-1", BrokerOrderStatus.FILLED, filled_qty=1000)]},
                available_positions={"000001.XSHE": 1000},
            )
            engine = OrderExecutionEngine(
                store, market_data, broker,
                ExecutionConfig(order_timeout_sec=0, poll_interval_sec=0),
            )

            result = engine.execute(self._make_signal(Action.SELL))

            self.assertEqual(result.status, ExecutionStatus.FILLED)
            self.assertEqual(len(broker.submitted), 1)
            store.close()

    def test_retry_after_partial_fill_hits_limit_down_marks_partial_timeout(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            store = SQLiteExecutionStore(Path(tmpdir) / "state.db")
            # 第1笔部分成交后撤单, 第2次取行情时已跌停封死 → 带成交量的部分成交超时终态
            market_data = FakeMarketData([
                Quote(last_price=10.0, ask1=10.01, bid1=10.0),
                Quote(last_price=9.0, ask1=9.0, bid1=None),
            ])
            broker = FakeBroker(
                {
                    "order-1": [
                        OrderSnapshot("order-1", BrokerOrderStatus.PARTIALLY_FILLED, filled_qty=400),
                    ],
                },
                available_positions={"000001.XSHE": 1000},
                cancel_snapshots_by_order={
                    "order-1": [
                        OrderSnapshot("order-1", BrokerOrderStatus.CANCELED, filled_qty=400),
                    ],
                },
            )
            engine = OrderExecutionEngine(
                store, market_data, broker,
                ExecutionConfig(
                    order_timeout_sec=0, max_attempts=3, poll_interval_sec=0,
                    skip_sell_when_limit_down=True,
                ),
            )

            result = engine.execute(self._make_signal(Action.SELL))

            self.assertEqual(result.status, ExecutionStatus.PARTIALLY_FILLED_TIMEOUT)
            self.assertEqual(result.filled_qty, 400)
            self.assertEqual(len(broker.submitted), 1)
            store.close()


class QueueLimitDownSellTests(unittest.TestCase):
    """跌停排队卖出 (limit_down_sell_mode=queue): 挂跌停价等开板, 截止撤单收尾。"""

    LOCKED_QUOTE = Quote(last_price=9.0, ask1=9.0, bid1=None, low_limit=9.0)

    def setUp(self):
        # 默认给足排队时长; 各用例按需覆盖 (0=已过截止, 小值=快速到期)。
        self._original_deadline_fn = executor_module._seconds_until_queue_sell_deadline
        executor_module._seconds_until_queue_sell_deadline = lambda _deadline: 60.0

    def tearDown(self):
        executor_module._seconds_until_queue_sell_deadline = self._original_deadline_fn

    def _make_signal(self, signal_id="sig-queue-sell") -> TradeSignal:
        return TradeSignal(
            signal_id=signal_id,
            strategy_id="harvester",
            action=Action.SELL,
            code="000001.XSHE",
            amount=1000,
            reference_price=10.0,
            created_at="2026-07-17 09:31:00",
        )

    def _make_engine(self, store, market_data, broker, **config_kwargs):
        defaults = dict(
            limit_down_sell_mode="queue",
            queue_sell_poll_interval_sec=0,
            order_timeout_sec=0,
            poll_interval_sec=0,
        )
        defaults.update(config_kwargs)
        return OrderExecutionEngine(store, market_data, broker, ExecutionConfig(**defaults))

    def test_queue_fills_after_reopen_without_cancel_or_resubmit(self):
        """开板成交: 只挂一笔跌停价委托, 全程零撤单零重挂。"""
        with tempfile.TemporaryDirectory() as tmpdir:
            store = SQLiteExecutionStore(Path(tmpdir) / "state.db")
            broker = FakeBroker(
                {
                    "order-1": [
                        OrderSnapshot("order-1", BrokerOrderStatus.OPEN, filled_qty=0),
                        OrderSnapshot("order-1", BrokerOrderStatus.OPEN, filled_qty=0),
                        OrderSnapshot("order-1", BrokerOrderStatus.FILLED, filled_qty=1000),
                    ]
                },
                available_positions={"000001.XSHE": 1000},
            )
            engine = self._make_engine(store, FakeMarketData([self.LOCKED_QUOTE]), broker)

            result = engine.execute(self._make_signal())

            self.assertEqual(result.status, ExecutionStatus.FILLED)
            self.assertEqual(result.filled_qty, 1000)
            self.assertEqual(broker.submitted, [("order-1", 1000, 9.0)])  # 挂跌停价
            self.assertEqual(broker.canceled, [])  # 排队期间绝不撤单重挂
            store.close()

    def test_queue_expires_at_deadline_cancels_once(self):
        """排到截止仍零成交: 只在截止时刻撤一次单 → LIMIT_DOWN_QUEUE_EXPIRED。"""
        executor_module._seconds_until_queue_sell_deadline = lambda _d: 0.05
        with tempfile.TemporaryDirectory() as tmpdir:
            store = SQLiteExecutionStore(Path(tmpdir) / "state.db")
            broker = FakeBroker(
                {"order-1": [OrderSnapshot("order-1", BrokerOrderStatus.OPEN, filled_qty=0)]},
                available_positions={"000001.XSHE": 1000},
            )
            engine = self._make_engine(
                store, FakeMarketData([self.LOCKED_QUOTE]), broker,
                queue_sell_poll_interval_sec=0.01,
            )

            result = engine.execute(self._make_signal())

            self.assertEqual(result.status, ExecutionStatus.LIMIT_DOWN_QUEUE_EXPIRED)
            self.assertEqual(result.filled_qty, 0)
            self.assertEqual(broker.canceled, ["order-1"])
            stored = store.get_signal(result.signal_id)
            self.assertEqual(stored.status, ExecutionStatus.LIMIT_DOWN_QUEUE_EXPIRED)
            store.close()

    def test_queue_partial_fill_at_deadline_marks_partial_timeout(self):
        """开板吃掉一部分后回封, 到截止撤单 → 带成交量的 PARTIALLY_FILLED_TIMEOUT。"""
        executor_module._seconds_until_queue_sell_deadline = lambda _d: 0.05
        with tempfile.TemporaryDirectory() as tmpdir:
            store = SQLiteExecutionStore(Path(tmpdir) / "state.db")
            broker = FakeBroker(
                {
                    "order-1": [
                        OrderSnapshot("order-1", BrokerOrderStatus.PARTIALLY_FILLED, filled_qty=300),
                    ]
                },
                available_positions={"000001.XSHE": 1000},
                cancel_snapshots_by_order={
                    "order-1": [OrderSnapshot("order-1", BrokerOrderStatus.CANCELED, filled_qty=300)],
                },
            )
            engine = self._make_engine(
                store, FakeMarketData([self.LOCKED_QUOTE]), broker,
                queue_sell_poll_interval_sec=0.01,
            )

            result = engine.execute(self._make_signal())

            self.assertEqual(result.status, ExecutionStatus.PARTIALLY_FILLED_TIMEOUT)
            self.assertEqual(result.filled_qty, 300)
            self.assertEqual(broker.canceled, ["order-1"])
            store.close()

    def test_queue_falls_back_to_skip_when_low_limit_unavailable(self):
        """行情源取不到跌停价 → 宁可跳过也不挂错价, 不产生任何委托。"""
        with tempfile.TemporaryDirectory() as tmpdir:
            store = SQLiteExecutionStore(Path(tmpdir) / "state.db")
            quote = Quote(last_price=9.0, ask1=9.0, bid1=None, low_limit=None)
            broker = FakeBroker({}, available_positions={"000001.XSHE": 1000})
            engine = self._make_engine(store, FakeMarketData([quote]), broker)

            result = engine.execute(self._make_signal())

            self.assertEqual(result.status, ExecutionStatus.SKIPPED_LIMIT_DOWN)
            self.assertEqual(broker.submitted, [])
            store.close()

    def test_queue_sell_without_live_position_is_skipped_without_submission(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            store = SQLiteExecutionStore(Path(tmpdir) / "state.db")
            broker = FakeBroker(
                {}, available_positions={"000001.XSHE": 0},
            )
            engine = self._make_engine(
                store, FakeMarketData([self.LOCKED_QUOTE]), broker,
            )

            result = engine.execute(self._make_signal("sig-queue-no-position"))

            self.assertEqual(result.status, ExecutionStatus.SKIPPED_NO_POSITION)
            self.assertEqual(result.attempts, 0)
            self.assertEqual(broker.submitted, [])
            store.close()

    def test_queue_falls_back_to_skip_past_deadline(self):
        """信号在排队截止之后才到 → 降级跳过。"""
        executor_module._seconds_until_queue_sell_deadline = lambda _d: 0.0
        with tempfile.TemporaryDirectory() as tmpdir:
            store = SQLiteExecutionStore(Path(tmpdir) / "state.db")
            broker = FakeBroker({}, available_positions={"000001.XSHE": 1000})
            engine = self._make_engine(store, FakeMarketData([self.LOCKED_QUOTE]), broker)

            result = engine.execute(self._make_signal())

            self.assertEqual(result.status, ExecutionStatus.SKIPPED_LIMIT_DOWN)
            self.assertEqual(broker.submitted, [])
            store.close()

    def test_queue_capacity_guard_degrades_to_skip(self):
        """排队并发已满 → 新的跌停信号降级跳过, 保住普通信号的 worker。"""
        with tempfile.TemporaryDirectory() as tmpdir:
            store = SQLiteExecutionStore(Path(tmpdir) / "state.db")
            broker = FakeBroker({}, available_positions={"000001.XSHE": 1000})
            engine = self._make_engine(
                store, FakeMarketData([self.LOCKED_QUOTE]), broker,
                max_concurrent_queue_sells=0,
            )

            result = engine.execute(self._make_signal())

            self.assertEqual(result.status, ExecutionStatus.SKIPPED_LIMIT_DOWN)
            self.assertEqual(broker.submitted, [])
            store.close()

    def test_legacy_skip_flag_still_skips_when_mode_unset(self):
        """旧配置兼容: 只设 skip_sell_when_limit_down=true (mode 留空) → 行为不变。"""
        with tempfile.TemporaryDirectory() as tmpdir:
            store = SQLiteExecutionStore(Path(tmpdir) / "state.db")
            broker = FakeBroker({}, available_positions={"000001.XSHE": 1000})
            engine = OrderExecutionEngine(
                store, FakeMarketData([self.LOCKED_QUOTE]), broker,
                ExecutionConfig(
                    order_timeout_sec=0, poll_interval_sec=0,
                    skip_sell_when_limit_down=True,
                ),
            )

            result = engine.execute(self._make_signal("sig-legacy-skip"))

            self.assertEqual(result.status, ExecutionStatus.SKIPPED_LIMIT_DOWN)
            self.assertEqual(broker.submitted, [])
            store.close()

    def test_queue_order_query_error_halts_later_signals(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            store = SQLiteExecutionStore(Path(tmpdir) / "state.db")
            broker = FakeSnapshotErrorBroker(
                {}, available_positions={"000001.XSHE": 1000},
            )
            market_data = FakeMarketData([self.LOCKED_QUOTE, self.LOCKED_QUOTE])
            engine = self._make_engine(store, market_data, broker)

            first = engine.execute(self._make_signal("sig-sell-query-uncertain"))
            second = engine.execute(self._make_signal("sig-after-sell-query-uncertain"))

            self.assertEqual(first.status, ExecutionStatus.FAILED_BROKER)
            self.assertIn("manual MiniQMT reconciliation", first.message)
            self.assertEqual(second.status, ExecutionStatus.FAILED_BROKER)
            self.assertEqual(len(broker.submitted), 1)

    def test_queue_submission_uncertainty_halts_later_signals(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            store = SQLiteExecutionStore(Path(tmpdir) / "state.db")
            broker = FakeDirectRejectBroker(
                {},
                [BrokerSubmissionUncertain("QMT queue submission state uncertain")],
                available_positions={"000001.XSHE": 1000},
            )
            market = FakeMarketData([self.LOCKED_QUOTE])
            engine = self._make_engine(store, market, broker)

            first = engine.execute(self._make_signal("sig-queue-submit-uncertain"))
            second = engine.execute(
                TradeSignal(
                    signal_id="sig-after-queue-submit-uncertain",
                    strategy_id="harvester",
                    action=Action.BUY,
                    code="000002.XSHE",
                    amount=1000,
                    reference_price=10.0,
                    created_at="2026-08-03 09:30:00",
                )
            )

            self.assertEqual(first.status, ExecutionStatus.FAILED_BROKER)
            self.assertIn("manual MiniQMT reconciliation", first.message)
            self.assertEqual(second.status, ExecutionStatus.FAILED_BROKER)
            self.assertIn("manual MiniQMT reconciliation", second.message)
            self.assertEqual(market.queries, ["000001.XSHE"])
            self.assertEqual(len(broker.submit_calls), 1)
            store.close()


class QueueLimitUpBuyTests(unittest.TestCase):
    """涨停排队买入：挂涨停价保留队列位置，截止前不撤不重挂。"""

    LOCKED_QUOTE = Quote(
        last_price=11.0,
        ask1=None,
        bid1=11.0,
        high_limit=11.0,
        low_limit=9.0,
    )

    def setUp(self):
        self._original_deadline_fn = executor_module._seconds_until_queue_buy_deadline
        self._original_poll_interval = executor_module._QUEUE_BUY_POLL_INTERVAL_SEC
        executor_module._seconds_until_queue_buy_deadline = lambda _deadline: 60.0
        executor_module._QUEUE_BUY_POLL_INTERVAL_SEC = 0.0

    def tearDown(self):
        executor_module._seconds_until_queue_buy_deadline = self._original_deadline_fn
        executor_module._QUEUE_BUY_POLL_INTERVAL_SEC = self._original_poll_interval

    @staticmethod
    def _make_signal(signal_id="sig-queue-buy") -> TradeSignal:
        return TradeSignal(
            signal_id=signal_id,
            strategy_id="harvester",
            action=Action.BUY,
            code="000001.XSHE",
            amount=1000,
            reference_price=10.0,
            created_at="2026-08-03 09:30:00",
        )

    @staticmethod
    def _make_engine(store, market_data, broker, **config_kwargs):
        defaults = dict(
            limit_up_buy_mode="queue",
            order_timeout_sec=0,
            max_attempts=3,
            max_total_duration_sec=15,
            poll_interval_sec=0,
        )
        defaults.update(config_kwargs)
        return OrderExecutionEngine(store, market_data, broker, ExecutionConfig(**defaults))

    def test_confirmed_limit_up_queues_one_order_until_full_fill(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            store = SQLiteExecutionStore(Path(tmpdir) / "state.db")
            broker = FakeBroker(
                {
                    "order-1": [
                        OrderSnapshot("order-1", BrokerOrderStatus.OPEN, 0),
                        OrderSnapshot("order-1", BrokerOrderStatus.OPEN, 0),
                        OrderSnapshot("order-1", BrokerOrderStatus.FILLED, 1000),
                    ]
                }
            )
            engine = self._make_engine(store, FakeMarketData([self.LOCKED_QUOTE]), broker)

            result = engine.execute(self._make_signal())

            self.assertEqual(result.status, ExecutionStatus.FILLED)
            self.assertEqual(result.filled_qty, 1000)
            self.assertEqual(broker.submitted, [("order-1", 1000, 11.0)])
            self.assertEqual(broker.canceled, [])

    def test_empty_ask_without_confirmed_limit_up_uses_normal_pricing(self):
        quote = Quote(
            last_price=10.0,
            ask1=None,
            bid1=10.0,
            high_limit=11.0,
            low_limit=9.0,
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            store = SQLiteExecutionStore(Path(tmpdir) / "state.db")
            broker = FakeBroker(
                {"order-1": [OrderSnapshot("order-1", BrokerOrderStatus.FILLED, 1000)]}
            )
            engine = self._make_engine(store, FakeMarketData([quote]), broker)

            result = engine.execute(self._make_signal("sig-not-limit-up"))

            self.assertEqual(result.status, ExecutionStatus.FILLED)
            self.assertEqual(broker.submitted, [("order-1", 1000, 10.03)])

    def test_one_tick_tolerance_confirms_limit_up(self):
        quote = Quote(
            last_price=10.98,
            ask1=None,
            bid1=10.99,
            high_limit=11.0,
            low_limit=9.0,
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            store = SQLiteExecutionStore(Path(tmpdir) / "state.db")
            broker = FakeBroker(
                {"order-1": [OrderSnapshot("order-1", BrokerOrderStatus.FILLED, 1000)]}
            )
            engine = self._make_engine(store, FakeMarketData([quote]), broker)

            result = engine.execute(self._make_signal("sig-limit-tolerance"))

            self.assertEqual(result.status, ExecutionStatus.FILLED)
            self.assertEqual(broker.submitted[0][2], 11.0)

    def test_queue_expires_and_cancels_once_at_deadline(self):
        executor_module._seconds_until_queue_buy_deadline = lambda _deadline: 0.03
        executor_module._QUEUE_BUY_POLL_INTERVAL_SEC = 0.01
        with tempfile.TemporaryDirectory() as tmpdir:
            store = SQLiteExecutionStore(Path(tmpdir) / "state.db")
            broker = FakeBroker(
                {"order-1": [OrderSnapshot("order-1", BrokerOrderStatus.OPEN, 0)]}
            )
            engine = self._make_engine(store, FakeMarketData([self.LOCKED_QUOTE]), broker)

            result = engine.execute(self._make_signal("sig-queue-buy-expired"))

            self.assertEqual(result.status, ExecutionStatus.LIMIT_UP_QUEUE_EXPIRED)
            self.assertEqual(result.filled_qty, 0)
            self.assertEqual(broker.canceled, ["order-1"])
            self.assertEqual(
                store.get_signal(result.signal_id).status,
                ExecutionStatus.LIMIT_UP_QUEUE_EXPIRED,
            )

    def test_partial_fill_stays_on_original_order_until_deadline(self):
        executor_module._seconds_until_queue_buy_deadline = lambda _deadline: 0.03
        executor_module._QUEUE_BUY_POLL_INTERVAL_SEC = 0.01
        with tempfile.TemporaryDirectory() as tmpdir:
            store = SQLiteExecutionStore(Path(tmpdir) / "state.db")
            broker = FakeBroker(
                {
                    "order-1": [
                        OrderSnapshot("order-1", BrokerOrderStatus.PARTIALLY_FILLED, 300),
                    ]
                },
                cancel_snapshots_by_order={
                    "order-1": [OrderSnapshot("order-1", BrokerOrderStatus.CANCELED, 300)]
                },
            )
            engine = self._make_engine(store, FakeMarketData([self.LOCKED_QUOTE]), broker)

            result = engine.execute(self._make_signal("sig-queue-buy-partial"))

            self.assertEqual(result.status, ExecutionStatus.PARTIALLY_FILLED_TIMEOUT)
            self.assertEqual(result.filled_qty, 300)
            self.assertEqual(len(broker.submitted), 1)
            self.assertEqual(broker.canceled, ["order-1"])

    def test_queue_capacity_guard_skips_without_order(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            store = SQLiteExecutionStore(Path(tmpdir) / "state.db")
            broker = FakeBroker({})
            engine = self._make_engine(
                store,
                FakeMarketData([self.LOCKED_QUOTE]),
                broker,
                max_concurrent_queue_buys=0,
            )

            result = engine.execute(self._make_signal("sig-queue-buy-capacity"))

            self.assertEqual(result.status, ExecutionStatus.SKIPPED_LIMIT_UP)
            self.assertEqual(broker.submitted, [])

    def test_recoverable_queue_rejection_refreshes_into_normal_order(self):
        reopened_quote = Quote(
            last_price=10.90,
            ask1=10.91,
            bid1=10.90,
            high_limit=11.0,
            low_limit=9.0,
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            store = SQLiteExecutionStore(Path(tmpdir) / "state.db")
            broker = FakeBroker(
                {
                    "order-1": [
                        OrderSnapshot(
                            "order-1",
                            BrokerOrderStatus.REJECTED,
                            rejection_reason="订单价格超出范围",
                            rejection_kind=BrokerRejectionKind.PRICE,
                        )
                    ],
                    "order-2": [OrderSnapshot("order-2", BrokerOrderStatus.FILLED, 1000)],
                }
            )
            engine = self._make_engine(
                store,
                FakeMarketData([self.LOCKED_QUOTE, reopened_quote]),
                broker,
            )

            result = engine.execute(self._make_signal("sig-queue-buy-reject"))

            self.assertEqual(result.status, ExecutionStatus.FILLED)
            self.assertEqual(result.attempts, 2)
            self.assertEqual(broker.submitted[0][2], 11.0)
            self.assertEqual(broker.submitted[1][2], 10.93)

    def test_queue_cancel_uncertainty_halts_later_signals(self):
        executor_module._seconds_until_queue_buy_deadline = lambda _deadline: 0.01
        executor_module._QUEUE_BUY_POLL_INTERVAL_SEC = 0.01
        with tempfile.TemporaryDirectory() as tmpdir:
            store = SQLiteExecutionStore(Path(tmpdir) / "state.db")
            broker = FakeBroker(
                {"order-1": [OrderSnapshot("order-1", BrokerOrderStatus.OPEN, 0)]},
                cancel_snapshots_by_order={
                    "order-1": [OrderSnapshot("order-1", BrokerOrderStatus.OPEN, 0)]
                },
            )
            market_data = FakeMarketData([self.LOCKED_QUOTE, 10.0])
            engine = self._make_engine(
                store,
                market_data,
                broker,
                cancel_confirm_timeout_sec=0,
            )

            first = engine.execute(self._make_signal("sig-queue-buy-uncertain"))
            second = engine.execute(self._make_signal("sig-after-queue-uncertain"))

            self.assertEqual(first.status, ExecutionStatus.FAILED_BROKER)
            self.assertIn("manual MiniQMT reconciliation", first.message)
            self.assertEqual(second.status, ExecutionStatus.FAILED_BROKER)
            self.assertEqual(len(broker.submitted), 1)

    def test_queue_order_query_error_halts_later_signals(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            store = SQLiteExecutionStore(Path(tmpdir) / "state.db")
            broker = FakeSnapshotErrorBroker({})
            market_data = FakeMarketData([self.LOCKED_QUOTE, 10.0])
            engine = self._make_engine(store, market_data, broker)

            first = engine.execute(self._make_signal("sig-queue-query-uncertain"))
            second = engine.execute(self._make_signal("sig-after-queue-query-uncertain"))

            self.assertEqual(first.status, ExecutionStatus.FAILED_BROKER)
            self.assertIn("manual MiniQMT reconciliation", first.message)
            self.assertEqual(second.status, ExecutionStatus.FAILED_BROKER)
            self.assertEqual(len(broker.submitted), 1)

if __name__ == "__main__":
    unittest.main()

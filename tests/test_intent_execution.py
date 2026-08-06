import shutil
import tempfile
import unittest
from pathlib import Path

import miniqmt_follower.executor as executor_module
from miniqmt_follower.executor import OrderExecutionEngine
from miniqmt_follower.models import (
    Action,
    BrokerOrderStatus,
    ExecutionConfig,
    ExecutionStatus,
    OrderSnapshot,
    Quote,
    TradeSignal,
)
from miniqmt_follower.store import SQLiteExecutionStore
from tests.test_executor import FakeBroker, FakeMarketData


executor_module._seconds_until_market_open = lambda: 0.0


class IntentQuantityExecutionTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls._tmpdir = tempfile.mkdtemp(prefix="intent-exec-")
        cls._db_counter = 0

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls._tmpdir, ignore_errors=True)

    def _engine(self, broker, market_data, config=None):
        self._db_counter += 1
        store = SQLiteExecutionStore(Path(self._tmpdir) / f"state-{self._db_counter}.db")
        return OrderExecutionEngine(
            store=store,
            market_data=market_data,
            broker=broker,
            config=config or ExecutionConfig(
                order_timeout_sec=0, max_attempts=2, poll_interval_sec=0,
            ),
        )

    def test_sell_all_executes_full_available_position(self):
        broker = FakeBroker(
            {"order-1": [OrderSnapshot("order-1", BrokerOrderStatus.FILLED, 500)]},
            available_positions={"000001.XSHE": 500},
        )
        market_data = FakeMarketData([Quote(last_price=10.0, bid1=9.99)])
        engine = self._engine(broker, market_data)
        signal = TradeSignal(
            signal_id="s-sell-all", strategy_id="harvester", action=Action.SELL,
            code="000001.XSHE", amount=0, reference_price=10.0,
            created_at="2026-08-06 09:28:00", quantity_mode="sell_all",
        )
        result = engine.execute(signal)
        self.assertEqual(result.status, ExecutionStatus.FILLED)
        self.assertEqual(result.requested_qty, 500)
        self.assertEqual(broker.submitted[0][1], 500)

    def test_sell_all_with_no_position_skips_without_order(self):
        broker = FakeBroker({}, available_positions={"000001.XSHE": 0})
        market_data = FakeMarketData([Quote(last_price=10.0, bid1=9.99)])
        engine = self._engine(broker, market_data)
        signal = TradeSignal(
            signal_id="s-sell-none", strategy_id="harvester", action=Action.SELL,
            code="000001.XSHE", amount=0, reference_price=10.0,
            created_at="2026-08-06 09:28:00", quantity_mode="sell_all",
        )
        result = engine.execute(signal)
        self.assertEqual(result.status, ExecutionStatus.SKIPPED_NO_POSITION)
        self.assertEqual(broker.submitted, [])

    def test_sell_half_rounds_to_lot(self):
        broker = FakeBroker(
            {"order-1": [OrderSnapshot("order-1", BrokerOrderStatus.FILLED, 500)]},
            available_positions={"000001.XSHE": 1100},
        )
        market_data = FakeMarketData([Quote(last_price=10.0, bid1=9.99)])
        engine = self._engine(broker, market_data)
        signal = TradeSignal(
            signal_id="s-half", strategy_id="harvester", action=Action.SELL,
            code="000001.XSHE", amount=0, reference_price=10.0,
            created_at="2026-08-06 10:30:00", quantity_mode="sell_half",
        )
        result = engine.execute(signal)
        self.assertEqual(result.status, ExecutionStatus.FILLED)
        self.assertEqual(broker.submitted[0][1], 500)

    def test_auto_buy_computes_shares_from_real_funds(self):
        broker = FakeBroker(
            {"order-1": [OrderSnapshot("order-1", BrokerOrderStatus.FILLED, 2000)]},
            available_cash=100_000,
            available_total_assets=1_000_000,
            available_positions={},
        )
        market_data = FakeMarketData(
            [Quote(last_price=10.0, ask1=10.01), Quote(last_price=10.0, ask1=10.01)]
        )
        engine = self._engine(
            broker,
            market_data,
            ExecutionConfig(
                order_timeout_sec=0, max_attempts=2, poll_interval_sec=0,
                max_single_position_pct=0.2,
            ),
        )
        signal = TradeSignal(
            signal_id="s-buy-auto", strategy_id="harvester", action=Action.BUY,
            code="600000.XSHG", amount=0, reference_price=12.3,
            created_at="2026-08-06 09:28:00", quantity_mode="auto_buy",
            budget_group_size=5,
        )
        result = engine.execute(signal)
        self.assertEqual(result.status, ExecutionStatus.FILLED)
        self.assertEqual(broker.submitted[0][1], 2000)


if __name__ == "__main__":
    unittest.main()

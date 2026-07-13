import threading
import unittest
from types import SimpleNamespace

from qmt_follower.adapters.qmt import QmtBrokerAdapter
from qmt_follower.models import Action, BrokerOrderStatus, OrderSnapshot, TradeSignal


class _BlockingTrader:
    def __init__(self, blocking_method):
        self.blocking_method = blocking_method
        self.query_entered = threading.Event()
        self.release_query = threading.Event()
        self.submit_entered = threading.Event()

    def _block_if_selected(self, method):
        if self.blocking_method == method:
            self.query_entered.set()
            self.release_query.wait(timeout=1.0)

    def query_stock_asset(self, _account):
        self._block_if_selected("asset")
        return SimpleNamespace(m_dAvailable=100000.0)

    def query_stock_positions(self, _account):
        self._block_if_selected("positions")
        return []

    def query_stock_orders(self, _account):
        self._block_if_selected("orders")
        return []

    def order_stock(self, *_args):
        self.submit_entered.set()
        return 123456


class _PositionTrader:
    def __init__(self, code, available_qty):
        self.code = code
        self.available_qty = available_qty
        self.position_queries = 0

    def query_stock_positions(self, _account):
        self.position_queries += 1
        return [SimpleNamespace(stock_code=self.code, can_use_volume=self.available_qty)]


class _CancelTrader:
    @staticmethod
    def cancel_order_stock(_account, _order_id):
        return 0


class QmtAdapterConcurrencyTests(unittest.TestCase):
    def test_position_is_queried_fresh_for_every_call(self):
        trader = _PositionTrader("517110.SH", 13700)
        adapter = self._build_adapter(trader)

        first = adapter.query_available_position("517110.XSHG")
        second = adapter.query_available_position("517110.XSHG")

        self.assertEqual(first, 13700)
        self.assertEqual(second, 13700)
        self.assertEqual(trader.position_queries, 2)

    def test_cancel_request_invalidates_cached_snapshot(self):
        adapter = self._build_adapter(_CancelTrader())
        adapter._orders_cache["123456"] = OrderSnapshot(
            "123456", BrokerOrderStatus.PARTIALLY_FILLED, filled_qty=400,
        )
        adapter._orders_cache_time = 123.0

        adapter.cancel_order("123456")

        self.assertNotIn("123456", adapter._orders_cache)
        self.assertEqual(adapter._orders_cache_time, 0.0)

    def test_qmt_queries_and_order_submission_share_one_lock(self):
        for query_name in ("asset", "positions", "orders"):
            with self.subTest(query=query_name):
                trader = _BlockingTrader(query_name)
                adapter = self._build_adapter(trader)
                query_call = {
                    "asset": adapter.query_available_cash,
                    "positions": lambda: adapter.query_available_position("159309.XSHE"),
                    "orders": lambda: adapter.get_order_snapshot("123456"),
                }[query_name]
                errors = []

                query_thread = threading.Thread(target=self._run, args=(query_call, errors))
                submit_thread = threading.Thread(
                    target=self._run,
                    args=(lambda: adapter.submit_order(self._signal(), 100, 1.26), errors),
                )

                query_thread.start()
                self.assertTrue(trader.query_entered.wait(timeout=1.0))
                submit_thread.start()
                submitted_while_query_active = trader.submit_entered.wait(timeout=0.1)
                trader.release_query.set()
                query_thread.join(timeout=1.0)
                submit_thread.join(timeout=1.0)

                self.assertFalse(query_thread.is_alive())
                self.assertFalse(submit_thread.is_alive())
                self.assertEqual(errors, [])
                self.assertFalse(submitted_while_query_active)

    @staticmethod
    def _run(call, errors):
        try:
            call()
        except Exception as exc:
            errors.append(exc)

    @staticmethod
    def _build_adapter(trader):
        adapter = QmtBrokerAdapter.__new__(QmtBrokerAdapter)
        adapter._trader = trader
        adapter._account = object()
        adapter._strategy_name = "test"
        adapter._STOCK_BUY = 23
        adapter._STOCK_SELL = 24
        adapter._FIX_PRICE = 11
        adapter._trading_lock = threading.Lock()
        adapter._positions_cache = {}
        adapter._positions_cache_time = 0.0
        adapter._orders_cache = {}
        adapter._orders_cache_time = 0.0
        return adapter

    @staticmethod
    def _signal():
        return TradeSignal(
            signal_id="concurrency-test",
            strategy_id="stable_discount_hunter",
            action=Action.BUY,
            code="159309.XSHE",
            amount=100,
            reference_price=1.26,
            created_at="2026-07-10 09:30:11",
        )


if __name__ == "__main__":
    unittest.main()

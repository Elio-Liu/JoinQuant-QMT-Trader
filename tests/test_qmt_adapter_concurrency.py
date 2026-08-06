import sys
import threading
import unittest
from types import ModuleType
from types import SimpleNamespace
from unittest.mock import patch

from miniqmt_follower.adapters.qmt import QmtBrokerAdapter
from miniqmt_follower.config import TradingConfig
from miniqmt_follower.models import (
    Action,
    BrokerOrderRejected,
    BrokerOrderStatus,
    BrokerRejectionKind,
    BrokerSubmissionUncertain,
    OrderSnapshot,
    TradeSignal,
)


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


class _OrdersTrader:
    def __init__(self, orders):
        self.orders = orders

    def query_stock_orders(self, _account):
        return self.orders


class _RejectedTrader:
    @staticmethod
    def order_stock(*_args):
        return -1


class _ExplodingTrader:
    @staticmethod
    def order_stock(*_args):
        raise ConnectionError("QMT connection dropped")


class _CallbackBase:
    pass


class _InitTrader:
    last_instance = None

    def __init__(self, _path, _session_id):
        self.callback = None
        _InitTrader.last_instance = self

    def register_callback(self, callback):
        self.callback = callback

    @staticmethod
    def start():
        return None

    @staticmethod
    def connect():
        return 0

    @staticmethod
    def subscribe(_account):
        return 0


class QmtAdapterConcurrencyTests(unittest.TestCase):
    def test_constructor_registers_lightweight_order_error_callback(self):
        xtconstant = ModuleType("xtquant.xtconstant")
        xtconstant.FIX_PRICE = 11
        xtconstant.STOCK_BUY = 23
        xtconstant.STOCK_SELL = 24
        xttrader = ModuleType("xtquant.xttrader")
        xttrader.XtQuantTrader = _InitTrader
        xttrader.XtQuantTraderCallback = _CallbackBase
        xttype = ModuleType("xtquant.xttype")
        xttype.StockAccount = lambda account_id: SimpleNamespace(account_id=account_id)

        with patch.dict(
            sys.modules,
            {
                "xtquant.xtconstant": xtconstant,
                "xtquant.xttrader": xttrader,
                "xtquant.xttype": xttype,
            },
        ):
            adapter = QmtBrokerAdapter(
                TradingConfig(
                    enabled=True,
                    account_id="test-account",
                    miniqmt_path="C:/test/userdata_mini",
                    session_id=7,
                )
            )

        callback = _InitTrader.last_instance.callback
        self.assertIsNotNone(callback)
        callback.on_order_error(
            SimpleNamespace(
                order_id=-1,
                error_id=110001,
                error_msg="委托价不正确",
                order_remark="sig-from-callback",
            )
        )
        cached = adapter._take_order_error_by_remark(
            "sig-from-callback", wait_timeout=0,
        )
        self.assertEqual(cached.reason, "委托价不正确")

    def test_rejected_order_snapshot_preserves_status_message_and_classification(self):
        trader = _OrdersTrader(
            [
                SimpleNamespace(
                    order_id=123456,
                    order_status=57,
                    traded_volume=0,
                    status_msg="委托价不正确",
                )
            ]
        )
        adapter = self._build_adapter(trader)

        adapter._refresh_orders_cache()

        snapshot = adapter._orders_cache["123456"]
        self.assertEqual(snapshot.status, BrokerOrderStatus.REJECTED)
        self.assertEqual(snapshot.rejection_reason, "委托价不正确")
        self.assertEqual(snapshot.rejection_kind, BrokerRejectionKind.PRICE)

    def test_rejected_snapshot_uses_callback_error_when_status_message_is_empty(self):
        trader = _OrdersTrader(
            [
                SimpleNamespace(
                    order_id=123456,
                    order_status=57,
                    traded_volume=0,
                    status_msg="",
                )
            ]
        )
        adapter = self._build_adapter(trader)
        adapter._cache_order_error(
            SimpleNamespace(
                order_id=123456,
                error_id=110001,
                error_msg="委托价不正确",
                order_remark="concurrency-test",
            )
        )

        adapter._refresh_orders_cache()

        snapshot = adapter._orders_cache["123456"]
        self.assertEqual(snapshot.rejection_reason, "委托价不正确")
        self.assertEqual(snapshot.rejection_code, "110001")
        self.assertEqual(snapshot.rejection_kind, BrokerRejectionKind.PRICE)

    def test_negative_order_id_uses_callback_reason_as_confirmed_rejection(self):
        adapter = self._build_adapter(_RejectedTrader())
        adapter._cache_order_error(
            SimpleNamespace(
                order_id=-1,
                error_id=110001,
                error_msg="订单价格超出范围",
                order_remark="concurrency-test",
            )
        )

        with self.assertRaises(BrokerOrderRejected) as raised:
            adapter.submit_order(self._signal(), 100, 1.26)

        self.assertEqual(raised.exception.reason, "订单价格超出范围")
        self.assertEqual(raised.exception.error_code, "110001")
        self.assertEqual(raised.exception.kind, BrokerRejectionKind.PRICE)
        self.assertEqual(adapter._orders_cache, {})

    def test_negative_order_id_without_callback_is_unknown_confirmed_rejection(self):
        adapter = self._build_adapter(_RejectedTrader())

        with self.assertRaises(BrokerOrderRejected) as raised:
            adapter.submit_order(self._signal(), 100, 1.26)

        self.assertEqual(raised.exception.kind, BrokerRejectionKind.UNKNOWN)
        self.assertIn("returned -1", raised.exception.reason)

    def test_order_stock_exception_is_submission_uncertain(self):
        adapter = self._build_adapter(_ExplodingTrader())

        with self.assertRaises(BrokerSubmissionUncertain) as raised:
            adapter.submit_order(self._signal(), 100, 1.26)

        self.assertIn("QMT connection dropped", str(raised.exception))

    def test_order_error_callback_cache_does_not_wait_for_trading_lock(self):
        trader = _BlockingTrader("asset")
        adapter = self._build_adapter(trader)
        errors = []
        query_thread = threading.Thread(
            target=self._run,
            args=(adapter.query_available_cash, errors),
        )
        query_thread.start()
        self.assertTrue(trader.query_entered.wait(timeout=1.0))

        callback_finished = threading.Event()

        def cache_error():
            adapter._cache_order_error(
                SimpleNamespace(
                    order_id=-1,
                    error_id=1,
                    error_msg="委托价不正确",
                    order_remark="concurrency-test",
                )
            )
            callback_finished.set()

        callback_thread = threading.Thread(target=cache_error)
        callback_thread.start()
        completed_while_query_active = callback_finished.wait(timeout=0.1)
        trader.release_query.set()
        query_thread.join(timeout=1.0)
        callback_thread.join(timeout=1.0)

        self.assertTrue(completed_while_query_active)
        self.assertEqual(errors, [])

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
        adapter._order_error_lock = threading.Lock()
        adapter._order_errors_by_remark = {}
        adapter._order_errors_by_id = {}
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

import unittest

from qmt_follower.adapters.qmt import (
    _qmt_order_status_to_broker_status,
    jq_code_to_qmt_code,
)
from qmt_follower.models import BrokerOrderStatus


class QmtCodeMappingTests(unittest.TestCase):
    def test_shenzhen_stock_code_conversion(self):
        self.assertEqual(jq_code_to_qmt_code("000001.XSHE"), "000001.SZ")
        self.assertEqual(jq_code_to_qmt_code("159915.XSHE"), "159915.SZ")

    def test_shanghai_stock_code_conversion(self):
        self.assertEqual(jq_code_to_qmt_code("510300.XSHG"), "510300.SH")
        self.assertEqual(jq_code_to_qmt_code("600000.XSHG"), "600000.SH")

    def test_already_qmt_format_passes_through(self):
        self.assertEqual(jq_code_to_qmt_code("000001.SZ"), "000001.SZ")
        self.assertEqual(jq_code_to_qmt_code("510300.SH"), "510300.SH")

    def test_unknown_format_passes_through(self):
        self.assertEqual(jq_code_to_qmt_code("000001"), "000001")
        self.assertEqual(jq_code_to_qmt_code("AAPL"), "AAPL")


class QmtOrderStatusMappingTests(unittest.TestCase):
    def test_unreported_wait_reported_reported_are_open(self):
        for status_int in (48, 49, 50):  # ORDER_UNREPORTED, ORDER_WAIT_REPORTING, ORDER_REPORTED
            self.assertEqual(
                _qmt_order_status_to_broker_status(status_int),
                BrokerOrderStatus.OPEN,
            )

    def test_partial_fill(self):
        self.assertEqual(
            _qmt_order_status_to_broker_status(55),  # ORDER_PART_SUCC
            BrokerOrderStatus.PARTIALLY_FILLED,
        )

    def test_filled(self):
        self.assertEqual(
            _qmt_order_status_to_broker_status(56),  # ORDER_SUCCEEDED
            BrokerOrderStatus.FILLED,
        )

    def test_cancel_pending_variants_remain_nonterminal(self):
        self.assertEqual(
            _qmt_order_status_to_broker_status(51),  # ORDER_REPORTED_CANCEL: 已报待撤
            BrokerOrderStatus.OPEN,
        )
        self.assertEqual(
            _qmt_order_status_to_broker_status(52),  # ORDER_PARTSUCC_CANCEL: 部成待撤
            BrokerOrderStatus.PARTIALLY_FILLED,
        )

    def test_canceled_variants(self):
        for status_int in (53, 54):  # ORDER_PART_CANCEL: 部撤, ORDER_CANCELED: 已撤
            self.assertEqual(
                _qmt_order_status_to_broker_status(status_int),
                BrokerOrderStatus.CANCELED,
            )

    def test_rejected(self):
        self.assertEqual(
            _qmt_order_status_to_broker_status(57),  # ORDER_JUNK
            BrokerOrderStatus.REJECTED,
        )

    def test_unknown_status_defaults_to_open(self):
        self.assertEqual(
            _qmt_order_status_to_broker_status(999),
            BrokerOrderStatus.OPEN,
        )


if __name__ == "__main__":
    unittest.main()

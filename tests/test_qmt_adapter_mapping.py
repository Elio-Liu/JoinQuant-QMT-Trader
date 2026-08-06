import unittest

from miniqmt_follower.adapters.qmt import (
    _qmt_order_status_to_broker_status,
    classify_qmt_rejection,
    jq_code_to_qmt_code,
)
from miniqmt_follower.models import BrokerOrderStatus, BrokerRejectionKind


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


class QmtRejectionClassificationTests(unittest.TestCase):
    def test_price_rejections_are_recoverable(self):
        for reason in ("委托价不正确", "订单价格超出范围", "价格笼子校验失败"):
            with self.subTest(reason=reason):
                self.assertEqual(classify_qmt_rejection(reason), BrokerRejectionKind.PRICE)

    def test_resource_rejections_refresh_account_resources(self):
        for reason in ("可用资金不足", "可卖数量不足", "委托数量不正确"):
            with self.subTest(reason=reason):
                self.assertEqual(classify_qmt_rejection(reason), BrokerRejectionKind.RESOURCE)

    def test_transient_rejections_are_retried(self):
        for reason in ("交易通道繁忙，请稍后重试", "柜台忙", "请求频率过高"):
            with self.subTest(reason=reason):
                self.assertEqual(classify_qmt_rejection(reason), BrokerRejectionKind.TRANSIENT)

    def test_permanent_rejections_stop_immediately(self):
        for reason in (
            "证券停牌",
            "账户状态异常",
            "无交易权限",
            "股东账户不存在",
            "该证券禁止买入",
            "该客户未开通创业板交易权限",
            "股东代码不存在",
            "证券账户未指定",
            "该证券禁止交易",
            "账号未登录",
        ):
            with self.subTest(reason=reason):
                self.assertEqual(classify_qmt_rejection(reason), BrokerRejectionKind.HARD_STOP)

    def test_unrecognized_confirmed_rejection_defaults_to_retry(self):
        self.assertEqual(
            classify_qmt_rejection("柜台返回未识别文本"),
            BrokerRejectionKind.UNKNOWN,
        )


if __name__ == "__main__":
    unittest.main()

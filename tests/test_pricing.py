import unittest

from qmt_follower.models import Action, ExecutionConfig, Quote, TradeSignal
from qmt_follower.pricing import PriceDeviationError, calculate_order_price, tick_size_for


def _signal(action=Action.BUY, reference_price=10.0, code="000001.XSHE"):
    return TradeSignal(
        signal_id="sig-1",
        strategy_id="hunter",
        action=action,
        code=code,
        amount=1000,
        reference_price=reference_price,
        created_at="2026-06-08 09:30:00",
    )


class SlippagePricingTests(unittest.TestCase):
    def test_buy_price_applies_fixed_positive_slippage(self):
        config = ExecutionConfig(buy_slippage_pct=0.003, sell_slippage_pct=0.003)

        price = calculate_order_price(_signal(Action.BUY), Quote(last_price=10.0), config)

        self.assertEqual(price, 10.03)

    def test_sell_price_applies_fixed_negative_slippage(self):
        config = ExecutionConfig(buy_slippage_pct=0.003, sell_slippage_pct=0.003)

        price = calculate_order_price(_signal(Action.SELL), Quote(last_price=10.0), config)

        self.assertEqual(price, 9.97)

    def test_rejects_price_too_far_from_reference(self):
        config = ExecutionConfig(max_deviation_from_signal_price_pct=0.02)

        with self.assertRaises(PriceDeviationError):
            calculate_order_price(_signal(Action.BUY), Quote(last_price=10.5), config)


class BookPricingTests(unittest.TestCase):
    def test_buy_uses_ask1_plus_tick_offset(self):
        config = ExecutionConfig(pricing_mode="book", book_tick_offset=2)
        quote = Quote(last_price=10.0, ask1=10.02, bid1=10.01)

        self.assertEqual(calculate_order_price(_signal(Action.BUY), quote, config), 10.04)

    def test_sell_uses_bid1_minus_tick_offset(self):
        config = ExecutionConfig(pricing_mode="book", book_tick_offset=2)
        quote = Quote(last_price=10.0, ask1=10.02, bid1=10.01)

        self.assertEqual(calculate_order_price(_signal(Action.SELL), quote, config), 9.99)

    def test_buy_falls_back_to_slippage_when_ask_missing(self):
        """涨停时卖一无档 → 回退最新价滑点定价。"""
        config = ExecutionConfig(pricing_mode="book", buy_slippage_pct=0.003)
        quote = Quote(last_price=10.0, ask1=None, bid1=10.0)

        self.assertEqual(calculate_order_price(_signal(Action.BUY), quote, config), 10.03)

    def test_sell_falls_back_to_slippage_when_bid_missing(self):
        """跌停时买一无档 → 回退最新价滑点定价。"""
        config = ExecutionConfig(pricing_mode="book", sell_slippage_pct=0.003)
        quote = Quote(last_price=10.0, ask1=10.0, bid1=None)

        self.assertEqual(calculate_order_price(_signal(Action.SELL), quote, config), 9.97)

    def test_deviation_guard_checks_book_price_not_last(self):
        """book 模式下偏离保护看对手盘价: 最新价还在阈值内, 但卖一已飞出 2% → 拒绝。"""
        config = ExecutionConfig(pricing_mode="book", max_deviation_from_signal_price_pct=0.02)
        quote = Quote(last_price=10.1, ask1=10.5, bid1=10.1)

        with self.assertRaises(PriceDeviationError):
            calculate_order_price(_signal(Action.BUY), quote, config)


class TickSizeTests(unittest.TestCase):
    def test_stock_tick_is_one_cent(self):
        self.assertEqual(tick_size_for("000001.XSHE"), 0.01)
        self.assertEqual(tick_size_for("600519.XSHG"), 0.01)
        self.assertEqual(tick_size_for("600519.SH"), 0.01)

    def test_fund_tick_is_one_tenth_cent(self):
        self.assertEqual(tick_size_for("510300.XSHG"), 0.001)
        self.assertEqual(tick_size_for("511880.XSHG"), 0.001)
        self.assertEqual(tick_size_for("159915.XSHE"), 0.001)
        self.assertEqual(tick_size_for("510300.SH"), 0.001)

    def test_etf_book_buy_uses_milli_tick_offset(self):
        """ETF book 模式: 卖一 1.001 + 2 个 0.001 tick = 1.003, 不能被 round 到分。"""
        config = ExecutionConfig(pricing_mode="book", book_tick_offset=2)
        quote = Quote(last_price=1.000, ask1=1.001, bid1=1.000)

        price = calculate_order_price(_signal(code="510300.XSHG", reference_price=1.0), quote, config)

        self.assertEqual(price, 1.003)

    def test_etf_slippage_price_keeps_three_decimals(self):
        """ETF 滑点模式: 1.000*(1+0.003)=1.003 必须保留到厘, 旧版 round 到分会挂 1.00 买不到。"""
        config = ExecutionConfig(buy_slippage_pct=0.003)
        quote = Quote(last_price=1.000)

        price = calculate_order_price(_signal(code="510300.XSHG", reference_price=1.0), quote, config)

        self.assertEqual(price, 1.003)


if __name__ == "__main__":
    unittest.main()

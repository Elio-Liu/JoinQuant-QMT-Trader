import unittest
from contextlib import contextmanager

from miniqmt_follower import pricing as pricing_module
from miniqmt_follower.models import Action, ExecutionConfig, Quote, TradeSignal
from miniqmt_follower.pricing import calculate_order_price, tick_size_for


def setUpModule():
    # 测试与真实时钟解耦: 若测试恰好在 9:15~9:30 运行, 竞价排队定价会改写报价,
    # 破坏常规 book/slippage 用例的预期。默认关闭, 竞价用例单独用 _in_auction 打开。
    pricing_module._in_call_auction = lambda: False


@contextmanager
def _in_auction():
    """在上下文内把定价模块当成处于 9:15~9:30 竞价排队时段。"""
    pricing_module._in_call_auction = lambda: True
    try:
        yield
    finally:
        pricing_module._in_call_auction = lambda: False


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

    def test_buy_still_prices_when_market_ran_far_above_reference(self):
        """跟单一致性优先: 行情已拉离参考价 5%, 买单照常追, 不再拒单。

        旧实现在这里抛 PriceDeviationError, 结果是聚宽建了仓、实盘没建, 而且
        没有补单机制 —— 一次拦截换来长期持仓分叉。
        """
        config = ExecutionConfig(buy_slippage_pct=0.003)

        price = calculate_order_price(_signal(Action.BUY), Quote(last_price=10.5), config)

        self.assertEqual(price, round(10.5 * 1.003, 2))

    def test_sell_still_prices_when_market_ran_far_below_reference(self):
        """瀑布行情里价格离参考价越远越该卖, 不能被拦。"""
        config = ExecutionConfig(sell_slippage_pct=0.003)

        price = calculate_order_price(_signal(Action.SELL), Quote(last_price=9.0), config)

        self.assertEqual(price, round(9.0 * 0.997, 2))


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

    def test_book_buy_chases_ask_that_ran_away_from_reference(self):
        """卖一已飞出参考价 5%: 照样吃对手盘, 跟上模拟盘的建仓。"""
        config = ExecutionConfig(pricing_mode="book", book_tick_offset=2)
        quote = Quote(last_price=10.1, ask1=10.5, bid1=10.1)

        self.assertEqual(calculate_order_price(_signal(Action.BUY), quote, config), 10.52)


class AuctionQueuePricingTests(unittest.TestCase):
    """9:15~9:30 排队报价: 买 +pct / 卖 -pct, 强制夹在涨跌停内。"""

    # 昨收 10 元的主板股: 涨停 11.00, 跌停 9.00。
    LIMITS = {"high_limit": 11.0, "low_limit": 9.0}

    def _config(self, **overrides):
        # 竞价用例默认走 book, 用来证明竞价分支确实盖过了常规模式。
        base = dict(pricing_mode="book", book_tick_offset=3, auction_aggressive_pct=0.02)
        base.update(overrides)
        return ExecutionConfig(**base)

    def test_buy_queues_above_last_price(self):
        quote = Quote(last_price=10.0, ask1=10.02, bid1=10.0, **self.LIMITS)

        with _in_auction():
            price = calculate_order_price(_signal(Action.BUY), quote, self._config())

        # 10.0 * 1.02 = 10.20, 远高于 book 模式的 10.02+0.03=10.05
        self.assertEqual(price, 10.20)

    def test_sell_queues_below_last_price(self):
        quote = Quote(last_price=10.0, ask1=10.0, bid1=9.98, **self.LIMITS)

        with _in_auction():
            price = calculate_order_price(_signal(Action.SELL), quote, self._config())

        self.assertEqual(price, 9.80)

    def test_buy_price_clamped_to_high_limit(self):
        """接近涨停时 +2% 会越过涨停价, 必须夹回涨停价, 否则交易所废单。"""
        quote = Quote(last_price=10.95, ask1=10.96, bid1=10.95, **self.LIMITS)

        with _in_auction():
            price = calculate_order_price(
                _signal(Action.BUY, reference_price=10.95), quote, self._config()
            )

        self.assertEqual(price, 11.0)

    def test_sell_price_clamped_to_low_limit(self):
        """低开逼近跌停时 -2% 会跌穿跌停价, 必须夹回跌停价。"""
        quote = Quote(last_price=9.05, ask1=9.05, bid1=9.04, **self.LIMITS)

        with _in_auction():
            price = calculate_order_price(
                _signal(Action.SELL, reference_price=9.05), quote, self._config()
            )

        self.assertEqual(price, 9.0)

    def test_falls_back_when_limit_prices_unavailable(self):
        """取不到涨跌停价就无法封顶, 宁可排位靠后也回退常规定价。"""
        quote = Quote(last_price=10.0, ask1=10.02, bid1=9.98)

        with _in_auction():
            price = calculate_order_price(_signal(Action.BUY), quote, self._config())

        self.assertEqual(price, 10.05)  # book: 10.02 + 3 tick

    def test_disabled_by_zero_pct(self):
        quote = Quote(last_price=10.0, ask1=10.02, bid1=9.98, **self.LIMITS)

        with _in_auction():
            price = calculate_order_price(
                _signal(Action.BUY), quote, self._config(auction_aggressive_pct=0.0)
            )

        self.assertEqual(price, 10.05)

    def test_not_applied_outside_auction_window(self):
        """setUpModule 已把时段判定固定为 False, 常规时段必须走 book。"""
        quote = Quote(last_price=10.0, ask1=10.02, bid1=9.98, **self.LIMITS)

        price = calculate_order_price(_signal(Action.BUY), quote, self._config())

        self.assertEqual(price, 10.05)

    def test_prices_off_reference_still_queue(self):
        """竞价里行情已离参考价 5%: 照常排队报价, 唯一的封顶是涨跌停。"""
        quote = Quote(last_price=10.5, ask1=10.5, bid1=10.49, **self.LIMITS)

        with _in_auction():
            price = calculate_order_price(_signal(Action.BUY), quote, self._config())

        self.assertEqual(price, 10.71)  # 10.5 * 1.02, 未触及涨停 11.0

    def test_etf_auction_price_keeps_milli_tick(self):
        """ETF 报价单位 0.001: 1.000*1.02=1.020 不能被 round 到分。"""
        quote = Quote(last_price=1.000, ask1=1.001, bid1=1.000, high_limit=1.1, low_limit=0.9)

        with _in_auction():
            price = calculate_order_price(
                _signal(code="510300.XSHG", reference_price=1.0), quote, self._config()
            )

        self.assertEqual(price, 1.020)


class StockDynamicPriceCageTests(unittest.TestCase):
    """沪深 A 股委托价必须落在连续竞价动态有效申报范围内。"""

    def test_buy_uses_ask1_as_dynamic_cage_reference(self):
        config = ExecutionConfig(pricing_mode="book", book_tick_offset=100)
        quote = Quote(last_price=9.90, ask1=10.00, bid1=9.99, high_limit=11.0, low_limit=9.0)

        price = calculate_order_price(_signal(Action.BUY), quote, config)

        self.assertEqual(price, 10.20)

    def test_buy_falls_back_from_missing_ask_to_bid_then_last(self):
        config = ExecutionConfig(buy_slippage_pct=0.50)

        from_bid = calculate_order_price(
            _signal(Action.BUY),
            Quote(last_price=9.90, ask1=None, bid1=10.00, high_limit=11.0, low_limit=9.0),
            config,
        )
        from_last = calculate_order_price(
            _signal(Action.BUY),
            Quote(last_price=10.00, ask1=None, bid1=None, high_limit=11.0, low_limit=9.0),
            config,
        )

        self.assertEqual(from_bid, 10.20)
        self.assertEqual(from_last, 10.20)

    def test_buy_uses_ten_tick_floor_when_wider_than_two_percent(self):
        config = ExecutionConfig(buy_slippage_pct=0.50)
        quote = Quote(last_price=1.00, ask1=1.00, bid1=0.99, high_limit=1.2, low_limit=0.8)

        price = calculate_order_price(_signal(Action.BUY), quote, config)

        self.assertEqual(price, 1.10)

    def test_sell_uses_bid1_then_ask1_then_last_as_reference(self):
        config = ExecutionConfig(sell_slippage_pct=0.50)

        from_bid = calculate_order_price(
            _signal(Action.SELL),
            Quote(last_price=10.10, ask1=10.01, bid1=10.00, high_limit=11.0, low_limit=9.0),
            config,
        )
        from_ask = calculate_order_price(
            _signal(Action.SELL),
            Quote(last_price=10.10, ask1=10.00, bid1=None, high_limit=11.0, low_limit=9.0),
            config,
        )
        from_last = calculate_order_price(
            _signal(Action.SELL),
            Quote(last_price=10.00, ask1=None, bid1=None, high_limit=11.0, low_limit=9.0),
            config,
        )

        self.assertEqual(from_bid, 9.80)
        self.assertEqual(from_ask, 9.80)
        self.assertEqual(from_last, 9.80)

    def test_daily_limit_is_stricter_than_dynamic_cage(self):
        buy_price = calculate_order_price(
            _signal(Action.BUY),
            Quote(last_price=10.0, ask1=10.0, bid1=9.99, high_limit=10.15, low_limit=9.0),
            ExecutionConfig(pricing_mode="book", book_tick_offset=100),
        )
        sell_price = calculate_order_price(
            _signal(Action.SELL),
            Quote(last_price=10.0, ask1=10.01, bid1=10.0, high_limit=11.0, low_limit=9.85),
            ExecutionConfig(pricing_mode="book", book_tick_offset=100),
        )

        self.assertEqual(buy_price, 10.15)
        self.assertEqual(sell_price, 9.85)

    def test_dynamic_boundary_uses_half_up_tick_rounding(self):
        config = ExecutionConfig(pricing_mode="book", book_tick_offset=100)
        quote = Quote(last_price=10.75, ask1=10.75, bid1=10.74, high_limit=12.0, low_limit=9.0)

        price = calculate_order_price(_signal(Action.BUY), quote, config)

        self.assertEqual(price, 10.97)

    def test_stock_candidate_price_uses_half_up_tick_rounding(self):
        config = ExecutionConfig(buy_slippage_pct=0.0005)
        quote = Quote(last_price=10.0, high_limit=11.0, low_limit=9.0)

        price = calculate_order_price(_signal(Action.BUY), quote, config)

        self.assertEqual(price, 10.01)

    def test_funds_are_not_subject_to_stock_dynamic_cage(self):
        config = ExecutionConfig(pricing_mode="book", book_tick_offset=300)
        quote = Quote(last_price=1.0, ask1=1.0, bid1=0.999, high_limit=1.5, low_limit=0.5)

        price = calculate_order_price(
            _signal(Action.BUY, code="510300.XSHG", reference_price=1.0), quote, config,
        )

        self.assertEqual(price, 1.300)

    def test_future_shanghai_six_prefix_stock_uses_dynamic_cage(self):
        config = ExecutionConfig(pricing_mode="book", book_tick_offset=100)
        quote = Quote(last_price=10.0, ask1=10.0, bid1=9.99, high_limit=11.0, low_limit=9.0)

        price = calculate_order_price(
            _signal(Action.BUY, code="609999.XSHG"), quote, config,
        )

        self.assertEqual(price, 10.20)


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

import unittest

from miniqmt_follower.sizing import resolve_auto_buy, resolve_sell_all, resolve_sell_half


class SizingTests(unittest.TestCase):
    def test_sell_all_returns_available_position(self):
        self.assertEqual(resolve_sell_all(500), 500)
        self.assertEqual(resolve_sell_all(0), 0)
        self.assertEqual(resolve_sell_all(-100), 0)

    def test_sell_half_rounds_down_to_lot(self):
        self.assertEqual(resolve_sell_half(1000), 500)
        self.assertEqual(resolve_sell_half(1100), 500)
        self.assertEqual(resolve_sell_half(100), 100)  # 一手: 默认全卖
        self.assertEqual(resolve_sell_half(150), 150)  # 不足一手: 默认全卖
        self.assertEqual(resolve_sell_half(0), 0)

    def test_sell_half_insufficient_lot_skip_mode(self):
        # skip 模式: 半仓不足一手 → 不卖；足一手仍按正常半仓。
        self.assertEqual(resolve_sell_half(100, "skip"), 0)
        self.assertEqual(resolve_sell_half(150, "skip"), 0)
        self.assertEqual(resolve_sell_half(300, "skip"), 100)
        self.assertEqual(resolve_sell_half(1000, "skip"), 500)

    def test_auto_buy_splits_cash_by_count(self):
        self.assertEqual(
            resolve_auto_buy(
                available_cash=100_000, total_assets=200_000, buy_count=5,
                price=10.0, max_single_position_pct=0.2,
            ),
            2000,
        )

    def test_auto_buy_caps_by_single_position_pct(self):
        # 总资产 20万×20%=4万 < 等分 10万 → 按 4万 → 4000 股
        self.assertEqual(
            resolve_auto_buy(
                available_cash=500_000, total_assets=200_000, buy_count=5,
                price=10.0, max_single_position_pct=0.2,
            ),
            4000,
        )
        # 总资产 200万×20%=40万 > 等分 10万 → 仍按 10万 → 10000 股
        self.assertEqual(
            resolve_auto_buy(
                available_cash=500_000, total_assets=2_000_000, buy_count=5,
                price=10.0, max_single_position_pct=0.2,
            ),
            10000,
        )

    def test_auto_buy_rounds_to_lot_and_guards(self):
        self.assertEqual(
            resolve_auto_buy(
                available_cash=99_999, total_assets=1_000_000, buy_count=5,
                price=10.0, max_single_position_pct=0.2,
            ),
            1900,
        )
        self.assertEqual(
            resolve_auto_buy(
                available_cash=100_000, total_assets=1_000_000, buy_count=0,
                price=10.0, max_single_position_pct=0.2,
            ),
            0,
        )
        self.assertEqual(
            resolve_auto_buy(
                available_cash=100_000, total_assets=1_000_000, buy_count=5,
                price=0.0, max_single_position_pct=0.2,
            ),
            0,
        )


if __name__ == "__main__":
    unittest.main()

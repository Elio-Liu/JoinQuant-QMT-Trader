import os
from pathlib import Path
import unittest


# 策略部署副本路径通过环境变量注入，仓库内不写死策略文件名。
# 未设置或文件不存在时跳过用例（CI / 无本地部署副本的环境）。
STRATEGY_PATH = os.environ.get("HARVESTER_STRATEGY_PATH") or ""
STRATEGY = Path(STRATEGY_PATH) if STRATEGY_PATH else None


@unittest.skipUnless(
    STRATEGY is not None and STRATEGY.exists(),
    "requires HARVESTER_STRATEGY_PATH pointing to the local strategy deployment copy",
)
class HarvesterOpeningContractTests(unittest.TestCase):
    def test_only_sell_orders_are_prepublished_before_open(self):
        source = STRATEGY.read_text(encoding="utf-8")
        # 09:25~09:30 只允许提前发布卖出意图; 买入由日计划驱动, 不再发 buy+amount。
        self.assertNotIn(
            'run_daily(publish_pre_open_buy_signals, "09:29:30")', source
        )
        self.assertNotIn("def publish_pre_open_buy_signals(context):", source)
        self.assertNotIn("g.pre_sent_buy_amounts", source)
        self.assertNotIn("SIGNAL_OPEN_FLOOR_HHMMSS", source)
        self.assertIn('run_daily(check_auction_stop_loss, "09:27")', source)
        self.assertIn('run_daily(buy_at_open, "09:30:00")', source)

    def test_daily_plan_is_sent_after_auction_stop_loss(self):
        source = STRATEGY.read_text(encoding="utf-8")
        self.assertIn('run_daily(send_daily_plan, "09:28:00")', source)
        self.assertIn("def send_daily_plan(context):", source)

    def test_buy_at_open_no_longer_publishes_amount_signal(self):
        source = STRATEGY.read_text(encoding="utf-8")
        self.assertNotIn(
            'publish_trade_signal_to_redis(\n                context, "buy"', source
        )

    def test_midday_and_exit_use_intent_signals(self):
        source = STRATEGY.read_text(encoding="utf-8")
        self.assertIn("publish_sell_half_to_redis(context, stock, price)", source)
        self.assertIn("publish_sell_all_to_redis(context, stock, price)", source)


if __name__ == "__main__":
    unittest.main()

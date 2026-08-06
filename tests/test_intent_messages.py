import unittest
import json

from miniqmt_follower.models import Action, DailyPlan, TradeSignal
from miniqmt_follower.redis_stream import _parse_message


class TradeSignalIntentParsingTests(unittest.TestCase):
    def test_legacy_buy_with_amount_stays_exact(self):
        sig = TradeSignal.from_dict({
            "signal_id": "s1", "strategy_id": "harvester", "action": "buy",
            "code": "600000.XSHG", "amount": 1000, "reference_price": 10.0,
            "created_at": "2026-08-06 09:30:00",
        })
        self.assertEqual(sig.quantity_mode, "exact")
        self.assertEqual(sig.amount, 1000)

    def test_buy_without_amount_is_auto_buy(self):
        sig = TradeSignal.from_dict({
            "signal_id": "s2", "strategy_id": "harvester", "action": "buy",
            "code": "600000.XSHG", "reference_price": 10.0,
            "created_at": "2026-08-06 09:28:00",
        })
        self.assertEqual(sig.action, Action.BUY)
        self.assertEqual(sig.quantity_mode, "auto_buy")
        self.assertEqual(sig.amount, 0)

    def test_sell_half_and_sell_all_map_to_sell(self):
        half = TradeSignal.from_dict({
            "signal_id": "s3", "strategy_id": "harvester", "action": "sell_half",
            "code": "000001.XSHE", "reference_price": 10.5,
            "created_at": "2026-08-06 10:30:00",
        })
        self.assertEqual(half.action, Action.SELL)
        self.assertEqual(half.quantity_mode, "sell_half")
        all_ = TradeSignal.from_dict({
            "signal_id": "s4", "strategy_id": "harvester", "action": "sell_all",
            "code": "000002.XSHE", "reference_price": 9.8,
            "created_at": "2026-08-06 14:30:00",
        })
        self.assertEqual(all_.action, Action.SELL)
        self.assertEqual(all_.quantity_mode, "sell_all")

    def test_exact_sell_requires_amount(self):
        with self.assertRaises(ValueError):
            TradeSignal.from_dict({
                "signal_id": "s5", "strategy_id": "harvester", "action": "sell",
                "code": "000001.XSHE", "reference_price": 10.0,
                "created_at": "2026-08-06 10:30:00",
            })

    def test_invalid_quantity_mode_rejected(self):
        with self.assertRaises(ValueError):
            TradeSignal.from_dict({
                "signal_id": "s6", "strategy_id": "harvester", "action": "buy",
                "code": "600000.XSHG", "amount": 100, "reference_price": 10.0,
                "quantity_mode": "weird", "created_at": "2026-08-06 09:30:00",
            })

    def test_budget_group_size_parsed(self):
        sig = TradeSignal.from_dict({
            "signal_id": "s7", "strategy_id": "harvester", "action": "buy",
            "code": "600000.XSHG", "reference_price": 10.0,
            "budget_group_size": 5, "created_at": "2026-08-06 09:28:00",
        })
        self.assertEqual(sig.budget_group_size, 5)

    def test_daily_plan_from_dict(self):
        plan = DailyPlan.from_dict({
            "signal_id": "harvester-20260806-plan", "strategy_id": "harvester",
            "action": "plan", "codes_to_sell": ["000001.XSHE"],
            "codes_to_buy": ["600000.XSHG", "000002.XSHE"],
            "created_at": "2026-08-06 09:28:00",
        })
        self.assertEqual(plan.codes_to_sell, ("000001.XSHE",))
        self.assertEqual(plan.codes_to_buy, ("600000.XSHG", "000002.XSHE"))

    def test_daily_plan_rejects_non_plan_action(self):
        with self.assertRaises(ValueError):
            DailyPlan.from_dict({"action": "buy", "signal_id": "x"})


class PlanMessageRoutingTests(unittest.TestCase):
    def test_parse_message_routes_plan(self):
        message = _parse_message("1-0", {"payload": json.dumps({
            "signal_id": "harvester-20260806-plan", "strategy_id": "harvester",
            "action": "plan", "codes_to_sell": [], "codes_to_buy": ["600000.XSHG"],
        })})
        self.assertIsNone(message.signal)
        self.assertIsNotNone(message.plan)
        self.assertEqual(message.plan.signal_id, "harvester-20260806-plan")

    def test_parse_message_routes_intent_signal(self):
        message = _parse_message("1-1", {"payload": json.dumps({
            "signal_id": "s-half", "strategy_id": "harvester", "action": "sell_half",
            "code": "000001.XSHE", "reference_price": 10.5,
        })})
        self.assertIsNotNone(message.signal)
        self.assertEqual(message.signal.quantity_mode, "sell_half")


if __name__ == "__main__":
    unittest.main()

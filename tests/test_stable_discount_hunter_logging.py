# -*- coding: utf-8 -*-
import datetime as dt
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace
import sys
import types
import unittest
from unittest import mock

import pandas as pd


MODULE_PATH = (
    Path(__file__).resolve().parents[1]
    / "strategies"
    / "潮汐量化-ETF稳健折价猎手.py"
)
HAS_LOCAL_STRATEGY = MODULE_PATH.exists()


def load_strategy_module():
    spec = importlib.util.spec_from_file_location("stable_discount_hunter", str(MODULE_PATH))
    module = importlib.util.module_from_spec(spec)
    with mock.patch.dict(sys.modules, {
        "jqdata": types.ModuleType("jqdata"),
        "redis": types.ModuleType("redis"),
    }):
        spec.loader.exec_module(module)
    return module


class FakeLog:
    def __init__(self):
        self.messages = []

    def info(self, message):
        self.messages.append(message)

    def warning(self, message):
        self.messages.append(message)

    def error(self, message):
        self.messages.append(message)


class FakeRedisClient:
    def __init__(self):
        self.payloads = []

    def xadd(self, _stream, fields, **_kwargs):
        self.payloads.append(json.loads(fields["payload"]))
        return "1-0"


@unittest.skipUnless(HAS_LOCAL_STRATEGY, "requires ignored local strategy deployment copy")
class StableDiscountHunterLoggingTests(unittest.TestCase):
    def setUp(self):
        self.strategy = load_strategy_module()

    def test_etf_label_includes_code_and_chinese_name(self):
        self.strategy.g = SimpleNamespace(etf_names={"510300.XSHG": "沪深300ETF"})

        self.assertEqual(
            getattr(self.strategy, "etf_label", lambda _code: None)("510300.XSHG"),
            "510300.XSHG（沪深300ETF）",
        )

    def test_candidate_pool_logs_cross_border_and_liquidity_filters_with_names(self):
        log = FakeLog()
        self.strategy.log = log
        self.strategy.g = SimpleNamespace(
            min_liquidity_threshold=5e6,
            max_liquidity_threshold=8e7,
            candidate_pool=None,
        )
        all_etfs = pd.DataFrame(
            {"display_name": ["沪深300ETF", "恒生科技ETF", "低流动ETF"]},
            index=["510300.XSHG", "513130.XSHG", "510500.XSHG"],
        )
        money = {
            "510300.XSHG": [10_000_000],
            "510500.XSHG": [4_000_000],
        }
        nav = pd.DataFrame({"510300.XSHG": [1.0]}, index=["2026-07-10"])
        context = SimpleNamespace(previous_date="2026-07-10")

        with mock.patch.dict(self.strategy.__dict__, {
            "get_all_securities": mock.Mock(return_value=all_etfs),
            "history": mock.Mock(return_value=money),
            "get_extras": mock.Mock(return_value=nav),
        }):
            self.strategy.prepare_candidate_pool(context)

        log_text = "\n".join(log.messages)
        self.assertIn("跨境过滤｜剔除1只", log_text)
        self.assertIn("513130.XSHG（恒生科技ETF）", log_text)
        self.assertIn("流动性筛选｜境内2只", log_text)
        self.assertIn("510300.XSHG（沪深300ETF）", log_text)

    def test_live_selection_publishes_preopen_sell_and_buy(self):
        log = FakeLog()
        self.strategy.log = log
        self.strategy.g = SimpleNamespace(
            candidate_pool=pd.DataFrame(
                {
                    "display_name": ["目标ETF"],
                    "unit_net": [1.0],
                    "money": [10_000_000],
                },
                index=["TARGET.XSHG"],
            ),
            signal_candidates=None,
            max_holdings=1,
            premium_discount_threshold=1.0,
            single_position_cap=1.0,
            etf_names={"TARGET.XSHG": "目标ETF", "OLD.XSHG": "旧ETF"},
            prepublished_open_signals=set(),
        )
        positions = {
            "OLD.XSHG": SimpleNamespace(total_amount=1000, price=1.01),
        }
        context = SimpleNamespace(
            current_dt=dt.datetime(2026, 7, 13, 9, 25),
            portfolio=SimpleNamespace(positions=positions, total_value=100_000),
        )
        current_data = {
            "TARGET.XSHG": SimpleNamespace(paused=False, day_open=0.98, last_price=0.98),
            "OLD.XSHG": SimpleNamespace(paused=False, day_open=1.01, last_price=1.01),
        }
        publisher = mock.Mock(side_effect=[{"sent": True}, {"sent": True}])

        with mock.patch.dict(self.strategy.__dict__, {
            "get_current_data": mock.Mock(return_value=current_data),
            "publish_watchlist_to_redis": mock.Mock(return_value={"sent": True}),
            "publish_trade_signal_to_redis": publisher,
            "_signal_mode": mock.Mock(return_value=("live", context.current_dt)),
        }):
            self.strategy.generate_raw_signal(context)

        self.assertEqual(
            publisher.call_args_list,
            [
                mock.call(context, "sell", "OLD.XSHG", 1000, 1.01),
                mock.call(context, "buy", "TARGET.XSHG", 102000, 0.98),
            ],
        )
        self.assertEqual(
            self.strategy.g.prepublished_open_signals,
            {("sell", "OLD.XSHG"), ("buy", "TARGET.XSHG")},
        )

    def test_preopen_payloads_omit_execute_at_and_expire_from_send_time(self):
        self.strategy.g = SimpleNamespace(etf_names={"510300.XSHG": "沪深300ETF"})
        self.strategy.log = FakeLog()
        client = FakeRedisClient()
        context_time = dt.datetime(2026, 7, 13, 9, 25, 30)
        send_time = dt.datetime(2026, 7, 13, 9, 25, 31)
        context = SimpleNamespace(current_dt=context_time)

        class FixedDateTime(dt.datetime):
            @classmethod
            def now(cls, tz=None):
                return send_time

        with mock.patch.object(self.strategy.dt, "datetime", FixedDateTime), mock.patch.dict(
            self.strategy.__dict__,
            {
                "_signal_mode": mock.Mock(return_value=("live", context_time)),
                "_signal_redis_client": mock.Mock(return_value=client),
            },
        ):
            self.strategy.publish_trade_signal_to_redis(context, "buy", "510300.XSHG", 1000, 4.0)
            self.strategy.publish_trade_signal_to_redis(context, "sell", "510300.XSHG", 1000, 4.0)

        buy_payload, sell_payload = client.payloads
        self.assertNotIn("execute_at", buy_payload)
        self.assertNotIn("execute_at", sell_payload)
        self.assertEqual(buy_payload["expire_at"], "2026-07-13 09:25:51")
        self.assertEqual(sell_payload["expire_at"], "2026-07-13 09:25:51")

    def test_live_shadow_trade_skips_convergence_filter(self):
        log = FakeLog()
        self.strategy.log = log
        self.strategy.g = SimpleNamespace(
            target_positions={},
            signal_candidates=pd.DataFrame(
                {
                    "display_name": ["目标ETF"],
                    "open_price": [0.98],
                    "unit_net": [1.0],
                    "premium": [-2.0],
                },
                index=["TARGET.XSHG"],
            ),
            converge_warning=0.4,
            single_position_cap=1.0,
            etf_names={"TARGET.XSHG": "目标ETF"},
            prepublished_open_signals={("buy", "TARGET.XSHG")},
        )
        context = SimpleNamespace(
            current_dt=dt.datetime(2026, 7, 13, 9, 30),
            portfolio=SimpleNamespace(positions={}, available_cash=100_000, total_value=100_000),
        )
        current_data = {
            "TARGET.XSHG": SimpleNamespace(paused=False, last_price=0.95),
        }
        order_target_value = mock.Mock()

        with mock.patch.dict(self.strategy.__dict__, {
            "get_current_data": mock.Mock(return_value=current_data),
            "order_target_value": order_target_value,
            "_signal_mode": mock.Mock(return_value=("live", context.current_dt)),
        }):
            self.strategy.validate_and_trade(context)

        order_target_value.assert_called_once_with("TARGET.XSHG", 100_000.0)

    def test_zero_converge_warning_disables_filter_in_backtest(self):
        self.strategy.log = FakeLog()
        self.strategy.g = SimpleNamespace(
            target_positions={},
            signal_candidates=pd.DataFrame(
                {
                    "display_name": ["目标ETF"],
                    "open_price": [0.98],
                    "unit_net": [1.0],
                    "premium": [-2.0],
                },
                index=["TARGET.XSHG"],
            ),
            converge_warning=0,
            single_position_cap=1.0,
            etf_names={"TARGET.XSHG": "目标ETF"},
            prepublished_open_signals=set(),
        )
        context = SimpleNamespace(
            current_dt=dt.datetime(2026, 7, 13, 9, 30),
            portfolio=SimpleNamespace(positions={}, available_cash=100_000, total_value=100_000),
        )
        current_data = {
            "TARGET.XSHG": SimpleNamespace(paused=False, last_price=0.95),
        }
        order_target_value = mock.Mock()

        with mock.patch.dict(self.strategy.__dict__, {
            "get_current_data": mock.Mock(return_value=current_data),
            "order_target_value": order_target_value,
            "_signal_mode": mock.Mock(return_value=("backtest", context.current_dt)),
        }):
            self.strategy.validate_and_trade(context)

        order_target_value.assert_called_once_with("TARGET.XSHG", 100_000.0)


if __name__ == "__main__":
    unittest.main()

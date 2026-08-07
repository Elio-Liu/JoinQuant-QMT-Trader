from __future__ import annotations

import datetime
import importlib.util
import json
import queue
import time
import types
import unittest
from pathlib import Path


MODULE_PATH = Path(__file__).resolve().parents[1] / "bigqmt_follower" / "bigqmt_redis_follower.py"


def load_module():
    spec = importlib.util.spec_from_file_location("bigqmt_redis_follower_test_target", MODULE_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class BigQmtParsingAndPricingTests(unittest.TestCase):
    def test_deployable_single_file_exists(self):
        self.assertTrue(MODULE_PATH.is_file())

    def setUp(self):
        self.module = load_module()

    def test_parse_trade_payload_accepts_legacy_price_and_timestamp(self):
        message = self.module.parse_stream_message(
            "100-0",
            {
                "payload": json.dumps(
                    {
                        "signal_id": "sig-1",
                        "strategy_id": "hunter",
                        "mode": "live",
                        "action": "buy",
                        "code": "510300.XSHG",
                        "amount": 1000,
                        "price": 3.912,
                        "timestamp": "2026-07-12 09:30:00",
                    }
                )
            },
        )

        self.assertEqual(message["kind"], "trade")
        self.assertEqual(message["message_id"], "100-0")
        self.assertEqual(message["signal"]["reference_price"], 3.912)
        self.assertEqual(message["signal"]["created_at"], "2026-07-12 09:30:00")

    def test_parse_trade_payload_preserves_expire_at_and_ignores_legacy_execute_at(self):
        message = self.module.parse_stream_message(
            "102-0",
            {
                "payload": json.dumps(
                    {
                        "signal_id": "sig-expiry",
                        "strategy_id": "hunter",
                        "mode": "live",
                        "action": "buy",
                        "code": "510300.XSHG",
                        "amount": 1000,
                        "reference_price": 3.912,
                        "created_at": "2026-07-12 09:30:00",
                        "execute_at": "2099-01-01 09:30:00",
                        "expire_at": "2026-07-12 09:30:20",
                    }
                )
            },
        )

        self.assertEqual(message["signal"]["expire_at"], "2026-07-12 09:30:20")
        self.assertNotIn("execute_at", message["signal"])

    def test_parse_watchlist_command(self):
        message = self.module.parse_stream_message(
            "101-0",
            {
                "payload": json.dumps(
                    {
                        "action": "subscribe",
                        "strategy_id": "hunter",
                        "codes": ["510300.XSHG", "159915.XSHE"],
                    }
                )
            },
        )

        self.assertEqual(message["kind"], "watchlist")
        self.assertEqual(message["codes"], ["510300.XSHG", "159915.XSHE"])

    def test_parse_intent_actions_share_miniqmt_quantity_mode(self):
        sell_half = self.module.parse_stream_message(
            "103-0",
            {
                "payload": json.dumps(
                    {
                        "signal_id": "harvester-20260806-000001XSHE-sell_half",
                        "strategy_id": "harvester",
                        "mode": "live",
                        "action": "sell_half",
                        "code": "000001.XSHE",
                        "reference_price": 10.0,
                    }
                )
            },
        )
        sell_all = self.module.parse_stream_message(
            "104-0",
            {
                "payload": json.dumps(
                    {
                        "signal_id": "harvester-20260806-000001XSHE-sell_all",
                        "strategy_id": "harvester",
                        "mode": "live",
                        "action": "sell_all",
                        "code": "000001.XSHE",
                        "reference_price": 10.0,
                    }
                )
            },
        )
        auto_buy = self.module.parse_stream_message(
            "105-0",
            {
                "payload": json.dumps(
                    {
                        "signal_id": "harvester-20260806-600000XSHG-buy",
                        "strategy_id": "harvester",
                        "mode": "live",
                        "action": "buy",
                        "code": "600000.XSHG",
                        "reference_price": 10.0,
                    }
                )
            },
        )

        self.assertEqual(sell_half["signal"]["action"], "sell")
        self.assertEqual(sell_half["signal"]["quantity_mode"], "sell_half")
        self.assertEqual(sell_all["signal"]["action"], "sell")
        self.assertEqual(sell_all["signal"]["quantity_mode"], "sell_all")
        self.assertEqual(auto_buy["signal"]["action"], "buy")
        self.assertEqual(auto_buy["signal"]["quantity_mode"], "auto_buy")
        self.assertEqual(auto_buy["signal"]["amount"], 0)

    def test_parse_plan_message(self):
        message = self.module.parse_stream_message(
            "106-0",
            {
                "payload": json.dumps(
                    {
                        "signal_id": "harvester-20260806-plan",
                        "strategy_id": "harvester",
                        "mode": "live",
                        "action": "plan",
                        "codes_to_sell": ["000001.XSHE"],
                        "codes_to_buy": ["600000.XSHG", "000002.XSHE"],
                        "created_at": "2026-08-06 09:28:00",
                    }
                )
            },
        )

        self.assertEqual(message["kind"], "plan")
        self.assertEqual(message["plan"]["codes_to_sell"], ["000001.XSHE"])
        self.assertEqual(
            message["plan"]["codes_to_buy"], ["600000.XSHG", "000002.XSHE"]
        )

    def test_resolve_sell_half_insufficient_lot_mode(self):
        self.assertEqual(self.module._resolve_sell_half(100), 100)
        self.assertEqual(self.module._resolve_sell_half(150), 150)
        self.assertEqual(self.module._resolve_sell_half(100, "skip"), 0)
        self.assertEqual(self.module._resolve_sell_half(150, "skip"), 0)
        self.assertEqual(self.module._resolve_sell_half(300, "skip"), 100)
        self.assertEqual(self.module._resolve_sell_half(1000, "skip"), 500)

    def test_code_and_tick_mapping(self):
        self.assertEqual(self.module.jq_code_to_qmt_code("510300.XSHG"), "510300.SH")
        self.assertEqual(self.module.jq_code_to_qmt_code("159915.XSHE"), "159915.SZ")
        self.assertEqual(self.module.tick_size_for("510300.XSHG"), 0.001)
        self.assertEqual(self.module.tick_size_for("000001.XSHE"), 0.01)

    def test_book_buy_uses_ask_plus_etf_ticks(self):
        config = dict(self.module.CONFIG)
        config.update({"pricing_mode": "book", "book_tick_offset": 2})
        signal = {"action": "buy", "code": "510300.XSHG", "reference_price": 1.0}
        tick = {"lastPrice": 1.0, "askPrice": [1.001], "bidPrice": [0.999]}

        price = self.module.calculate_order_price(signal, tick, config)

        self.assertEqual(price, 1.003)

    def test_book_buy_falls_back_to_slippage_when_ask_is_empty(self):
        config = dict(self.module.CONFIG)
        config.update({"pricing_mode": "book", "buy_slippage_pct": 0.003})
        signal = {"action": "buy", "code": "510300.XSHG", "reference_price": 1.0}
        tick = {"lastPrice": 1.0, "askPrice": [0], "bidPrice": [0.999]}

        price = self.module.calculate_order_price(signal, tick, config)

        self.assertEqual(price, 1.003)

    def test_buy_still_prices_when_market_ran_far_above_reference(self):
        """跟单一致性优先: 卖一已飞出参考价 2%, 照常吃对手盘, 不再拒单。"""
        config = dict(self.module.CONFIG)
        config.update({"pricing_mode": "book", "book_tick_offset": 0})
        signal = {"action": "buy", "code": "000001.XSHE", "reference_price": 10.0}
        tick = {"lastPrice": 10.0, "askPrice": [10.2], "bidPrice": [9.99]}

        price = self.module.calculate_order_price(signal, tick, config)

        self.assertEqual(price, 10.2)

    def test_a_share_buy_is_clamped_into_dynamic_price_cage(self):
        """沪深A股买单滑点超2%时压回动态价格笼子上限(基准+2%与+10tick取宽)。"""
        config = dict(self.module.CONFIG)
        config.update({"pricing_mode": "slippage", "buy_slippage_pct": 0.05})
        signal = {"action": "buy", "code": "000001.XSHE", "reference_price": 10.0}
        tick = {"lastPrice": 10.0, "askPrice": [0], "bidPrice": [10.0]}

        price = self.module.calculate_order_price(signal, tick, config)

        self.assertEqual(price, 10.2)

    def test_a_share_sell_is_clamped_into_dynamic_price_cage(self):
        """沪深A股卖单滑点超2%时抬回动态价格笼子下限(基准-2%与-10tick取宽)。"""
        config = dict(self.module.CONFIG)
        config.update({"pricing_mode": "slippage", "sell_slippage_pct": 0.05})
        signal = {"action": "sell", "code": "000001.XSHE", "reference_price": 10.0}
        tick = {"lastPrice": 10.0, "askPrice": [10.0], "bidPrice": [0]}

        price = self.module.calculate_order_price(signal, tick, config)

        self.assertEqual(price, 9.8)

    def test_etf_is_not_clamped_by_stock_price_cage(self):
        """价格笼子只套沪深A股, ETF/基金保持滑点价。"""
        config = dict(self.module.CONFIG)
        config.update({"pricing_mode": "slippage", "buy_slippage_pct": 0.05})
        signal = {"action": "buy", "code": "510300.XSHG", "reference_price": 1.0}
        tick = {"lastPrice": 1.0, "askPrice": [0], "bidPrice": [1.0]}

        price = self.module.calculate_order_price(signal, tick, config)

        self.assertEqual(price, 1.05)

    def test_auction_queue_price_clamped_into_limit_band(self):
        """竞价排队报价与 miniQMT 版同规则: 买 +2%, 且夹在涨跌停内。"""
        config = dict(self.module.CONFIG)
        config.update({"pricing_mode": "book", "auction_aggressive_pct": 0.02})
        signal = {"action": "buy", "code": "000001.XSHE", "reference_price": 10.0}
        tick = {"lastPrice": 10.0, "askPrice": [10.0], "bidPrice": [9.99]}
        original = self.module._in_call_auction
        self.module._in_call_auction = lambda: True
        try:
            normal = self.module.calculate_order_price(signal, tick, config, (11.0, 9.0))
            clamped = self.module.calculate_order_price(signal, tick, config, (10.1, 9.0))
            # 取不到涨跌停价 → 回退常规盘口定价, 不裸奔
            fallback = self.module.calculate_order_price(signal, tick, config, (None, None))
        finally:
            self.module._in_call_auction = original

        self.assertEqual(normal, 10.2)
        self.assertEqual(clamped, 10.1)
        self.assertEqual(fallback, 10.02)  # 卖一 10.0 + 2 tick

    def test_auction_queue_price_skipped_outside_window(self):
        config = dict(self.module.CONFIG)
        config.update({"pricing_mode": "book", "auction_aggressive_pct": 0.02})
        signal = {"action": "buy", "code": "000001.XSHE", "reference_price": 10.0}
        tick = {"lastPrice": 10.0, "askPrice": [10.0], "bidPrice": [9.99]}
        original = self.module._in_call_auction
        self.module._in_call_auction = lambda: False
        try:
            price = self.module.calculate_order_price(signal, tick, config, (11.0, 9.0))
        finally:
            self.module._in_call_auction = original

        self.assertEqual(price, 10.02)


class FakeRedisClient:
    def __init__(self):
        self.group_calls = []
        self.read_calls = []
        self.ack_calls = []
        self.group_error = None
        self.ack_error = None
        self.read_response = []

    def xgroup_create(self, stream, group, id, mkstream):
        self.group_calls.append((stream, group, id, mkstream))
        if self.group_error is not None:
            raise self.group_error

    def xreadgroup(self, group, consumer, streams, count, block):
        self.read_calls.append((group, consumer, streams, count, block))
        return self.read_response

    def xack(self, stream, group, message_id):
        if self.ack_error is not None:
            error = self.ack_error
            self.ack_error = None
            raise error
        self.ack_calls.append((stream, group, message_id))


class RedisStreamWorkerTests(unittest.TestCase):
    def setUp(self):
        self.module = load_module()
        self.config = dict(self.module.CONFIG)
        self.config.update(
            {
                "redis_host": "redis.example.test",
                "redis_stream": "signals",
                "redis_group": "big-group",
                "redis_block_ms": 20,
            }
        )
        self.inbound = queue.Queue()
        self.acks = queue.Queue()
        self.client = FakeRedisClient()
        self.worker = self.module.RedisStreamWorker(
            self.config,
            self.inbound,
            self.acks,
            redis_factory=lambda **_kwargs: self.client,
        )
        self.worker.connect()

    def test_group_is_created_at_latest_entry(self):
        self.worker.ensure_group()

        self.assertEqual(self.client.group_calls, [("signals", "big-group", "$", True)])

    def test_existing_group_is_not_an_error(self):
        self.client.group_error = RuntimeError("BUSYGROUP Consumer Group name already exists")

        self.worker.ensure_group()

    def test_read_once_only_requests_never_delivered_entries(self):
        payload = json.dumps(
            {
                "signal_id": "sig-redis",
                "strategy_id": "hunter",
                "mode": "live",
                "action": "buy",
                "code": "000001.XSHE",
                "amount": 100,
                "reference_price": 10.0,
            }
        )
        self.client.read_response = [("signals", [("200-0", {"payload": payload})])]

        self.worker.read_once()

        self.assertEqual(
            self.client.read_calls,
            [("big-group", self.worker.consumer_name, {"signals": ">"}, 10, 20)],
        )
        self.assertEqual(self.inbound.get_nowait()["message_id"], "200-0")

    def test_ack_queue_is_the_only_source_of_xack(self):
        self.acks.put("201-0")

        self.worker.process_ack_queue()

        self.assertEqual(self.client.ack_calls, [("signals", "big-group", "201-0")])

    def test_failed_xack_is_requeued_for_retry(self):
        self.client.ack_error = RuntimeError("temporary redis failure")
        self.acks.put("202-0")

        with self.assertRaisesRegex(RuntimeError, "temporary"):
            self.worker.process_ack_queue()

        self.assertEqual(self.acks.get_nowait(), "202-0")


class FakeContext:
    def __init__(self):
        self.universes = []
        self.accounts = []
        self.timers = []

    def set_universe(self, codes):
        self.universes.append(list(codes))

    def set_account(self, account):
        self.accounts.append(account)

    def run_time(self, name, period, start_time, market):
        self.timers.append((name, period, start_time, market))


class FakeGateway:
    def __init__(self):
        self.tick = {"lastPrice": 10.0, "askPrice": [10.0], "bidPrice": [9.99]}
        # 默认取不到涨跌停价 → 竞价排队报价自动回退, 用例与真实时钟解耦。
        self.limits = (None, None)
        self.cash = 100000.0
        self.total_assets = 100000.0
        self.positions = {}
        self.total_positions = {}
        self.submissions = []
        self.orders = []
        self.cancelable = True
        self.cancel_calls = []

    def latest_tick(self, _code):
        return self.tick

    def limit_prices(self, _code):
        return self.limits

    def available_cash(self):
        return self.cash

    def available_position(self, code):
        return self.positions.get(code, 0)

    def total_position(self, code):
        return self.total_positions.get(code, 0)

    def query_total_assets(self):
        return self.total_assets

    def submit(self, signal, quantity, price, remark):
        self.submissions.append((signal["action"], signal["code"], quantity, price, remark))

    def list_orders(self):
        return list(self.orders)

    def can_cancel(self, _order_id):
        return self.cancelable

    def cancel(self, order_id):
        self.cancel_calls.append(order_id)
        return True


def trade_message(
    message_id, signal_id, action="buy", code="000001.XSHE", amount=1000,
    expire_at=None, created_at="2026-07-12 09:30:00",
    quantity_mode=None, budget_group_size=None, strategy_id="hunter",
):
    message = {
        "kind": "trade",
        "message_id": message_id,
        "signal": {
            "signal_id": signal_id,
            "strategy_id": strategy_id,
            "mode": "live",
            "action": action,
            "code": code,
            "amount": amount,
            "reference_price": 10.0,
            "created_at": created_at,
            "sent_at_ms": None,
        },
    }
    if expire_at is not None:
        message["signal"]["expire_at"] = expire_at
    if quantity_mode is not None:
        message["signal"]["quantity_mode"] = quantity_mode
    if budget_group_size is not None:
        message["signal"]["budget_group_size"] = budget_group_size
    return message


def plan_message(
    message_id, plan_id, codes_to_sell, codes_to_buy,
    strategy_id="harvester", created_at="2026-07-12 09:30:00",
):
    return {
        "kind": "plan",
        "message_id": message_id,
        "plan": {
            "signal_id": plan_id,
            "strategy_id": strategy_id,
            "mode": "live",
            "codes_to_sell": codes_to_sell,
            "codes_to_buy": codes_to_buy,
            "created_at": created_at,
            "sent_at_ms": None,
        },
    }


class BigQmtSubmissionTests(unittest.TestCase):
    def setUp(self):
        self.module = load_module()
        self.config = dict(self.module.CONFIG)
        self.config.update(
            {
                "trading_enabled": True,
                "account_id": "test-account",
                "book_tick_offset": 0,
            }
        )
        self.inbound = queue.Queue()
        self.acks = queue.Queue()
        self.worker = types.SimpleNamespace(inbound_queue=self.inbound, ack_queue=self.acks)
        self.gateway = FakeGateway()
        self.runtime = self.module.BigQmtRuntime(
            self.config, self.worker, gateway_factory=lambda _context, _config: self.gateway
        )
        self.context = FakeContext()

    def test_watchlist_updates_universe_and_is_acked_without_order(self):
        self.inbound.put(
            {
                "kind": "watchlist",
                "message_id": "300-0",
                "codes": ["510300.XSHG", "159915.XSHE"],
            }
        )

        self.runtime.on_timer(self.context)

        self.assertEqual(set(self.context.universes[0]), {"510300.SH", "159915.SZ"})
        self.assertEqual(self.acks.get_nowait(), "300-0")
        self.assertEqual(self.gateway.submissions, [])

    def test_expired_fifo_signal_is_acked_without_submission_and_next_signal_continues(self):
        expired_now = time.mktime(datetime.datetime(2026, 7, 12, 9, 30, 21).timetuple())
        self.runtime.clock = lambda: expired_now
        self.inbound.put(
            trade_message("310-0", "sig-expired", expire_at="2026-07-12 09:30:20")
        )
        self.inbound.put(trade_message("311-0", "sig-valid"))

        self.runtime.on_timer(self.context)

        self.assertEqual(self.acks.get_nowait(), "310-0")
        self.assertEqual(self.gateway.submissions, [])
        self.assertIsNone(self.runtime.active)

        self.runtime.on_timer(self.context)

        self.assertEqual(len(self.gateway.submissions), 1)
        self.assertEqual(self.runtime.active["signal"]["signal_id"], "sig-valid")

    def test_invalid_expire_at_is_acked_without_submission(self):
        self.inbound.put(
            trade_message("312-0", "sig-invalid-expiry", expire_at="09:30:20")
        )

        self.runtime.on_timer(self.context)

        self.assertEqual(self.acks.get_nowait(), "312-0")
        self.assertEqual(self.gateway.submissions, [])
        self.assertIsNone(self.runtime.active)

    def test_buy_is_lot_capped_by_available_cash(self):
        self.gateway.cash = 9500.0
        self.inbound.put(trade_message("301-0", "sig-buy"))

        self.runtime.on_timer(self.context)

        self.assertEqual(self.gateway.submissions[0][2], 900)

    def test_sell_is_capped_by_available_position(self):
        self.gateway.positions["000001.SZ"] = 600
        self.inbound.put(trade_message("302-0", "sig-sell", action="sell"))

        self.runtime.on_timer(self.context)

        self.assertEqual(self.gateway.submissions[0][2], 600)

    def test_resource_query_failure_fails_signal_without_halting_channel(self):
        def unavailable_cash():
            raise RuntimeError("account query unavailable")

        self.gateway.available_cash = unavailable_cash
        self.inbound.put(trade_message("308-0", "sig-resource-failure"))

        self.runtime.on_timer(self.context)

        self.assertFalse(self.runtime.halted)
        self.assertIsNone(self.runtime.active)
        self.assertEqual(self.acks.get_nowait(), "308-0")

    def test_passorder_exception_halts_and_acks_failed_broker(self):
        def submit_unknown(*_args):
            raise RuntimeError("late client error")

        self.gateway.submit = submit_unknown
        self.inbound.put(trade_message("309-0", "sig-submit-unknown"))

        self.runtime.on_timer(self.context)

        self.assertTrue(self.runtime.halted)
        self.assertIsNone(self.runtime.active)
        self.assertEqual(self.acks.get_nowait(), "309-0")

    def test_only_first_fifo_signal_submits_while_it_is_active(self):
        self.inbound.put(trade_message("303-0", "sig-first"))
        self.inbound.put(trade_message("304-0", "sig-second"))

        self.runtime.on_timer(self.context)
        self.runtime.on_timer(self.context)

        self.assertEqual(len(self.gateway.submissions), 1)
        self.assertEqual(self.runtime.pending_count, 1)

    def test_sell_and_buy_submit_in_same_timer_without_waiting(self):
        self.gateway.positions["000001.SZ"] = 1000
        self.inbound.put(trade_message("313-0", "sig-sell", action="sell"))
        self.inbound.put(trade_message("314-0", "sig-buy", action="buy"))

        self.runtime.on_timer(self.context)

        self.assertEqual(
            [submission[0] for submission in self.gateway.submissions],
            ["sell", "buy"],
        )
        self.assertEqual(self.runtime.pending_count, 0)

    def test_duplicate_signal_in_same_run_is_acked_without_second_order(self):
        self.inbound.put(trade_message("305-0", "sig-duplicate"))
        self.inbound.put(trade_message("306-0", "sig-duplicate"))

        self.runtime.on_timer(self.context)

        self.assertEqual(len(self.gateway.submissions), 1)
        self.assertEqual(self.acks.get_nowait(), "306-0")

    def test_plan_expands_derived_signals_and_acks_after_all_terminal(self):
        self.gateway.positions["000001.SZ"] = 1000
        self.gateway.total_positions["000002.SZ"] = 500
        self.inbound.put(
            plan_message(
                "320-0", "harvester-20260806-plan",
                ["000001.XSHE"], ["600000.XSHG", "000002.XSHE"],
            )
        )

        self.runtime.on_timer(self.context)

        # 派生: 1 卖 + 1 买 (000002 已持仓被过滤), 买卖通道同时开始。
        self.assertEqual(
            [s[0] for s in self.gateway.submissions], ["sell", "buy"]
        )
        # auto_buy: min(可用资金÷1, 总资产×20%) = 20000 → 2000 股。
        self.assertEqual(self.gateway.submissions[1][2], 2000)
        self.assertTrue(self.acks.empty())

        sell_remark = self.runtime.active_by_action["sell"]["remark"]
        buy_remark = self.runtime.active_by_action["buy"]["remark"]
        self.runtime.on_order(order_info(sell_remark, status=56, traded=1000))
        self.runtime.on_timer(self.context)
        self.assertTrue(self.acks.empty())  # 买未完成, plan 不 ACK

        self.runtime.on_order(order_info(buy_remark, status=56, traded=2000))
        self.runtime.on_timer(self.context)

        self.assertEqual(self.acks.get_nowait(), "320-0")
        self.assertIsNone(self.runtime.active)

    def test_sell_half_intent_resolves_from_real_position(self):
        self.gateway.positions["000001.SZ"] = 1500
        self.inbound.put(
            trade_message(
                "321-0", "sig-half", action="sell",
                amount=None, quantity_mode="sell_half",
            )
        )

        self.runtime.on_timer(self.context)

        self.assertEqual(self.gateway.submissions[0][2], 700)

    def test_sell_half_one_lot_sells_all_by_default(self):
        self.gateway.positions["000001.SZ"] = 100
        self.inbound.put(
            trade_message(
                "323-0", "sig-half-lot", action="sell",
                amount=None, quantity_mode="sell_half",
            )
        )

        self.runtime.on_timer(self.context)

        self.assertEqual(self.gateway.submissions[0][2], 100)

    def test_sell_half_below_one_lot_skips_when_configured(self):
        self.config["sell_half_insufficient_lot_mode"] = "skip"
        self.gateway.positions["000001.SZ"] = 100
        self.inbound.put(
            trade_message(
                "324-0", "sig-half-skip", action="sell",
                amount=None, quantity_mode="sell_half",
            )
        )

        self.runtime.on_timer(self.context)

        self.assertEqual(self.gateway.submissions, [])
        self.assertIsNone(self.runtime.active)
        self.assertEqual(self.acks.get_nowait(), "324-0")

    def test_strategy_whitelist_acks_unknown_strategy_without_order(self):
        self.config["allowed_strategy_ids"] = ["harvester"]
        self.inbound.put(
            trade_message("322-0", "sig-other", strategy_id="other")
        )

        self.runtime.on_timer(self.context)

        self.assertEqual(self.gateway.submissions, [])
        self.assertIsNone(self.runtime.active)
        self.assertEqual(self.acks.get_nowait(), "322-0")

    def test_gateway_uses_normal_stock_passorder_signature(self):
        calls = []
        self.module.passorder = lambda *args: calls.append(args)
        gateway = self.module.BigQmtGateway(self.context, self.config)
        signal = trade_message("307-0", "sig-passorder")["signal"]

        gateway.submit(signal, 900, 10.03, "BQR-ABC-01")

        self.assertEqual(calls[0][0:2], (23, 1101))
        self.assertEqual(calls[0][2:7], ("test-account", "000001.SZ", 11, 10.03, 900))
        self.assertEqual(calls[0][7:10], ("bigqmt_redis_follower", 1, "BQR-ABC-01"))
        self.assertIs(calls[0][10], self.context)

    def test_gateway_matches_position_when_broker_splits_code_and_exchange(self):
        position = types.SimpleNamespace(
            m_strInstrumentID="000001", m_strExchangeID="SZ", m_nCanUseVolume=600
        )
        self.module.get_trade_detail_data = lambda *_args: [position]
        gateway = self.module.BigQmtGateway(self.context, self.config)

        self.assertEqual(gateway.available_position("000001.SZ"), 600)

    def test_gateway_falls_back_when_strategy_filtered_order_query_is_unsupported(self):
        order = object()
        calls = []

        def fake_trade_detail(*args):
            calls.append(args)
            if len(args) == 4:
                raise TypeError("strategyName unsupported")
            return [order]

        self.module.get_trade_detail_data = fake_trade_detail
        gateway = self.module.BigQmtGateway(self.context, self.config)

        self.assertEqual(gateway.list_orders(), [order])
        self.assertEqual(len(calls[0]), 4)
        self.assertEqual(len(calls[1]), 3)


class FakeClock:
    def __init__(self, now=1000.0):
        self.now = now

    def __call__(self):
        return self.now

    def advance(self, seconds):
        self.now += seconds


def order_info(
    remark, order_id="order-1", status=50, traded=0, original=1000, error_msg=""
):
    return types.SimpleNamespace(
        m_strRemark=remark,
        m_strOrderSysID=order_id,
        m_nOrderStatus=status,
        m_nVolumeTotalOriginal=original,
        m_nVolumeTraded=traded,
        m_nVolumeTotal=max(original - traded, 0),
        m_dTradedPrice=10.0,
        m_strCancelInfo="",
        m_strErrorMsg=error_msg,
    )


class BigQmtExecutionStateTests(unittest.TestCase):
    def setUp(self):
        self.module = load_module()
        self.config = dict(self.module.CONFIG)
        self.config.update(
            {
                "trading_enabled": True,
                "account_id": "test-account",
                "book_tick_offset": 0,
                "order_timeout_sec": 1.0,
                "order_visibility_timeout_sec": 1.0,
                "cancel_confirm_timeout_sec": 1.0,
                "max_attempts": 3,
                "max_total_duration_sec": 10.0,
            }
        )
        self.clock = FakeClock()
        self.inbound = queue.Queue()
        self.acks = queue.Queue()
        self.worker = types.SimpleNamespace(inbound_queue=self.inbound, ack_queue=self.acks)
        self.gateway = FakeGateway()
        self.gateway.positions["000001.SZ"] = 1000
        # 状态机测试默认不在盘前窗口, 避免真实时钟耦合; 屏障用例单独替换。
        self.module._in_preopen_window = lambda: False
        self.runtime = self.module.BigQmtRuntime(
            self.config,
            self.worker,
            gateway_factory=lambda _context, _config: self.gateway,
            clock=self.clock,
        )
        self.context = FakeContext()

    def submit_sell(self, signal_id="sig-exec"):
        self.inbound.put(trade_message("400-0", signal_id, action="sell"))
        self.runtime.on_timer(self.context)
        return self.runtime.active["remark"]

    def test_order_status_mapping_is_fail_closed(self):
        for status in (0, 48, 49, 50, 51, 52, 55, 86, 255, 999):
            self.assertEqual(self.module.classify_order_status(status), "OPEN")
        for status in (53, 54):
            self.assertEqual(self.module.classify_order_status(status), "CANCELED")
        self.assertEqual(self.module.classify_order_status(56), "FILLED")
        self.assertEqual(self.module.classify_order_status(57), "REJECTED")

    def test_full_fill_is_acked_and_releases_fifo(self):
        remark = self.submit_sell()
        self.gateway.orders = [order_info(remark, status=56, traded=1000)]

        self.runtime.on_timer(self.context)

        self.assertIsNone(self.runtime.active)
        self.assertEqual(self.acks.get_nowait(), "400-0")

    def test_partial_fill_cancel_reorders_only_late_fill_adjusted_remainder(self):
        remark = self.submit_sell()
        self.runtime.on_order(order_info(remark, status=55, traded=300))
        self.clock.advance(1.1)

        self.runtime.on_timer(self.context)

        self.assertEqual(self.gateway.cancel_calls, ["order-1"])
        self.assertEqual(self.runtime.active["state"], "CANCEL_REQUESTED")

        self.runtime.on_order(order_info(remark, status=54, traded=400))
        self.runtime.on_timer(self.context)

        self.assertEqual(len(self.gateway.submissions), 2)
        self.assertEqual(self.gateway.submissions[1][2], 600)

    def test_unique_deal_callback_updates_late_fill_before_reorder(self):
        remark = self.submit_sell("sig-late-deal")
        self.runtime.on_order(order_info(remark, status=54, traded=300))
        first_deal = types.SimpleNamespace(
            m_strRemark=remark, m_strTradeID="deal-1", m_nVolume=300
        )
        late_deal = types.SimpleNamespace(
            m_strRemark=remark, m_strTradeID="deal-2", m_nVolume=100
        )

        self.runtime.on_deal(first_deal)
        self.runtime.on_deal(late_deal)
        self.runtime.on_deal(late_deal)
        self.runtime.on_timer(self.context)

        self.assertEqual(len(self.gateway.submissions), 2)
        self.assertEqual(self.gateway.submissions[1][2], 600)

    def test_max_attempts_finishes_partial_without_another_order(self):
        self.config["max_attempts"] = 1
        remark = self.submit_sell("sig-max-attempts")
        self.runtime.on_order(order_info(remark, status=54, traded=300))

        self.runtime.on_timer(self.context)

        self.assertEqual(len(self.gateway.submissions), 1)
        self.assertIsNone(self.runtime.active)
        self.assertEqual(self.acks.get_nowait(), "400-0")

    def test_unseen_passorder_halts_and_acks_failed_broker(self):
        self.submit_sell("sig-invisible")
        self.clock.advance(1.1)

        self.runtime.on_timer(self.context)

        self.assertTrue(self.runtime.halted)
        self.assertIsNone(self.runtime.active)
        self.assertEqual(self.acks.get_nowait(), "400-0")

    def test_uncancelable_open_order_halts_and_acks_failed_broker(self):
        remark = self.submit_sell("sig-cancel-uncertain")
        self.runtime.on_order(order_info(remark, status=50, traded=0))
        self.gateway.cancelable = False
        self.clock.advance(1.1)

        self.runtime.on_timer(self.context)

        self.assertTrue(self.runtime.halted)
        self.assertIsNone(self.runtime.active)
        self.assertEqual(self.acks.get_nowait(), "400-0")

    def test_halted_channel_acks_later_signals_as_failed_broker(self):
        def submit_unknown(*_args):
            raise RuntimeError("late client error")

        self.gateway.submit = submit_unknown
        self.inbound.put(trade_message("401-0", "sig-halt-trigger"))
        self.runtime.on_timer(self.context)
        self.assertTrue(self.runtime.halted)
        self.assertEqual(self.acks.get_nowait(), "401-0")

        self.inbound.put(trade_message("402-0", "sig-after-halt"))
        self.runtime.on_timer(self.context)

        self.assertEqual(self.gateway.submissions, [])
        self.assertEqual(self.acks.get_nowait(), "402-0")
        self.assertEqual(self.runtime.pending_count, 0)

    def test_auction_signal_visibility_budget_extends_until_after_open(self):
        auction_time = __import__("time").mktime((2026, 7, 13, 9, 27, 0, 0, 0, -1))
        self.clock.now = auction_time
        self.submit_sell("sig-auction")

        self.clock.advance(10.0)
        self.runtime.on_timer(self.context)

        self.assertFalse(self.runtime.halted)

        self.clock.now = __import__("time").mktime((2026, 7, 13, 9, 30, 2, 0, 0, -1))
        self.runtime.on_timer(self.context)

        self.assertTrue(self.runtime.halted)


class BigQmtMiniQmtAlignmentTests(unittest.TestCase):
    """大 QMT 与 miniQMT 逻辑对齐: 拒单分类 / 开盘屏障 / 涨跌停排队。"""

    def setUp(self):
        self.module = load_module()
        self.config = dict(self.module.CONFIG)
        self.config.update(
            {
                "trading_enabled": True,
                "account_id": "test-account",
                "book_tick_offset": 0,
                "order_timeout_sec": 1.0,
                "order_visibility_timeout_sec": 1.0,
                "cancel_confirm_timeout_sec": 1.0,
                "max_attempts": 3,
                "max_total_duration_sec": 10.0,
            }
        )
        self.clock = FakeClock()
        self.inbound = queue.Queue()
        self.acks = queue.Queue()
        self.worker = types.SimpleNamespace(
            inbound_queue=self.inbound, ack_queue=self.acks
        )
        self.gateway = FakeGateway()
        self.gateway.positions["000001.SZ"] = 1000
        self.module._in_preopen_window = lambda: False
        self.runtime = self.module.BigQmtRuntime(
            self.config,
            self.worker,
            gateway_factory=lambda _context, _config: self.gateway,
            clock=self.clock,
        )
        self.context = FakeContext()

    def test_classify_qmt_rejection_keywords(self):
        self.assertEqual(
            self.module.classify_qmt_rejection("股票停牌，禁止交易"), "HARD_STOP"
        )
        self.assertEqual(
            self.module.classify_qmt_rejection("委托价格超出涨跌停范围"), "PRICE"
        )
        self.assertEqual(self.module.classify_qmt_rejection("可用资金不足"), "RESOURCE")
        self.assertEqual(
            self.module.classify_qmt_rejection("柜台繁忙，请稍后重试"), "TRANSIENT"
        )
        self.assertEqual(self.module.classify_qmt_rejection("未知文案"), "UNKNOWN")

    def test_hard_rejected_order_terminates_without_retry(self):
        self.inbound.put(trade_message("500-0", "sig-hard-reject", action="sell"))
        self.runtime.on_timer(self.context)
        remark = self.runtime.active["remark"]
        self.runtime.on_order(order_info(remark, status=57, traded=0, error_msg="股票停牌"))

        self.runtime.on_timer(self.context)

        self.assertEqual(len(self.gateway.submissions), 1)
        self.assertIsNone(self.runtime.active)
        self.assertEqual(self.acks.get_nowait(), "500-0")

    def test_transient_rejected_order_retries(self):
        self.inbound.put(trade_message("501-0", "sig-soft-reject", action="sell"))
        self.runtime.on_timer(self.context)
        remark = self.runtime.active["remark"]
        self.runtime.on_order(order_info(remark, status=57, traded=0, error_msg="柜台繁忙"))

        self.runtime.on_timer(self.context)

        self.assertEqual(len(self.gateway.submissions), 2)

    def test_buy_waits_for_preopen_sell_until_it_finishes(self):
        self.module._in_preopen_window = lambda: True
        self.inbound.put(
            trade_message(
                "510-0", "sig-preopen-sell", action="sell",
                created_at="2026-07-13 09:27:00",
            )
        )
        self.inbound.put(trade_message("511-0", "sig-buy", action="buy"))
        self.runtime.on_timer(self.context)

        self.assertEqual([s[0] for s in self.gateway.submissions], ["sell"])
        self.assertEqual(self.runtime.pending_count, 1)

        remark = self.runtime.active["remark"]
        self.module._in_preopen_window = lambda: False
        self.runtime.on_order(order_info(remark, status=56, traded=1000))
        self.runtime.on_timer(self.context)
        self.runtime.on_timer(self.context)

        self.assertEqual([s[0] for s in self.gateway.submissions], ["sell", "buy"])
        self.assertEqual(self.runtime.pending_count, 0)

    def test_buy_is_held_in_preopen_window_and_released_after_open(self):
        self.module._in_preopen_window = lambda: True
        self.inbound.put(trade_message("512-0", "sig-buy", action="buy"))
        self.runtime.on_timer(self.context)

        self.assertEqual(self.gateway.submissions, [])
        self.assertEqual(self.runtime.pending_count, 1)

        self.module._in_preopen_window = lambda: False
        self.runtime.on_timer(self.context)

        self.assertEqual(len(self.gateway.submissions), 1)

    def test_limit_down_sell_queues_at_low_limit_without_timeout_cancel(self):
        self.config["limit_down_sell_mode"] = "queue"
        self.gateway.tick = {"lastPrice": 9.0, "askPrice": [9.0], "bidPrice": [0]}
        self.gateway.limits = (11.0, 9.0)
        self.inbound.put(trade_message("520-0", "sig-ld", action="sell"))
        self.runtime.on_timer(self.context)

        self.assertEqual(self.gateway.submissions[0][3], 9.0)
        self.assertEqual(self.gateway.submissions[0][2], 1000)
        remark = self.runtime.active["remark"]
        self.runtime.on_order(order_info(remark, status=50, traded=0))
        self.clock.advance(5.0)
        self.runtime.on_timer(self.context)

        self.assertEqual(self.gateway.cancel_calls, [])

        self.runtime.on_order(order_info(remark, status=56, traded=1000))
        self.runtime.on_timer(self.context)

        self.assertIsNone(self.runtime.active)
        self.assertEqual(self.acks.get_nowait(), "520-0")

    def test_limit_down_sell_skips_when_mode_skip(self):
        self.config["limit_down_sell_mode"] = "skip"
        self.gateway.tick = {"lastPrice": 9.0, "askPrice": [9.0], "bidPrice": [0]}
        self.inbound.put(trade_message("521-0", "sig-ld-skip", action="sell"))
        self.runtime.on_timer(self.context)

        self.assertEqual(self.gateway.submissions, [])
        self.assertIsNone(self.runtime.active)
        self.assertEqual(self.acks.get_nowait(), "521-0")

    def test_limit_up_buy_queues_at_high_limit_when_confirmed(self):
        self.config["limit_up_buy_mode"] = "queue"
        self.gateway.tick = {"lastPrice": 11.0, "askPrice": [0], "bidPrice": [11.0]}
        self.gateway.limits = (11.0, 9.0)
        self.inbound.put(trade_message("522-0", "sig-lu", action="buy"))
        self.runtime.on_timer(self.context)

        self.assertEqual(self.gateway.submissions[0][3], 11.0)

    def test_limit_up_buy_skips_when_mode_skip(self):
        self.config["limit_up_buy_mode"] = "skip"
        self.gateway.tick = {"lastPrice": 11.0, "askPrice": [0], "bidPrice": [11.0]}
        self.gateway.limits = (11.0, 9.0)
        self.inbound.put(trade_message("523-0", "sig-lu-skip", action="buy"))
        self.runtime.on_timer(self.context)

        self.assertEqual(self.gateway.submissions, [])
        self.assertIsNone(self.runtime.active)
        self.assertEqual(self.acks.get_nowait(), "523-0")

    def test_limit_up_queue_cancels_at_deadline_and_expires(self):
        self.config["limit_up_buy_mode"] = "queue"
        self.gateway.tick = {"lastPrice": 11.0, "askPrice": [0], "bidPrice": [11.0]}
        self.gateway.limits = (11.0, 9.0)
        self.inbound.put(trade_message("524-0", "sig-lu-deadline", action="buy"))
        self.runtime.on_timer(self.context)
        remark = self.runtime.active["remark"]
        self.runtime.on_order(order_info(remark, status=50, traded=0))

        deadline = self.runtime._queue_deadline_epoch("14:56:30")
        self.clock.now = deadline + 1
        self.runtime.on_timer(self.context)

        self.assertEqual(self.gateway.cancel_calls, ["order-1"])
        self.assertEqual(self.runtime.active["state"], "CANCEL_REQUESTED")

        self.runtime.on_order(order_info(remark, status=54, traded=0))
        self.runtime.on_timer(self.context)

        self.assertIsNone(self.runtime.active)
        self.assertEqual(self.acks.get_nowait(), "524-0")

    def test_limit_up_queue_rejected_non_hard_stop_retries(self):
        self.config["limit_up_buy_mode"] = "queue"
        self.gateway.tick = {"lastPrice": 11.0, "askPrice": [0], "bidPrice": [11.0]}
        self.gateway.limits = (11.0, 9.0)
        self.inbound.put(trade_message("525-0", "sig-lu-rej", action="buy"))
        self.runtime.on_timer(self.context)
        remark = self.runtime.active["remark"]
        self.runtime.on_order(order_info(remark, status=50, traded=0))
        self.runtime.on_order(
            order_info(remark, status=57, traded=0, error_msg="委托数量不正确")
        )

        self.runtime.on_timer(self.context)

        # 非硬拒单: 与 miniQMT 主循环一致, 刷新后重新排队。
        self.assertEqual(len(self.gateway.submissions), 2)
        self.assertIsNotNone(self.runtime.active)
        self.assertTrue(self.acks.empty())

    def test_limit_up_queue_hard_rejected_terminates_without_retry(self):
        self.config["limit_up_buy_mode"] = "queue"
        self.gateway.tick = {"lastPrice": 11.0, "askPrice": [0], "bidPrice": [11.0]}
        self.gateway.limits = (11.0, 9.0)
        self.inbound.put(trade_message("526-0", "sig-lu-hard", action="buy"))
        self.runtime.on_timer(self.context)
        remark = self.runtime.active["remark"]
        self.runtime.on_order(order_info(remark, status=50, traded=0))
        self.runtime.on_order(
            order_info(remark, status=57, traded=0, error_msg="股票停牌")
        )

        self.runtime.on_timer(self.context)

        self.assertEqual(len(self.gateway.submissions), 1)
        self.assertIsNone(self.runtime.active)
        self.assertEqual(self.acks.get_nowait(), "526-0")

    def test_preopen_limit_down_queue_releases_buy_barrier_once_queued(self):
        """跌停排队卖单确认挂单后立即放行买单, 与 miniQMT 开盘屏障一致。"""
        self.config["limit_down_sell_mode"] = "queue"
        self.gateway.tick = {"lastPrice": 9.0, "askPrice": [9.0], "bidPrice": [0]}
        self.gateway.limits = (11.0, 9.0)
        self.module._in_preopen_window = lambda: True
        self.inbound.put(
            trade_message(
                "527-0", "sig-ld-preopen", action="sell",
                created_at="2026-07-13 09:27:00",
            )
        )
        self.inbound.put(trade_message("528-0", "sig-buy-after-ld", action="buy"))
        self.runtime.on_timer(self.context)

        self.assertEqual([s[0] for s in self.gateway.submissions], ["sell"])
        self.assertEqual(self.runtime.pending_count, 1)
        self.assertEqual(self.runtime._preopen_sell_count, 1)

        # 排队卖单拿到委托号 → 开盘屏障立即释放。
        remark = self.runtime.active["remark"]
        self.module._in_preopen_window = lambda: False
        self.runtime.on_order(order_info(remark, status=50, traded=0))
        self.runtime.on_timer(self.context)
        self.runtime.on_timer(self.context)

        self.assertEqual(self.runtime._preopen_sell_count, 0)
        self.assertEqual(
            [s[0] for s in self.gateway.submissions], ["sell", "buy"]
        )
        self.assertEqual(self.runtime.pending_count, 0)

    def test_buy_target_is_frozen_at_first_resource_cap(self):
        """首次资金缩量后冻结目标, 资金变多不再追买 (与 miniQMT target 语义一致)。"""
        self.gateway.cash = 9500.0
        self.inbound.put(trade_message("529-0", "sig-buy-target", action="buy"))
        self.runtime.on_timer(self.context)
        remark = self.runtime.active["remark"]
        self.assertEqual(self.gateway.submissions[0][2], 900)

        self.runtime.on_order(order_info(remark, status=54, traded=0))
        self.gateway.cash = 100000.0
        self.clock.advance(1.1)
        self.runtime.on_timer(self.context)

        self.assertEqual(len(self.gateway.submissions), 2)
        self.assertEqual(self.gateway.submissions[1][2], 900)

    def test_preopen_sell_first_attempt_gets_half_second_grace_after_open(self):
        """盘前卖单首笔: 开盘后 0.5s 回报宽限, 与 miniQMT 一致。"""
        auction_time = __import__("time").mktime((2026, 7, 13, 9, 27, 0, 0, 0, -1))
        self.clock.now = auction_time
        self.module._in_preopen_window = lambda: False
        self.inbound.put(
            trade_message(
                "530-0", "sig-preopen-grace", action="sell",
                created_at="2026-07-13 09:27:00",
            )
        )
        self.runtime.on_timer(self.context)
        remark = self.runtime.active["remark"]
        self.runtime.on_order(order_info(remark, status=50, traded=0))

        # 9:30:00.8: 宽限 0.5s 已过, 但普通 order_timeout(1s) 还没到 → 应已撤单。
        self.clock.now = __import__("time").mktime((2026, 7, 13, 9, 30, 0, 0, 0, -1)) + 0.8
        self.runtime.on_timer(self.context)

        self.assertEqual(self.gateway.cancel_calls, ["order-1"])
        self.assertEqual(self.runtime.active["state"], "CANCEL_REQUESTED")
        self.assertFalse(self.runtime.halted)


class FakeStartWorker:
    instances = []

    def __init__(self, config, inbound_queue, ack_queue, redis_factory=None):
        self.config = config
        self.inbound_queue = inbound_queue
        self.ack_queue = ack_queue
        self.started = False
        self.stopped = False
        self.__class__.instances.append(self)

    def start(self):
        self.started = True

    def stop(self):
        self.stopped = True


class BigQmtLifecycleTests(unittest.TestCase):
    def setUp(self):
        FakeStartWorker.instances = []
        self.module = load_module()
        self.context = FakeContext()

    def test_disabled_trading_refuses_to_start(self):
        self.module.CONFIG.update(
            {"trading_enabled": False, "account_id": "test-account", "redis_host": "redis"}
        )

        with self.assertRaisesRegex(RuntimeError, "trading_enabled"):
            self.module.init(self.context)

        self.assertEqual(self.context.accounts, [])

    def test_missing_account_or_redis_host_refuses_to_start(self):
        self.module.CONFIG.update(
            {"trading_enabled": True, "account_id": "", "redis_host": "redis"}
        )
        with self.assertRaisesRegex(RuntimeError, "account_id"):
            self.module.init(self.context)

        self.module.CONFIG.update({"account_id": "test-account", "redis_host": ""})
        with self.assertRaisesRegex(RuntimeError, "redis_host"):
            self.module.init(self.context)

    def test_valid_init_binds_account_registers_timer_and_starts_worker(self):
        self.module.CONFIG.update(
            {
                "trading_enabled": True,
                "account_id": "test-account",
                "redis_host": "redis.example.test",
            }
        )
        self.module.RedisStreamWorker = FakeStartWorker

        self.module.init(self.context)

        self.assertEqual(self.context.accounts, ["test-account"])
        self.assertEqual(
            self.context.timers,
            [("qmt_timer", "500nMilliSecond", "2000-01-01 00:00:00", "SH")],
        )
        self.assertTrue(FakeStartWorker.instances[0].started)

    def test_global_callbacks_forward_to_runtime_and_stop_worker(self):
        calls = []

        class RuntimeSpy:
            def __init__(self):
                self.worker = types.SimpleNamespace(stop=lambda: calls.append(("stop",)))

            def on_timer(self, context):
                calls.append(("timer", context))

            def on_order(self, order):
                calls.append(("order", order))

            def on_deal(self, deal):
                calls.append(("deal", deal))

        order = object()
        deal = object()
        self.module._RUNTIME = RuntimeSpy()

        self.module.qmt_timer(self.context)
        self.module.order_callback(self.context, order)
        self.module.deal_callback(self.context, deal)
        self.module.stop(self.context)

        self.assertEqual(
            calls,
            [("timer", self.context), ("order", order), ("deal", deal), ("stop",)],
        )

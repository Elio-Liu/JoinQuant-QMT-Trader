import datetime as dt
import inspect
import json
import sys
import types
import unittest

import joinquant_signal_sender
from joinquant_signal_sender import publish_trade_signal_to_redis, publish_watchlist_to_redis


class FakeContext:
    def __init__(self, current_dt):
        self.current_dt = current_dt


class FakeRedisClient:
    def __init__(self):
        self.xadd_calls = []

    def xadd(self, stream, fields, maxlen=None, approximate=True):
        self.xadd_calls.append(
            {
                "stream": stream,
                "fields": fields,
                "maxlen": maxlen,
                "approximate": approximate,
            }
        )
        return "1717800000000-0"


class CountingRedisFactory:
    def __init__(self):
        self.client = FakeRedisClient()
        self.created = 0

    def __call__(self, **_):
        self.created += 1
        return self.client


class JoinQuantSenderTests(unittest.TestCase):
    def setUp(self):
        self.original_redis = sys.modules.get("redis")
        for attr in ("_redis_client", "_redis_config_key"):
            if hasattr(publish_trade_signal_to_redis, attr):
                delattr(publish_trade_signal_to_redis, attr)

    def tearDown(self):
        if self.original_redis is None:
            sys.modules.pop("redis", None)
        else:
            sys.modules["redis"] = self.original_redis

    def test_publish_live_signal_uses_redis_stream_xadd(self):
        now = dt.datetime.now()
        client = FakeRedisClient()
        self._install_fake_redis(client)

        result = publish_trade_signal_to_redis(
            FakeContext(now),
            "sell",
            "000001.XSHE",
            500,
            10.5,
        )

        self.assertTrue(result["sent"])
        self.assertEqual(result["redis_message_id"], "1717800000000-0")
        self.assertEqual(client.xadd_calls[0]["stream"], "tidal_quant_signals")
        payload = json.loads(client.xadd_calls[0]["fields"]["payload"])
        expected_signal_id = "hunter-{}-000001XSHE-sell-500".format(now.strftime("%Y%m%d%H%M%S"))
        self.assertEqual(payload["signal_id"], expected_signal_id)
        self.assertEqual(payload["action"], "sell")
        self.assertEqual(payload["reference_price"], 10.5)
        self.assertEqual(payload["mode"], "live")

    def test_backtest_signal_is_not_sent(self):
        context_time = dt.datetime(2026, 6, 8, 9, 30, 1)
        client = FakeRedisClient()
        self._install_fake_redis(client)

        result = publish_trade_signal_to_redis(
            FakeContext(context_time),
            "buy",
            "000001.XSHE",
            1000,
            10.0,
        )

        self.assertFalse(result["sent"])
        self.assertEqual(result["mode"], "backtest")
        self.assertEqual(client.xadd_calls, [])

    def test_publish_function_keeps_original_strategy_call_signature(self):
        signature = inspect.signature(publish_trade_signal_to_redis)

        self.assertEqual(list(signature.parameters), ["context", "action", "code", "amount", "price"])

    def test_live_signal_reuses_cached_redis_connection(self):
        now = dt.datetime.now()
        factory = CountingRedisFactory()
        sys.modules["redis"] = types.SimpleNamespace(Redis=factory)

        first = publish_trade_signal_to_redis(FakeContext(now), "buy", "000001.XSHE", 1000, 10.0)
        second = publish_trade_signal_to_redis(FakeContext(now), "sell", "000002.XSHE", 500, 20.0)

        self.assertTrue(first["sent"])
        self.assertTrue(second["sent"])
        self.assertEqual(factory.created, 1)
        self.assertEqual(len(factory.client.xadd_calls), 2)

    def test_joinquant_sender_exposes_only_publish_functions(self):
        public_functions = [
            name
            for name, value in vars(joinquant_signal_sender).items()
            if inspect.isfunction(value) and not name.startswith("_")
        ]

        self.assertEqual(
            public_functions,
            ["publish_trade_signal_to_redis", "publish_watchlist_to_redis"],
        )

    def test_watchlist_publishes_subscribe_command(self):
        now = dt.datetime.now()
        client = FakeRedisClient()
        self._install_fake_redis(client)

        result = publish_watchlist_to_redis(FakeContext(now), ["000001.XSHE", "510300.XSHG"])

        self.assertTrue(result["sent"])
        self.assertEqual(client.xadd_calls[0]["stream"], "tidal_quant_signals")
        payload = json.loads(client.xadd_calls[0]["fields"]["payload"])
        self.assertEqual(payload["action"], "subscribe")
        self.assertEqual(payload["codes"], ["000001.XSHE", "510300.XSHG"])

    def test_watchlist_skipped_in_backtest(self):
        client = FakeRedisClient()
        self._install_fake_redis(client)

        result = publish_watchlist_to_redis(
            FakeContext(dt.datetime(2026, 6, 8, 9, 25, 50)), ["000001.XSHE"]
        )

        self.assertFalse(result["sent"])
        self.assertEqual(client.xadd_calls, [])

    def test_watchlist_reuses_trade_function_connection(self):
        now = dt.datetime.now()
        factory = CountingRedisFactory()
        sys.modules["redis"] = types.SimpleNamespace(Redis=factory)

        publish_trade_signal_to_redis(FakeContext(now), "buy", "000001.XSHE", 1000, 10.0)
        publish_watchlist_to_redis(FakeContext(now), ["000001.XSHE"])

        self.assertEqual(factory.created, 1)
        self.assertEqual(len(factory.client.xadd_calls), 2)

    def _install_fake_redis(self, client):
        fake_module = types.SimpleNamespace(Redis=lambda **_: client)
        sys.modules["redis"] = fake_module


if __name__ == "__main__":
    unittest.main()

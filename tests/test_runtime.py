import json
import os
import sys
import tempfile
import types
import unittest
from pathlib import Path
from types import SimpleNamespace

from qmt_follower.config import load_config
from qmt_follower.models import Action
from qmt_follower.redis_stream import RedisStreamClient, _parse_message, _parse_signal


class RuntimeTests(unittest.TestCase):
    def setUp(self):
        self.original_redis = sys.modules.get("redis")

    def tearDown(self):
        if self.original_redis is None:
            sys.modules.pop("redis", None)
        else:
            sys.modules["redis"] = self.original_redis

    def test_config_loads_execution_values_and_env_password(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "config.yaml"
            config_path.write_text(
                json.dumps(
                    {
                        "redis": {
                            "host": "127.0.0.1",
                            "port": 6379,
                            "password": "${TEST_REDIS_PASSWORD}",
                            "stream": "signals",
                            "group": "executors",
                            "consumer": "worker-1",
                            "block_ms": 25,
                        },
                        "execution": {
                            "buy_slippage_pct": 0.005,
                            "max_attempts": 5,
                        },
                        "state_db": str(Path(tmpdir) / "state.db"),
                    }
                ),
                encoding="utf-8",
            )
            os.environ["TEST_REDIS_PASSWORD"] = "secret"

            config = load_config(config_path)

        self.assertEqual(config.redis.password, "secret")
        self.assertEqual(config.redis.block_ms, 25)
        self.assertEqual(config.execution.buy_slippage_pct, 0.005)
        self.assertEqual(config.execution.max_attempts, 5)

    def test_redis_stream_uses_configured_low_latency_block_ms_by_default(self):
        class FakeRedis:
            def __init__(self, **_):
                self.block_values = []
                self.created_group = False

            def xgroup_create(self, *_args, **_kwargs):
                self.created_group = True

            def xreadgroup(self, *_args, **kwargs):
                self.block_values.append(kwargs["block"])
                return [
                    (
                        "signals",
                        [
                            (
                                "1-0",
                                {
                                    "payload": json.dumps(
                                        {
                                            "signal_id": "sig-1",
                                            "strategy_id": "hunter",
                                            "action": "buy",
                                            "code": "000001.XSHE",
                                            "amount": 1000,
                                            "reference_price": 10.0,
                                            "created_at": "2026-06-08 09:30:00",
                                            "expire_at": "2026-06-08 09:30:20",
                                        }
                                    )
                                },
                            )
                        ],
                    )
                ]

        fake = FakeRedis()
        sys.modules["redis"] = types.SimpleNamespace(Redis=lambda **_: fake)

        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "config.yaml"
            config_path.write_text(
                json.dumps(
                    {
                        "redis": {
                            "host": "127.0.0.1",
                            "port": 6379,
                            "password": None,
                            "stream": "signals",
                            "group": "executors",
                            "consumer": "worker-1",
                            "block_ms": 20,
                        }
                    }
                ),
                encoding="utf-8",
            )
            config = load_config(config_path)

        reader = RedisStreamClient(config.redis).read_forever()
        message = next(reader)

        self.assertEqual(fake.block_values, [20])
        self.assertEqual(message.message_id, "1-0")
        self.assertEqual(message.signal.expire_at, "2026-06-08 09:30:20")

    def test_redis_group_creation_log_uses_one_tag_and_one_emoji(self):
        class FakeRedis:
            @staticmethod
            def xgroup_create(*_args, **_kwargs):
                return None

        stream = RedisStreamClient.__new__(RedisStreamClient)
        stream.client = FakeRedis()
        stream.config = SimpleNamespace(stream="signals", group="executors")

        with self.assertLogs("qmt_follower.redis_stream", level="INFO") as captured:
            stream.ensure_group()

        message = captured.records[0].getMessage()
        self.assertTrue(message.startswith("【Redis】📡 "))
        self.assertNotIn("【系统】【Redis】", message)

    def test_parse_signal_accepts_json_payload(self):
        signal = _parse_signal(
            {
                "payload": json.dumps(
                    {
                        "signal_id": "sig-1",
                        "strategy_id": "hunter",
                        "action": "buy",
                        "code": "000001.XSHE",
                        "amount": 1000,
                        "reference_price": 10.0,
                        "created_at": "2026-06-08 09:30:00",
                        "execute_at": "2026-07-13 09:30:00",
                    }
                )
            }
        )

        self.assertEqual(signal.signal_id, "sig-1")
        self.assertEqual(signal.action, Action.BUY)
        self.assertEqual(signal.execute_at, "2026-07-13 09:30:00")

    def test_parse_message_recognizes_subscribe_command(self):
        message = _parse_message(
            "2-0",
            {
                "payload": json.dumps(
                    {
                        "action": "subscribe",
                        "codes": ["000001.XSHE", "510300.XSHG"],
                        "strategy_id": "hunter",
                    }
                )
            },
        )

        self.assertIsNone(message.signal)
        self.assertEqual(message.watchlist.codes, ("000001.XSHE", "510300.XSHG"))
        self.assertEqual(message.watchlist.strategy_id, "hunter")

    def test_parse_message_still_parses_trade_signal(self):
        message = _parse_message(
            "3-0",
            {
                "payload": json.dumps(
                    {
                        "signal_id": "sig-9",
                        "strategy_id": "hunter",
                        "action": "buy",
                        "code": "000001.XSHE",
                        "amount": 1000,
                        "reference_price": 10.0,
                        "created_at": "2026-06-08 09:30:00",
                    }
                )
            },
        )

        self.assertIsNone(message.watchlist)
        self.assertEqual(message.signal.signal_id, "sig-9")


if __name__ == "__main__":
    unittest.main()

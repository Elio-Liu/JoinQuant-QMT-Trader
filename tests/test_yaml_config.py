import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import yaml

from miniqmt_follower.config import load_config


class YamlConfigTests(unittest.TestCase):
    def test_config_loads_yaml_values_and_env_password(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "config.yaml"
            config_path.write_text(
                """redis:
  host: 127.0.0.1
  port: 6379
  password: ${TEST_REDIS_PASSWORD}
  stream: signals
  group: executors
  consumer: worker-1
  block_ms: 25
  allowed_strategy_ids: ["harvester"]
execution:
  buy_slippage_pct: 0.005
  max_attempts: 5
  cancel_confirm_timeout_sec: 12
state_db: state.db
""",
                encoding="utf-8",
            )
            with patch.dict(os.environ, {"TEST_REDIS_PASSWORD": "secret"}):
                config = load_config(config_path)
        self.assertEqual(config.redis.password, "secret")
        self.assertEqual(config.execution.max_attempts, 5)
        self.assertEqual(config.redis.allowed_strategy_ids, ("harvester",))
        self.assertEqual(config.execution.cancel_confirm_timeout_sec, 12.0)

    def test_allowlist_defaults_off(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "config.yaml"
            config_path.write_text(
                """redis:
  host: 127.0.0.1
state_db: state.db
""",
                encoding="utf-8",
            )
            config = load_config(config_path)
        self.assertEqual(config.redis.allowed_strategy_ids, ())

    def test_auction_aggressive_pct_loads_and_defaults_on(self):
        """竞价排队报价默认开启(2%), 配置可覆盖或置 0 关闭。"""
        with tempfile.TemporaryDirectory() as tmpdir:
            default_path = Path(tmpdir) / "default.yaml"
            default_path.write_text(
                """redis:
  host: 127.0.0.1
state_db: state.db
""",
                encoding="utf-8",
            )
            override_path = Path(tmpdir) / "override.yaml"
            override_path.write_text(
                """redis:
  host: 127.0.0.1
execution:
  auction_aggressive_pct: 0
state_db: state.db
""",
                encoding="utf-8",
            )
            self.assertEqual(load_config(default_path).execution.auction_aggressive_pct, 0.02)
            self.assertEqual(load_config(override_path).execution.auction_aggressive_pct, 0.0)

    def test_limit_up_buy_queue_config_loads(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "config.yaml"
            config_path.write_text(
                """redis:
  host: 127.0.0.1
execution:
  skip_buy_when_limit_up: true
  limit_up_buy_mode: queue
  queue_buy_deadline: "14:56:30"
  max_concurrent_queue_buys: 5
state_db: state.db
""",
                encoding="utf-8",
            )

            execution = load_config(config_path).execution

        self.assertEqual(execution.limit_up_buy_mode, "queue")
        self.assertEqual(execution.effective_limit_up_buy_mode(), "queue")
        self.assertEqual(execution.queue_buy_deadline, "14:56:30")
        self.assertEqual(execution.max_concurrent_queue_buys, 5)

    def test_legacy_limit_up_skip_flag_is_preserved_when_mode_is_unset(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "config.yaml"
            config_path.write_text(
                """redis:
  host: 127.0.0.1
execution:
  skip_buy_when_limit_up: true
state_db: state.db
""",
                encoding="utf-8",
            )

            execution = load_config(config_path).execution

        self.assertEqual(execution.limit_up_buy_mode, "")
        self.assertEqual(execution.effective_limit_up_buy_mode(), "skip")

    def test_invalid_limit_up_buy_mode_is_rejected(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "config.yaml"
            config_path.write_text(
                """redis:
  host: 127.0.0.1
execution:
  limit_up_buy_mode: forever
state_db: state.db
""",
                encoding="utf-8",
            )

            with self.assertRaisesRegex(ValueError, "limit_up_buy_mode"):
                load_config(config_path)

    def test_config_rejects_non_yaml_path(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "config.json"
            config_path.write_text('{"redis": {"host": "127.0.0.1"}}', encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "YAML"):
                load_config(config_path)

    def test_example_yaml_is_a_mapping_without_comment_keys(self):
        raw = yaml.safe_load(
            (Path(__file__).parents[1] / "config.example.yaml").read_text(encoding="utf-8")
        )
        self.assertIsInstance(raw, dict)
        self.assertIn("redis", raw)
        self.assertIn("execution", raw)
        self.assertNotIn("_comment", raw)
        self.assertEqual(raw["execution"]["limit_up_buy_mode"], "queue")
        self.assertEqual(raw["execution"]["queue_buy_deadline"], "14:56:30")
        self.assertEqual(raw["execution"]["max_concurrent_queue_buys"], 5)


class PlanDrivenConfigTests(unittest.TestCase):
    def test_plan_driven_defaults(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "default.yaml"
            config_path.write_text(
                """redis:\n  host: 127.0.0.1\nstate_db: state.db\n""",
                encoding="utf-8",
            )
            execution = load_config(config_path).execution
        self.assertTrue(execution.plan_enabled)
        self.assertEqual(execution.plan_execute_at, "09:30:00")
        self.assertEqual(execution.max_single_position_pct, 0.2)

    def test_plan_driven_override(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "override.yaml"
            config_path.write_text(
                "redis:\n"
                "  host: 127.0.0.1\n"
                "execution:\n"
                "  plan_enabled: false\n"
                '  plan_execute_at: "09:31:00"\n'
                "  max_single_position_pct: 0.25\n"
                "state_db: state.db\n",
                encoding="utf-8",
            )
            execution = load_config(config_path).execution
        self.assertFalse(execution.plan_enabled)
        self.assertEqual(execution.plan_execute_at, "09:31:00")
        self.assertEqual(execution.max_single_position_pct, 0.25)

    def test_invalid_plan_values_rejected(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            bad_time = Path(tmpdir) / "bad_time.yaml"
            bad_time.write_text(
                "redis:\n  host: 127.0.0.1\n"
                'execution:\n  plan_execute_at: "0930"\nstate_db: state.db\n',
                encoding="utf-8",
            )
            with self.assertRaises(ValueError):
                load_config(bad_time)
            bad_pct = Path(tmpdir) / "bad_pct.yaml"
            bad_pct.write_text(
                "redis:\n  host: 127.0.0.1\n"
                "execution:\n  max_single_position_pct: 1.5\nstate_db: state.db\n",
                encoding="utf-8",
            )
            with self.assertRaises(ValueError):
                load_config(bad_pct)


if __name__ == "__main__":
    unittest.main()

import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import yaml

from qmt_follower.config import load_config


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
execution:
  buy_slippage_pct: 0.005
  max_attempts: 5
state_db: state.db
""",
                encoding="utf-8",
            )
            with patch.dict(os.environ, {"TEST_REDIS_PASSWORD": "secret"}):
                config = load_config(config_path)
        self.assertEqual(config.redis.password, "secret")
        self.assertEqual(config.execution.max_attempts, 5)

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


if __name__ == "__main__":
    unittest.main()

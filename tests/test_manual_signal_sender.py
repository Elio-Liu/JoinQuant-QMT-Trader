import importlib.util
import tempfile
import unittest
from pathlib import Path


MODULE_PATH = Path("scripts/redis_target_config.py")
MANUAL_SCRIPT_PATH = Path("scripts/send_manual_signal.py")


class ManualSignalSenderTests(unittest.TestCase):
    def setUp(self):
        self.assertTrue(MODULE_PATH.exists(), "shared Redis target loader is not implemented")
        spec = importlib.util.spec_from_file_location("redis_target_config", MODULE_PATH)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        self.config = module

    def test_default_config_is_next_to_sender_scripts(self):
        self.assertEqual(self.config.DEFAULT_CONFIG_PATH, MODULE_PATH.with_name("redis_targets.yaml").resolve())

    def test_remote_target_is_loaded_from_yaml(self):
        config_path = self._write_config(
            """
targets:
  remote_prod:
    host: redis.example.test
    port: 6380
    password: test-secret
    stream: test-signals
"""
        )

        target = self.config.load_redis_target("remote_prod", config_path)

        self.assertEqual(target["host"], "redis.example.test")
        self.assertEqual(target["port"], 6380)
        self.assertEqual(target["password"], "test-secret")
        self.assertEqual(target["stream"], "test-signals")

    def test_missing_target_is_rejected(self):
        config_path = self._write_config("targets:\n  local:\n    host: 127.0.0.1\n")

        with self.assertRaisesRegex(ValueError, "remote_prod"):
            self.config.load_redis_target("remote_prod", config_path)

    def test_invalid_port_is_rejected(self):
        for port in ("not-a-port", 0, 65536):
            with self.subTest(port=port):
                config_path = self._write_config(
                    """
targets:
  remote_prod:
    host: redis.example.test
    port: {}
    password: null
    stream: test-signals
""".format(port)
                )

                with self.assertRaisesRegex(ValueError, "port"):
                    self.config.load_redis_target("remote_prod", config_path)

    def test_password_must_be_string_or_null(self):
        config_path = self._write_config(
            """
targets:
  remote_prod:
    host: redis.example.test
    port: 6379
    password: 123456
    stream: test-signals
"""
        )

        with self.assertRaisesRegex(ValueError, "password"):
            self.config.load_redis_target("remote_prod", config_path)

    def test_manual_sender_uses_shared_loader_without_legacy_helper(self):
        spec = importlib.util.spec_from_file_location("send_manual_signal", MANUAL_SCRIPT_PATH)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)

        self.assertTrue(hasattr(module, "load_redis_target"))
        self.assertEqual(module.load_redis_target.__module__, "scripts.redis_target_config")
        self.assertFalse(hasattr(module, "get_redis_target"))
        self.assertNotIn("os", vars(module))

    def _write_config(self, text):
        tempdir = tempfile.TemporaryDirectory()
        self.addCleanup(tempdir.cleanup)
        path = Path(tempdir.name) / "redis_targets.yaml"
        path.write_text(text, encoding="utf-8")
        return path



if __name__ == "__main__":
    unittest.main()

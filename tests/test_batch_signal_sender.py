import datetime as dt
import importlib.util
import unittest
from pathlib import Path


SCRIPT_PATH = Path("scripts/send_batch_signals.py")


class BatchSignalSenderTests(unittest.TestCase):
    def setUp(self):
        self.assertTrue(SCRIPT_PATH.exists(), "batch signal sender script is not implemented")
        spec = importlib.util.spec_from_file_location("send_batch_signals", SCRIPT_PATH)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        self.sender = module

    def test_default_stress_batch_matches_current_sell_and_buy_totals(self):
        self.sender.validate_batch(
            self.sender.SIGNALS,
            self.sender.MAX_SELL_QUANTITIES,
            self.sender.SEND_INTERVAL_SEC,
        )

        sells = [signal for signal in self.sender.SIGNALS if signal["action"] == "sell"]
        buys = [signal for signal in self.sender.SIGNALS if signal["action"] == "buy"]

        self.assertEqual(len(self.sender.SIGNALS), 10)
        self.assertEqual(sum(signal["amount"] for signal in sells), 5000)
        self.assertEqual(sum(signal["amount"] for signal in buys), 500)
        self.assertEqual([signal["action"] for signal in self.sender.SIGNALS[:5]], ["sell"] * 5)
        self.assertEqual([signal["action"] for signal in self.sender.SIGNALS[5:]], ["buy"] * 5)

    def test_batch_sender_uses_shared_loader_without_manual_sender_dependency(self):
        self.assertTrue(hasattr(self.sender, "load_redis_target"))
        source = SCRIPT_PATH.read_text(encoding="utf-8")
        self.assertNotIn("from scripts.send_manual_signal import get_redis_target", source)

    def test_build_payloads_assigns_unique_ordered_signal_ids(self):
        signals = [
            {"code": "517110.XSHG", "action": "sell", "amount": 2700, "reference_price": 0.71},
            {"code": "159309.XSHE", "action": "buy", "amount": 1400, "reference_price": 1.26},
        ]

        payloads = self.sender.build_payloads(signals, "manual_stress", "batch-001")

        self.assertEqual(len({payload["signal_id"] for payload in payloads}), 2)
        self.assertIn("batch-001-01", payloads[0]["signal_id"])
        self.assertIn("batch-001-02", payloads[1]["signal_id"])
        self.assertEqual([payload["action"] for payload in payloads], ["sell", "buy"])

    def test_validate_batch_rejects_sell_total_above_available_position(self):
        signals = [
            {"code": "517110.XSHG", "action": "sell", "amount": 13800, "reference_price": 0.71},
        ]

        with self.assertRaisesRegex(ValueError, "13700"):
            self.sender.validate_batch(signals, {"517110.XSHG": 13700}, 0.1)

    def test_publish_batch_waits_configured_interval_between_messages(self):
        class FakeStream:
            def __init__(self):
                self.payloads = []

            def publish_signal(self, payload):
                self.payloads.append(payload.copy())
                return f"message-{len(self.payloads)}"

        signals = [
            {"code": "517110.XSHG", "action": "sell", "amount": 100, "reference_price": 0.71},
            {"code": "517110.XSHG", "action": "sell", "amount": 100, "reference_price": 0.71},
            {"code": "159309.XSHE", "action": "buy", "amount": 100, "reference_price": 1.26},
        ]
        payloads = self.sender.build_payloads(signals, "manual_stress", "batch-002")
        times = iter(
            [
                dt.datetime(2026, 7, 10, 10, 0, 0, 1000),
                dt.datetime(2026, 7, 10, 10, 0, 0, 2000),
                dt.datetime(2026, 7, 10, 10, 0, 0, 3000),
            ]
        )
        sleeps = []
        stream = FakeStream()

        published = self.sender.publish_batch(
            stream,
            payloads,
            interval_sec=0.25,
            now_fn=lambda: next(times),
            sleep_fn=sleeps.append,
        )

        self.assertEqual(sleeps, [0.25, 0.25])
        self.assertEqual([item["message_id"] for item in published], ["message-1", "message-2", "message-3"])
        self.assertEqual(len(stream.payloads), 3)
        self.assertTrue(all("created_at" in payload for payload in stream.payloads))
        self.assertTrue(all("sent_at_ms" in payload for payload in stream.payloads))


if __name__ == "__main__":
    unittest.main()

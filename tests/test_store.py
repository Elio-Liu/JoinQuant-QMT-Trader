import json
import tempfile
import threading
import unittest
from pathlib import Path

from qmt_follower.models import Action, ExecutionStatus, TradeSignal
from qmt_follower.store import SQLiteExecutionStore


class StoreTests(unittest.TestCase):
    def test_signal_expire_at_is_preserved_in_raw_json(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            store = SQLiteExecutionStore(Path(tmpdir) / "state.db")
            signal = TradeSignal(
                signal_id="sig-expiry-audit",
                strategy_id="hunter",
                action=Action.BUY,
                code="000001.XSHE",
                amount=1000,
                reference_price=10.0,
                created_at="2026-07-10 09:30:00",
                expire_at="2026-07-10 09:30:20",
            )

            store.try_accept_signal(signal)

            with store._connect() as conn:
                row = conn.execute(
                    "SELECT raw_json FROM signals WHERE signal_id = ?",
                    (signal.signal_id,),
                ).fetchone()
            raw = json.loads(row["raw_json"])
            self.assertEqual(raw["expire_at"], "2026-07-10 09:30:20")

    def test_two_threads_can_accept_distinct_signals_and_persist_them(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            store = SQLiteExecutionStore(Path(tmpdir) / "state.db")
            first_registered = threading.Event()
            release_first = threading.Event()
            results = []
            errors = []

            def make_signal(signal_id):
                return TradeSignal(
                    signal_id=signal_id,
                    strategy_id="stable_discount_hunter",
                    action=Action.BUY,
                    code="159309.XSHE",
                    amount=76800,
                    reference_price=1.26,
                    created_at="2026-07-10 09:30:11",
                )

            def accept_first():
                try:
                    results.append(store.try_accept_signal(make_signal("concurrent-1")))
                    first_registered.set()
                    release_first.wait(timeout=6.0)
                except Exception as exc:
                    errors.append(exc)
                finally:
                    store.close()

            def accept_second():
                try:
                    self.assertTrue(first_registered.wait(timeout=1.0))
                    results.append(store.try_accept_signal(make_signal("concurrent-2")))
                except Exception as exc:
                    errors.append(exc)
                finally:
                    release_first.set()
                    store.close()

            threads = [threading.Thread(target=accept_first), threading.Thread(target=accept_second)]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join(timeout=7.0)

            self.assertFalse(any(thread.is_alive() for thread in threads))
            self.assertEqual(errors, [])
            self.assertEqual(results, [True, True])
            with store._connect() as conn:
                persisted = conn.execute(
                    "SELECT COUNT(*) FROM signals WHERE signal_id IN (?, ?)",
                    ("concurrent-1", "concurrent-2"),
                ).fetchone()[0]
            self.assertEqual(persisted, 2)

    def test_signal_id_can_only_be_accepted_once(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            store = SQLiteExecutionStore(Path(tmpdir) / "state.db")
            signal = TradeSignal(
                signal_id="sig-1",
                strategy_id="hunter",
                action=Action.BUY,
                code="000001.XSHE",
                amount=1000,
                reference_price=10.0,
                created_at="2026-06-08 09:30:00",
            )

            self.assertTrue(store.try_accept_signal(signal))
            self.assertFalse(store.try_accept_signal(signal))

    def test_status_updates_are_persisted(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            store = SQLiteExecutionStore(Path(tmpdir) / "state.db")
            signal = TradeSignal(
                signal_id="sig-2",
                strategy_id="hunter",
                action=Action.SELL,
                code="000001.XSHE",
                amount=500,
                reference_price=20.0,
                created_at="2026-06-08 09:31:00",
            )

            store.try_accept_signal(signal)
            store.update_signal_status(signal.signal_id, ExecutionStatus.FILLED, filled_qty=500)

            record = store.get_signal(signal.signal_id)
            self.assertEqual(record.status, ExecutionStatus.FILLED)
            self.assertEqual(record.filled_qty, 500)

    def test_store_enables_wal_for_lower_latency_writes(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            store = SQLiteExecutionStore(Path(tmpdir) / "state.db")

            with store._connect() as conn:
                journal_mode = conn.execute("PRAGMA journal_mode").fetchone()[0]
                synchronous = conn.execute("PRAGMA synchronous").fetchone()[0]

            self.assertEqual(journal_mode.lower(), "wal")
            self.assertEqual(synchronous, 1)


if __name__ == "__main__":
    unittest.main()

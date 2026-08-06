import datetime as dt
import threading
import unittest
from unittest import mock

from miniqmt_follower.models import Action, TradeSignal
from miniqmt_follower.opening import (
    OpeningSellBarrier,
    is_preopen_sell,
    seconds_until_market_open,
)


def signal(action: Action, created_at: str, signal_id: str = "sig-1") -> TradeSignal:
    return TradeSignal(
        signal_id=signal_id,
        strategy_id="harvester",
        action=action,
        code="000001.XSHE",
        amount=1000,
        reference_price=10.0,
        created_at=created_at,
    )


class OpeningSellBarrierTests(unittest.TestCase):
    def test_market_open_wait_only_applies_from_0925_to_0930(self):
        cases = [
            (dt.datetime(2026, 8, 3, 9, 24, 59), 0.0),
            (dt.datetime(2026, 8, 3, 9, 25, 0), 300.0),
            (dt.datetime(2026, 8, 3, 9, 29, 59), 1.0),
            (dt.datetime(2026, 8, 3, 9, 30, 0), 0.0),
        ]
        for now, expected in cases:
            with self.subTest(now=now), mock.patch(
                "miniqmt_follower.opening.dt.datetime"
            ) as datetime_mock:
                datetime_mock.now.return_value = now
                self.assertEqual(seconds_until_market_open(), expected)

    def test_only_0925_to_0930_sell_is_registered(self):
        self.assertTrue(is_preopen_sell(signal(Action.SELL, "2026-08-03 09:25:00")))
        self.assertTrue(is_preopen_sell(signal(Action.SELL, "2026-08-03 09:29:59")))
        self.assertFalse(is_preopen_sell(signal(Action.SELL, "2026-08-03 09:30:00")))
        self.assertFalse(is_preopen_sell(signal(Action.BUY, "2026-08-03 09:29:00")))
        self.assertFalse(is_preopen_sell(signal(Action.SELL, "invalid")))

    def test_buy_waiter_is_released_only_after_all_registered_sells(self):
        barrier = OpeningSellBarrier()
        barrier.register(signal(Action.SELL, "2026-08-03 09:27:00", "sell-1"))
        barrier.register(signal(Action.SELL, "2026-08-03 09:28:00", "sell-2"))
        waiter_done = threading.Event()
        waiter = threading.Thread(
            target=lambda: (barrier.wait_until_released(), waiter_done.set())
        )
        waiter.start()
        self.assertFalse(waiter_done.wait(0.05))
        barrier.release("sell-1")
        self.assertFalse(waiter_done.wait(0.05))
        barrier.release("sell-2")
        self.assertTrue(waiter_done.wait(0.5))
        waiter.join()

    def test_release_is_idempotent(self):
        barrier = OpeningSellBarrier()
        barrier.register(signal(Action.SELL, "2026-08-03 09:27:00"))
        barrier.release("sig-1")
        barrier.release("sig-1")
        self.assertEqual(barrier.pending_count(), 0)

    def test_duplicate_preopen_sell_cannot_release_original_early(self):
        barrier = OpeningSellBarrier()
        duplicate = signal(Action.SELL, "2026-08-03 09:27:00", "same-sell")
        barrier.register(duplicate)
        barrier.register(duplicate)
        waiter_done = threading.Event()
        waiter = threading.Thread(
            target=lambda: (barrier.wait_until_released(), waiter_done.set())
        )
        waiter.start()

        barrier.release("same-sell")

        self.assertFalse(waiter_done.wait(0.05))
        barrier.release("same-sell")
        self.assertTrue(waiter_done.wait(0.5))
        waiter.join()

    def test_limit_down_queue_acceptance_releases_all_duplicate_work_items(self):
        barrier = OpeningSellBarrier()
        duplicate = signal(Action.SELL, "2026-08-03 09:27:00", "same-sell")
        barrier.register(duplicate)
        barrier.register(duplicate)

        barrier.release_all("same-sell")

        self.assertEqual(barrier.pending_count(), 0)


if __name__ == "__main__":
    unittest.main()

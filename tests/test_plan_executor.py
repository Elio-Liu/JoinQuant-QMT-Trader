import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from miniqmt_follower.models import (
    Action,
    DailyPlan,
    ExecutionResult,
    ExecutionStatus,
)
from miniqmt_follower.opening import OpeningSellBarrier
from miniqmt_follower.plan_executor import PlanExecutor, submit_plan_tasks
from miniqmt_follower.store import SQLiteExecutionStore


class RecordingBroker:
    def __init__(self, positions):
        self.positions = positions

    def query_position(self, code):
        return self.positions.get(code, 0)


class RecordingEngine:
    def __init__(self):
        self.executed = []

    def execute(self, signal):
        self.executed.append(signal)
        return ExecutionResult(
            signal_id=signal.signal_id,
            status=ExecutionStatus.FILLED,
            requested_qty=signal.amount,
            filled_qty=signal.amount,
            attempts=1,
        )


def _execute_safe(engine, signal, opening_barrier):
    return engine.execute(signal)


def _plan(**kw):
    defaults = dict(
        signal_id="harvester-20260806-plan",
        strategy_id="harvester",
        codes_to_sell=(),
        codes_to_buy=(),
        created_at="2026-08-06 09:28:00",
        mode="live",
        sent_at_ms=None,
    )
    defaults.update(kw)
    return DailyPlan(**defaults)


class PlanExecutorTests(unittest.TestCase):
    def test_derived_signals_skip_existing_positions(self):
        executor = PlanExecutor(
            store=object(),
            broker=RecordingBroker({"600000.XSHG": 500}),
        )
        plan = _plan(
            codes_to_sell=["000001.XSHE"],
            codes_to_buy=["600000.XSHG", "000002.XSHE"],
        )
        derived = list(executor.derived_signals(plan))
        self.assertEqual(
            [s.signal_id for s in derived],
            [
                "harvester-20260806-plan-sell-000001XSHE",
                "harvester-20260806-plan-buy-000002XSHE",
            ],
        )
        sell, buy = derived
        self.assertEqual((sell.action, sell.quantity_mode), (Action.SELL, "sell_all"))
        self.assertEqual(
            (buy.action, buy.quantity_mode, buy.budget_group_size),
            (Action.BUY, "auto_buy", 1),
        )

    def test_submit_plan_tasks_combines_until_all_done(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            store = SQLiteExecutionStore(Path(tmpdir) / "state.db")
            executor = PlanExecutor(store=store, broker=RecordingBroker({}))
            engine = RecordingEngine()
            barrier = OpeningSellBarrier()
            plan = _plan(
                codes_to_sell=["000001.XSHE"],
                codes_to_buy=["600000.XSHG", "000002.XSHE"],
            )
            with ThreadPoolExecutor(max_workers=4) as sell_pool, ThreadPoolExecutor(
                max_workers=4
            ) as buy_pool:
                pools = {Action.SELL: sell_pool, Action.BUY: buy_pool}
                combined = submit_plan_tasks(
                    plan, executor, pools, engine, _execute_safe, barrier,
                )
                results = combined.result(timeout=10)
        self.assertEqual(len(results), 3)
        self.assertEqual(len(engine.executed), 3)
        self.assertEqual(
            {s.signal_id for s in engine.executed},
            {
                "harvester-20260806-plan-sell-000001XSHE",
                "harvester-20260806-plan-buy-600000XSHG",
                "harvester-20260806-plan-buy-000002XSHE",
            },
        )

    def test_record_plan_audit_is_idempotent(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            store = SQLiteExecutionStore(Path(tmpdir) / "state.db")
            executor = PlanExecutor(store=store, broker=RecordingBroker({}))
            plan = _plan(codes_to_sell=["000001.XSHE"])
            self.assertTrue(executor.record(plan))
            self.assertFalse(executor.record(plan))

    def test_submit_plan_tasks_returns_none_when_nothing_to_do(self):
        executor = PlanExecutor(store=object(), broker=RecordingBroker({}))
        plan = _plan()
        self.assertIsNone(
            submit_plan_tasks(
                plan, executor, {}, object(), lambda e, s, b: None, OpeningSellBarrier(),
            )
        )


if __name__ == "__main__":
    unittest.main()

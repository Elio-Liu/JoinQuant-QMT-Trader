"""日计划执行: 把 plan 消息展开为派生交易信号并投递到执行池。

派生信号 id 由 plan.signal_id 确定性生成（f"{plan_id}-sell-{code}" /
f"{plan_id}-buy-{code}"），走现有 signals 表幂等 —— 重启后 Redis 重投 plan
会重新派生，已受理的派生信号被幂等拦截，未受理的继续执行，不会重复下单。
"""

from __future__ import annotations

import re
import threading
from collections.abc import Iterator
from concurrent.futures import Future

from miniqmt_follower.models import Action, DailyPlan, ExecutionResult, TradeSignal

_SIGNAL_CODE_RE = re.compile(r"[^0-9A-Za-z]")


class PlanExecutor:
    """把日计划展开为派生信号；broker 用于"已有持仓不补仓"过滤。"""

    def __init__(self, store, broker):
        self.store = store
        self.broker = broker

    def record(self, plan: DailyPlan) -> bool:
        """记录 plan 已收到（审计用，非硬去重）。"""
        return self.store.try_accept_plan(plan)

    def derived_signals(self, plan: DailyPlan) -> Iterator[TradeSignal]:
        """展开派生信号: 清仓清单 → sell_all；待买清单过滤已有持仓 → auto_buy。"""
        for code in plan.codes_to_sell:
            yield _derived_signal(plan, code, Action.SELL, "sell_all", None)
        buy_codes = tuple(
            code for code in plan.codes_to_buy
            if self.broker.query_position(code) <= 0
        )
        for code in buy_codes:
            yield _derived_signal(plan, code, Action.BUY, "auto_buy", len(buy_codes))


def _derived_signal(
    plan: DailyPlan,
    code: str,
    action: Action,
    quantity_mode: str,
    budget_group_size: int | None,
) -> TradeSignal:
    safe = _SIGNAL_CODE_RE.sub("", str(code))
    return TradeSignal(
        signal_id=f"{plan.signal_id}-{action.value}-{safe}",
        strategy_id=plan.strategy_id,
        action=action,
        code=str(code),
        amount=0,
        reference_price=0.0,
        created_at=plan.created_at,
        mode=plan.mode,
        sent_at_ms=plan.sent_at_ms,
        quantity_mode=quantity_mode,
        budget_group_size=budget_group_size,
    )


def submit_plan_tasks(
    plan: DailyPlan,
    plan_executor: PlanExecutor,
    pools: dict[Action, "ThreadPoolExecutor"],
    engine,
    execute_safe,
    opening_barrier,
) -> Future[list[ExecutionResult]] | None:
    """把 plan 展开为派生信号并投递，返回组合 Future（全部完成后才 set_result）。

    返回 None 表示没有可执行的派生信号（调用方应直接 ACK）。
    """
    derived = list(plan_executor.derived_signals(plan))
    if not derived:
        return None

    children: list[Future[ExecutionResult]] = []
    for sig in derived:
        opening_barrier.register(sig)
        pool = pools[sig.action]
        children.append(pool.submit(execute_safe, engine, sig, opening_barrier))

    combined: Future[list[ExecutionResult]] = Future()

    # 最后两条派生信号并发终态时，两个回调会同时看到"全部完成"并各自
    # set_result，第二次抛 InvalidStateError（只会被 futures 记一行日志）。
    # 用一把锁 + 一次性标志定胜负，谁先到谁负责收口。
    combine_lock = threading.Lock()
    combined_done = False

    def _combine(_future: Future[ExecutionResult]) -> None:
        nonlocal combined_done
        if not all(child.done() for child in children):
            return
        with combine_lock:
            if combined_done:
                return
            combined_done = True
        combined.set_result([child.result() for child in children])

    for child in children:
        child.add_done_callback(_combine)
    return combined

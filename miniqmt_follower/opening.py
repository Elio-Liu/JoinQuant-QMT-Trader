"""开盘卖出闸门：识别盘前卖出信号并阻塞买单直到卖出释放。

盘前卖出窗口内的 SELL 信号登记到闸门，BUY 工作线程等待闸门清空后才继续下单，
实现"先卖后买"的开盘顺序；跌停排队单可提前释放对应信号以解锁买单。

本模块约定:
- register/release 按 signal_id 计数，同一逻辑信号可能登记多个重复工作项；
- 计数归零才 notify_all，一次性唤醒所有等待的买单线程。
"""

from __future__ import annotations

import datetime as dt
import threading

from miniqmt_follower.config import MachineScheduleConfig
from miniqmt_follower.models import Action, TradeSignal


def seconds_until_market_open(machine_schedule: MachineScheduleConfig) -> float:
    """盘前卖出窗口内返回距连续竞价秒数，防止买单提前触达券商。"""
    now = dt.datetime.now()
    market_session = machine_schedule.market_session
    if (
        market_session.preopen_sell_start_at
        <= now.time()
        < market_session.continuous_trading_start_at
    ):
        open_dt = now.replace(
            hour=market_session.continuous_trading_start_at.hour,
            minute=market_session.continuous_trading_start_at.minute,
            second=market_session.continuous_trading_start_at.second,
            microsecond=0,
        )
        return (open_dt - now).total_seconds()
    return 0.0


def is_preopen_sell(
    signal: TradeSignal, machine_schedule: MachineScheduleConfig
) -> bool:
    """判断信号是否为盘前卖出窗口内产生的卖出信号（按 created_at 时间判定）。"""
    if signal.action != Action.SELL:
        return False
    try:
        created = dt.datetime.strptime(signal.created_at, "%Y-%m-%d %H:%M:%S")
    except (TypeError, ValueError):
        return False
    market_session = machine_schedule.market_session
    return (
        market_session.preopen_sell_start_at
        <= created.time()
        < market_session.continuous_trading_start_at
    )


class OpeningSellBarrier:
    """线程安全的开盘卖出闸门：登记盘前卖出并阻塞买单直到全部卖出释放。"""

    def __init__(self, machine_schedule: MachineScheduleConfig) -> None:
        self._machine_schedule = machine_schedule
        self._condition = threading.Condition()
        self._pending_signal_counts: dict[str, int] = {}

    def register(
        self, signal: TradeSignal, *, opening_sequence: bool = False
    ) -> bool:
        """把盘前卖出信号（或显式 opening_sequence 的信号）登记入闸，返回是否入闸。"""
        if not opening_sequence and not is_preopen_sell(
            signal, self._machine_schedule
        ):
            return False
        with self._condition:
            self._pending_signal_counts[signal.signal_id] = (
                self._pending_signal_counts.get(signal.signal_id, 0) + 1
            )
        return True

    def release(self, signal_id: str) -> None:
        """释放一个 signal_id 的工作项；计数归零时唤醒所有等待的买单线程。"""
        with self._condition:
            pending_count = self._pending_signal_counts.get(signal_id, 0)
            if pending_count <= 1:
                self._pending_signal_counts.pop(signal_id, None)
            else:
                self._pending_signal_counts[signal_id] = pending_count - 1
            if not self._pending_signal_counts:
                self._condition.notify_all()

    def release_all(self, signal_id: str) -> None:
        """跌停委托已确认排队时，释放同一逻辑信号的所有重复工作项。"""
        with self._condition:
            self._pending_signal_counts.pop(signal_id, None)
            if not self._pending_signal_counts:
                self._condition.notify_all()

    def wait_until_released(self) -> None:
        """阻塞直到所有登记信号释放完毕（跌停排队可经 release_all 提前解锁）。"""
        with self._condition:
            while self._pending_signal_counts:
                self._condition.wait()

    def pending_count(self) -> int:
        """返回当前仍在闸门内等待的独立 signal_id 数量。"""
        with self._condition:
            return len(self._pending_signal_counts)

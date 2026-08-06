from __future__ import annotations

import datetime as dt
import threading

from miniqmt_follower.models import Action, TradeSignal

_PREOPEN_SELL_START = dt.time(9, 25)
_MARKET_OPEN = dt.time(9, 30)


def seconds_until_market_open() -> float:
    """09:25~09:30 返回距开盘秒数，防止任何盘前买单提前触达券商。"""
    now = dt.datetime.now()
    if _PREOPEN_SELL_START <= now.time() < _MARKET_OPEN:
        open_dt = now.replace(
            hour=_MARKET_OPEN.hour,
            minute=_MARKET_OPEN.minute,
            second=0,
            microsecond=0,
        )
        return (open_dt - now).total_seconds()
    return 0.0


def is_preopen_sell(signal: TradeSignal) -> bool:
    if signal.action != Action.SELL:
        return False
    try:
        created = dt.datetime.strptime(signal.created_at, "%Y-%m-%d %H:%M:%S")
    except (TypeError, ValueError):
        return False
    return _PREOPEN_SELL_START <= created.time() < _MARKET_OPEN


class OpeningSellBarrier:
    def __init__(self) -> None:
        self._condition = threading.Condition()
        self._pending_signal_counts: dict[str, int] = {}

    def register(self, signal: TradeSignal) -> bool:
        if not is_preopen_sell(signal):
            return False
        with self._condition:
            self._pending_signal_counts[signal.signal_id] = (
                self._pending_signal_counts.get(signal.signal_id, 0) + 1
            )
        return True

    def release(self, signal_id: str) -> None:
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
        with self._condition:
            while self._pending_signal_counts:
                self._condition.wait()

    def pending_count(self) -> int:
        with self._condition:
            return len(self._pending_signal_counts)

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from enum import StrEnum


class Action(StrEnum):
    """聚宽侧传来的交易方向。"""

    BUY = "buy"
    SELL = "sell"


class BrokerOrderStatus(StrEnum):
    """券商/miniQMT 订单状态的内部统一表达。"""

    OPEN = "open"
    PARTIALLY_FILLED = "partially_filled"
    FILLED = "filled"
    CANCELED = "canceled"
    REJECTED = "rejected"


class ExecutionStatus(StrEnum):
    """执行引擎落库的信号级状态, 用于幂等、恢复和盘后审计。"""

    RECEIVED = "received"
    ACCEPTED = "accepted"
    DUPLICATE_IGNORED = "duplicate_ignored"
    ORDER_SUBMITTED = "order_submitted"
    FILLED = "filled"
    EXPIRED = "expired"
    FAILED_TIMEOUT = "failed_timeout"
    PARTIALLY_FILLED_TIMEOUT = "partially_filled_timeout"
    FAILED_RISK = "failed_risk"
    FAILED_BROKER = "failed_broker"


_CONSOLE_EVENT_EMOJIS = {
    "重复": "⏭️",
    "停止": "🛑",
    "风控": "⚠️",
    "竞价": "⏳",
    "超时": "⏰",
    "过期": "⌛",
    "失败": "❌",
    "成交": "✅",
    "重试": "🔁",
}


@dataclass(frozen=True)
class TradeSignal:
    """一条来自 Redis Stream 的订单信号。

    reference_price 是策略参考价, 真实委托价由执行端根据最新行情和滑点配置计算。
    sent_at_ms 是聚宽侧发送时刻的毫秒时间戳, 仅用于延迟测量, 缺省时相关日志跳过。
    """

    signal_id: str
    strategy_id: str
    action: Action
    code: str
    amount: int
    reference_price: float
    created_at: str
    mode: str = "live"
    sent_at_ms: int | None = None
    execute_at: str | None = None
    expire_at: str | None = None

    @property
    def label(self) -> str:
        """日志用简短标识: 代码+方向+数量, 比完整 signal_id 更易扫读。

        完整 signal_id 仍以 DEBUG 级别记录（只进文件不上控制台), 需要精确核对
        SQLite 记录或排查幂等去重时可以从文件日志里找。
        """
        return f"{self.code} {self.action.value} {self.amount}股"

    @property
    def action_label(self) -> str:
        """终端展示用买卖方向。"""
        return "买单" if self.action == Action.BUY else "卖单"

    @property
    def task_id(self) -> str:
        """完整 signal_id 的稳定四位短标识，便于终端和文件日志串联。"""
        return hashlib.blake2s(self.signal_id.encode("utf-8"), digest_size=2).hexdigest().upper()

    @property
    def console_prefix(self) -> str:
        return f"【{self.action_label}】📥 信号#{self.task_id}"

    def console_event(self, event: str) -> str:
        emoji = _CONSOLE_EVENT_EMOJIS.get(event, "ℹ️")
        return f"【{self.action_label}】{emoji} 信号#{self.task_id}"

    @classmethod
    def from_dict(cls, raw: dict) -> "TradeSignal":
        # 兼容旧字段 price, 便于从早期 publish 版本平滑迁移。
        reference_price = raw.get("reference_price", raw.get("price"))
        if reference_price is None:
            raise ValueError("signal requires reference_price or price")

        sent_at_ms = raw.get("sent_at_ms")
        execute_at = raw.get("execute_at")
        expire_at = raw.get("expire_at")
        return cls(
            signal_id=str(raw["signal_id"]),
            strategy_id=str(raw["strategy_id"]),
            action=Action(str(raw["action"]).lower()),
            code=str(raw["code"]),
            amount=int(raw["amount"]),
            reference_price=float(reference_price),
            created_at=str(raw.get("created_at") or raw.get("timestamp") or ""),
            mode=str(raw.get("mode", "live")),
            sent_at_ms=int(sent_at_ms) if sent_at_ms is not None else None,
            execute_at=str(execute_at) if execute_at else None,
            expire_at=str(expire_at) if expire_at else None,
        )


@dataclass(frozen=True)
class Quote:
    """一次行情快照: 最新成交价 + 买一/卖一。

    ask1/bid1 为 None 表示该侧盘口不可得(涨跌停单边无档、行情源未提供等),
    定价时应回退到 last_price 滑点模式。
    """

    last_price: float
    ask1: float | None = None
    bid1: float | None = None


@dataclass(frozen=True)
class ExecutionConfig:
    """执行参数, 全部来自配置文件, 方便盘中调参后重启生效。

    pricing_mode:
    - "slippage": 最新成交价 ± 固定百分比滑点(原有行为)。
    - "book": 盘口价定价, 买入=卖一价+book_tick_offset个tick, 卖出=买一价-offset,
      追求首次挂单即成交; 对应盘口缺失时自动回退 slippage 模式。
    """

    buy_slippage_pct: float = 0.003
    sell_slippage_pct: float = 0.003
    order_timeout_sec: float = 3.0
    max_attempts: int = 3
    max_total_duration_sec: float = 15.0
    max_deviation_from_signal_price_pct: float = 0.02
    poll_interval_sec: float = 0.2
    pricing_mode: str = "slippage"
    book_tick_offset: int = 2


@dataclass(frozen=True)
class OrderSnapshot:
    """某个券商订单在一次查询时的快照。filled_qty 是该订单自身的累计成交量。"""

    order_id: str
    status: BrokerOrderStatus
    filled_qty: int = 0


@dataclass(frozen=True)
class ExecutionResult:
    """一条信号最终执行结果, 用于日志、测试和未来执行回报。"""

    signal_id: str
    status: ExecutionStatus
    requested_qty: int
    filled_qty: int
    attempts: int
    message: str = ""


@dataclass(frozen=True)
class StoredSignal:
    """SQLite 中已接收信号的最小读取模型。"""

    signal_id: str
    status: ExecutionStatus
    filled_qty: int

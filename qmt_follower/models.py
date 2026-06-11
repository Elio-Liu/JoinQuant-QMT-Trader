from __future__ import annotations

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
    FAILED_TIMEOUT = "failed_timeout"
    PARTIALLY_FILLED_TIMEOUT = "partially_filled_timeout"
    FAILED_RISK = "failed_risk"
    FAILED_BROKER = "failed_broker"


@dataclass(frozen=True)
class TradeSignal:
    """一条来自 Redis Stream 的订单信号。

    reference_price 是策略参考价, 真实委托价由执行端根据最新行情和滑点配置计算。
    """

    signal_id: str
    strategy_id: str
    action: Action
    code: str
    amount: int
    reference_price: float
    created_at: str
    mode: str = "live"

    @classmethod
    def from_dict(cls, raw: dict) -> "TradeSignal":
        # 兼容旧字段 price, 便于从早期 publish 版本平滑迁移。
        reference_price = raw.get("reference_price", raw.get("price"))
        if reference_price is None:
            raise ValueError("signal requires reference_price or price")

        return cls(
            signal_id=str(raw["signal_id"]),
            strategy_id=str(raw["strategy_id"]),
            action=Action(str(raw["action"]).lower()),
            code=str(raw["code"]),
            amount=int(raw["amount"]),
            reference_price=float(reference_price),
            created_at=str(raw.get("created_at") or raw.get("timestamp") or ""),
            mode=str(raw.get("mode", "live")),
        )


@dataclass(frozen=True)
class ExecutionConfig:
    """执行参数, 全部来自配置文件, 方便盘中调参后重启生效。"""

    buy_slippage_pct: float = 0.003
    sell_slippage_pct: float = 0.003
    order_timeout_sec: float = 3.0
    max_attempts: int = 3
    max_total_duration_sec: float = 15.0
    max_deviation_from_signal_price_pct: float = 0.02
    poll_interval_sec: float = 0.2


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

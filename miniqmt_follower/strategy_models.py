"""定义本地策略引擎的数据模型与候选计划解析。

包含候选计划、策略日、策略事件与行情/持仓/账户快照等不可变数据类，
并集中校验候选计划的字段合法性与代码格式，是策略引擎与 SQLite 账本之间的数据结构层。

本模块约定:
- 候选计划只承载选股结果，阈值、仓位与订单数量由交易机本地 YAML 决定。
- 候选计划字段白名单严格校验，策略端误带旧交易决策会被直接拒绝。
"""

from __future__ import annotations

import datetime as dt
import re
from dataclasses import dataclass
from enum import StrEnum
from typing import Any


# 候选计划允许携带的字段白名单，策略端误带旧交易决策字段会在解析时被拒绝。
_CANDIDATE_FIELDS = frozenset(
    {
        "action",
        "schema_version",
        "plan_id",
        "strategy_id",
        "mode",
        "trading_date",
        "candidates",
        "strategy_version",
        "created_at",
        "sent_at_ms",
    }
)
# 聚宽证券代码格式（六位数字 + 交易所后缀），候选代码必须完全匹配。
_JQ_CODE_PATTERN = re.compile(r"^\d{6}\.(?:XSHG|XSHE)$")


@dataclass(frozen=True)
class CandidatePlan:
    """聚宽只发送选股结果的日候选计划。

    阈值、仓位、时间和订单数量均不属于消息协议，全部由交易机本地私有 YAML
    决定。严格字段集合可防止策略端误把旧交易决策混入候选消息。
    """

    plan_id: str
    strategy_id: str
    trading_date: dt.date
    candidates: tuple[str, ...]
    schema_version: int
    strategy_version: str
    created_at: str
    mode: str
    sent_at_ms: int

    @classmethod
    def from_dict(cls, raw: dict) -> "CandidatePlan":
        """严格解析候选计划消息，字段缺失、非法或格式错误均抛出 ValueError。"""
        if not isinstance(raw, dict):
            raise ValueError("candidate_plan 必须是 JSON 对象")
        unknown = sorted(set(raw) - _CANDIDATE_FIELDS)
        if unknown:
            raise ValueError(f"candidate_plan 包含未知字段: {', '.join(unknown)}")
        missing = sorted(_CANDIDATE_FIELDS - set(raw))
        if missing:
            raise ValueError(f"candidate_plan 缺少字段: {', '.join(missing)}")
        if raw["action"] != "candidate_plan":
            raise ValueError("action 必须是 candidate_plan")
        schema_version = raw["schema_version"]
        if isinstance(schema_version, bool) or schema_version != 1:
            raise ValueError(f"schema_version 只支持 1: {schema_version!r}")
        plan_id = _nonempty_string(raw["plan_id"], "plan_id")
        strategy_id = _nonempty_string(raw["strategy_id"], "strategy_id")
        if raw["mode"] != "live":
            raise ValueError(f"mode 必须是 live: {raw['mode']!r}")
        try:
            trading_date = dt.datetime.strptime(
                str(raw["trading_date"]), "%Y-%m-%d"
            ).date()
        except ValueError as exc:
            raise ValueError(
                f"trading_date 格式非法，需要 YYYY-MM-DD: {raw['trading_date']!r}"
            ) from exc
        raw_candidates = raw["candidates"]
        if not isinstance(raw_candidates, list):
            raise ValueError("candidates 必须是 JSON 列表")
        candidates: list[str] = []
        seen: set[str] = set()
        for code in raw_candidates:
            if not isinstance(code, str) or not _JQ_CODE_PATTERN.fullmatch(code):
                raise ValueError(f"candidates 包含非法聚宽证券代码: {code!r}")
            if code in seen:
                raise ValueError(f"candidates 包含重复代码: {code}")
            seen.add(code)
            candidates.append(code)
        strategy_version = raw["strategy_version"]
        if not isinstance(strategy_version, str):
            raise ValueError("strategy_version 必须是字符串")
        created_at = raw["created_at"]
        if not isinstance(created_at, str):
            raise ValueError("created_at 必须是 YYYY-MM-DD HH:MM:SS 字符串")
        try:
            created_dt = dt.datetime.strptime(created_at, "%Y-%m-%d %H:%M:%S")
        except ValueError as exc:
            raise ValueError(
                f"created_at 格式非法，需要 YYYY-MM-DD HH:MM:SS: {created_at!r}"
            ) from exc
        if created_dt.date() != trading_date:
            raise ValueError("created_at 日期必须与 trading_date 一致")
        sent_at_ms = raw["sent_at_ms"]
        if isinstance(sent_at_ms, bool) or not isinstance(sent_at_ms, int) or sent_at_ms <= 0:
            raise ValueError(f"sent_at_ms 必须是正整数毫秒时间戳: {sent_at_ms!r}")
        return cls(
            plan_id=plan_id,
            strategy_id=strategy_id,
            trading_date=trading_date,
            candidates=tuple(candidates),
            schema_version=schema_version,
            strategy_version=strategy_version,
            created_at=created_at,
            mode="live",
            sent_at_ms=sent_at_ms,
        )


class CandidatePlanStatus(StrEnum):
    """候选计划的受理状态。"""

    READY = "READY"
    EMPTY = "EMPTY"
    LATE = "LATE"
    REJECTED = "REJECTED"
    CONFLICT = "CONFLICT"


class StrategyDayStatus(StrEnum):
    """单策略单交易日的生命周期状态。"""

    ACTIVE = "ACTIVE"
    NO_PLAN = "NO_PLAN"
    PLAN_CONFLICT = "PLAN_CONFLICT"
    HALTED = "HALTED"
    CLOSED = "CLOSED"


class StrategyEventStatus(StrEnum):
    """策略事件在本地执行流水线中的处理状态。"""

    TRIGGERED = "TRIGGERED"
    SUBMITTED = "SUBMITTED"
    TERMINAL = "TERMINAL"
    SKIPPED_NO_POSITION = "SKIPPED_NO_POSITION"
    SKIPPED_T1 = "SKIPPED_T1"
    SKIPPED_LIMIT_UP = "SKIPPED_LIMIT_UP"
    SUPERSEDED = "SUPERSEDED"
    BLOCKED_DATA = "BLOCKED_DATA"
    BLOCKED_ACCOUNT_HALT = "BLOCKED_ACCOUNT_HALT"
    MISSED_DEADLINE = "MISSED_DEADLINE"


@dataclass(frozen=True)
class StoredCandidatePlan:
    """已落库的候选计划及其受理状态。"""

    plan: CandidatePlan
    status: CandidatePlanStatus


@dataclass(frozen=True)
class StrategyDay:
    """单个策略在单个交易日内的生命周期记录。"""

    strategy_id: str
    trading_date: dt.date
    status: StrategyDayStatus
    plan_id: str | None = None
    halt_reason: str = ""


@dataclass(frozen=True)
class StrategyEvent:
    """策略引擎产生的单条规则决策事件及其执行状态。"""

    strategy_id: str
    trading_date: dt.date
    rule_name: str
    code: str
    decision: str
    status: StrategyEventStatus
    signal_id: str | None = None
    reason: str = ""
    market_snapshot: dict[str, Any] | None = None
    position_snapshot: dict[str, Any] | None = None


class StrategyAction(StrEnum):
    """策略决策支持的卖出动作类型。"""

    SELL_ALL = "sell_all"
    SELL_HALF = "sell_half"
    BLOCK = "block"


@dataclass(frozen=True)
class MarketSnapshot:
    """某只证券在决策时刻的行情快照。"""

    code: str
    last_price: float
    open_price: float | None
    previous_close: float | None
    ask1: float | None
    bid1: float | None
    high_limit: float | None
    low_limit: float | None
    quote_time: dt.datetime | None
    trading_date: dt.date | None
    # 当日最高价(QMT tick high 字段), 回落止盈规则用它播种/更新买入以来最高;
    # None 表示行情源未提供, 依赖该字段的规则封闭失败。
    day_high: float | None = None


@dataclass(frozen=True)
class PositionSnapshot:
    """某只证券的持仓快照。"""

    code: str
    total_qty: int
    available_qty: int
    cost_price: float
    market_value: float


@dataclass(frozen=True)
class AccountSnapshot:
    """账户资金与持仓的汇总快照。"""

    available_cash: float
    total_assets: float
    market_value: float
    positions: tuple[PositionSnapshot, ...]
    # 冻结资金: 已报未成委托占用的资金。幽灵单三重校验用它排除"订单其实在途
    # 只是回报丢失" —— 买单在途必然冻结资金, 冻结为 0 才是幽灵单的证据之一。
    frozen_cash: float = 0.0

    def position_of(self, code: str) -> PositionSnapshot | None:
        """按证券代码查找持仓快照，未持有该证券时返回 None。"""
        return next((position for position in self.positions if position.code == code), None)


@dataclass(frozen=True)
class StrategyDecision:
    """策略引擎对单条事件作出的卖出/封板决策。"""

    action: StrategyAction
    reason: str
    sell_ratio: float | None = None


def _nonempty_string(raw: object, field: str) -> str:
    """校验非空字符串并返回去除首尾空白后的值。"""
    if not isinstance(raw, str) or not raw.strip():
        raise ValueError(f"{field} 必须是非空字符串")
    return raw.strip()

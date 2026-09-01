"""本地策略引擎的配置模型与加载校验：把 ``config.strategy.yaml`` 解析为强类型配置。

本模块定义 StrategyEngineConfig 及其嵌套子配置的 dataclass 模型，提供严格字段
校验（未知字段/缺失字段/取值范围/日程先后），并在加载后交叉校验与运行时配置、
worker 数量的一致性，任何冲突都在连接 Redis/QMT 前阻止启动。

本模块约定:
- 字段校验严格失败即抛 ValueError，不做静默默认或自动迁移；
- 旧日程字段（如 execute_at）一经发现直接拒绝启动，不兼容读取。
"""

from __future__ import annotations

import datetime as dt
import math
import re
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any

import yaml

from miniqmt_follower.config import (
    RuntimeConfig,
    _validated_limit_down_sell_mode,
    _validated_limit_up_buy_mode,
    _validated_max_single_position_pct,
    _validated_sell_half_insufficient_lot_mode,
    load_config,
)

# ---------------------------------------------------------------------------
# 策略层执行开关: 已从主配置(交易机)迁移到 config.strategy.yaml 的 execution 节点
# ---------------------------------------------------------------------------
# 这些键描述"策略想怎么交易"(涨跌停处理模式、半仓不足一手语义、单票集中度上限),
# 属于策略层决策; 交易机主配置只保留机器/券商/执行机制类参数。主配置残留这些键
# 时拒绝启动, 避免两层各写一份、互相覆盖的静默错配。旧开关 skip_sell_when_limit_down /
# skip_buy_when_limit_up 一并禁止(由新模式名取代)。
_MIGRATED_EXECUTION_KEYS = frozenset(
    {
        "limit_down_sell_mode",
        "limit_up_buy_mode",
        "max_single_position_pct",
        "sell_half_insufficient_lot_mode",
        "skip_sell_when_limit_down",
        "skip_buy_when_limit_up",
        # 四个报价幅度参数已合并为 execution.quote_band_pct (统一 ±1.5% 口径)。
        "buy_slippage_pct",
        "sell_slippage_pct",
        "auction_aggressive_pct",
        "opening_aggressive_pct",
    }
)
# config.strategy.yaml 的 execution 节点: 四个键严格必填、禁止未知键,
# 取值校验与主配置同源(直接复用 config.py 的校验函数), 不提供任何代码默认值。
_EXECUTION_OVERLAY_FIELDS = frozenset(
    {
        "limit_down_sell_mode",
        "limit_up_buy_mode",
        "max_single_position_pct",
        "sell_half_insufficient_lot_mode",
    }
)


# ---------------------------------------------------------------------------
# 配置模型
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class CandidatePlanConfig:
    """候选计划接收约束：开关、候选数上限、最大时延与是否随收订阅。"""
    enabled: bool
    max_candidates: int
    max_age_sec: int
    subscribe_on_receive: bool


@dataclass(frozen=True)
class CandidatePlanScheduleConfig:
    """候选计划接收截止时间。"""
    accept_until: dt.time


@dataclass(frozen=True)
class WindowScheduleConfig:
    """时间窗口规则的触发与重试截止时刻。"""
    trigger_at: dt.time
    retry_until: dt.time


@dataclass(frozen=True)
class OpeningBuyScheduleConfig:
    """开盘买入窗口: 盘前挂单开始、开盘与接纳截止时刻。"""
    preopen_start_at: dt.time
    start_at: dt.time
    admit_until: dt.time


@dataclass(frozen=True)
class HardStopScheduleConfig:
    """硬止损监控窗口与检查间隔。"""
    start_at: dt.time
    last_check_at: dt.time
    interval_sec: float


@dataclass(frozen=True)
class StrategyScheduleConfig:
    """本地策略各规则日程的汇总。"""
    candidate_plan: CandidatePlanScheduleConfig
    opening_exit: WindowScheduleConfig
    opening_buy: OpeningBuyScheduleConfig
    hard_stop: HardStopScheduleConfig
    morning_exit: WindowScheduleConfig
    afternoon_exit: WindowScheduleConfig


@dataclass(frozen=True)
class CapitalAllocationConfig:
    """开盘买入资金分配：等额预算 + 总仓位/现金约束。

    total_position_limit_pct / cash_reserve_pct 为 0 时关闭对应约束
    (total=总仓位上限, cash_reserve=现金预留)。单票仓位上限已合并到
    config.strategy.yaml 的 execution.max_single_position_pct 单一键,
    由 LocalStrategyEngine 在计算预算时注入, 此处不再单独配置。
    """
    enabled: bool
    method: str
    total_position_limit_pct: float
    cash_reserve_pct: float
    skip_existing_positions: bool
    redistribute_failed_budget: bool


@dataclass(frozen=True)
class OpeningBuyConfig:
    """开盘买入开关、是否等待开盘卖出、回款补仓与资金分配。"""
    enabled: bool
    wait_for_opening_sells: bool
    topup_enabled: bool
    capital_allocation: CapitalAllocationConfig


@dataclass(frozen=True)
class LowOpenExitConfig:
    """低开卖出阈值。"""
    enabled: bool
    threshold_pct: float


@dataclass(frozen=True)
class LimitDownExitConfig:
    """跌停卖出开关与排队模式要求。"""
    enabled: bool
    require_queue_mode: bool


@dataclass(frozen=True)
class OpeningExitConfig:
    """开盘卖出（低开/跌停）开关。"""
    enabled: bool
    low_open_exit: LowOpenExitConfig
    limit_down_exit: LimitDownExitConfig


@dataclass(frozen=True)
class IntradayHardStopConfig:
    """盘中固定止损开关与亏损阈值。"""
    enabled: bool
    loss_pct: float
    skip_if_limit_up: bool


@dataclass(frozen=True)
class TrailingTakeProfitConfig:
    """回落止盈: 现价相对买入以来最高价回落达到阈值即全清。"""
    enabled: bool
    pullback_pct: float


@dataclass(frozen=True)
class ProfitReduceConfig:
    """盈利减仓（卖半仓）开关与卖出比例。"""
    enabled: bool
    sell_ratio: float
    insufficient_lot_mode: str


@dataclass(frozen=True)
class MorningExitConfig:
    """上午检查卖出（亏损清仓/盈利减半）开关。"""
    enabled: bool
    require_not_limit_up: bool
    loss_exit: bool
    profit_reduce: ProfitReduceConfig


@dataclass(frozen=True)
class AfternoonExitConfig:
    """下午检查卖出开关与涨停持有策略。"""
    enabled: bool
    sell_if_not_limit_up: bool
    hold_if_limit_up: bool


@dataclass(frozen=True)
class LimitDetectionConfig:
    """涨跌停判定容差与涨停价必需性。"""
    tolerance_pct: float
    require_limit_prices: bool


@dataclass(frozen=True)
class DataSafetyConfig:
    """行情数据安全校验约束。"""
    max_tick_age_sec: float
    require_current_trade_date: bool
    require_open_and_previous_close: bool
    invalid_data_action: str


@dataclass(frozen=True)
class ExternalSignalsConfig:
    """聚宽侧卖出信号的放行白名单(本地策略引擎专用账户模式)。

    只有 quantity_mode=sell_all 且 purpose 在该名单内的聚宽消息会被执行,
    其余同策略旧交易消息一律 ACK 拒绝, 防止聚宽与交易端双重管理真实仓位。
    名单内容属于策略契约的一部分, 由 config.strategy.yaml 显式提供,
    代码不内置任何默认值; 空列表表示拒绝一切外部卖出信号。
    """
    allowed_sell_purposes: tuple[str, ...]


@dataclass(frozen=True)
class StrategyEngineConfig:
    """本地策略引擎配置根。"""
    enabled: bool
    strategy_id: str
    external_signals: ExternalSignalsConfig
    schedule: StrategyScheduleConfig
    candidate_plan: CandidatePlanConfig
    opening_buy: OpeningBuyConfig
    opening_exit: OpeningExitConfig
    intraday_hard_stop: IntradayHardStopConfig
    trailing_take_profit: TrailingTakeProfitConfig
    morning_exit: MorningExitConfig
    afternoon_exit: AfternoonExitConfig
    limit_detection: LimitDetectionConfig
    data_safety: DataSafetyConfig


_LEGACY_SCHEDULE_FIELDS = (
    ("candidate_plan", "receive_deadline"),
    ("opening_exit", "execute_at"),
    ("opening_exit", "latest_start_at"),
    ("opening_buy", "execute_at"),
    ("opening_buy", "latest_start_at"),
    ("intraday_hard_stop", "monitor_start_at"),
    ("intraday_hard_stop", "monitor_end_at"),
    ("intraday_hard_stop", "check_interval_sec"),
    ("morning_exit", "execute_at"),
    ("morning_exit", "latest_start_at"),
    ("afternoon_exit", "execute_at"),
    ("afternoon_exit", "latest_start_at"),
)


# ---------------------------------------------------------------------------
# 字段校验辅助
# ---------------------------------------------------------------------------


def _mapping(raw: object, field: str, allowed: set[str]) -> dict[str, Any]:
    """校验映射字段恰好为允许集合，未知或缺失字段都抛 ValueError。"""
    if not isinstance(raw, dict):
        raise ValueError(f"{field} 必须是 YAML 映射")
    unknown = sorted(set(raw) - allowed)
    if unknown:
        raise ValueError(f"{field} 包含未知字段: {', '.join(unknown)}")
    missing = sorted(allowed - set(raw))
    if missing:
        raise ValueError(f"{field} 缺少字段: {', '.join(missing)}")
    return raw


def _bool(raw: object, field: str) -> bool:
    if not isinstance(raw, bool):
        raise ValueError(f"{field} 必须写成 YAML 布尔值 true/false: {raw!r}")
    return raw


def _string(raw: object, field: str) -> str:
    if not isinstance(raw, str) or not raw.strip():
        raise ValueError(f"{field} 必须是非空字符串: {raw!r}")
    return raw.strip()


def _positive_int(raw: object, field: str) -> int:
    if isinstance(raw, bool) or not isinstance(raw, int) or raw <= 0:
        raise ValueError(f"{field} 必须是大于0的整数: {raw!r}")
    return raw


def _positive_float(raw: object, field: str) -> float:
    if isinstance(raw, bool) or not isinstance(raw, (int, float)):
        raise ValueError(f"{field} 必须是大于0的数字: {raw!r}")
    value = float(raw)
    if not math.isfinite(value) or value <= 0:
        raise ValueError(f"{field} 必须是有限且大于0的数字: {raw!r}")
    return value


def _pct(
    raw: object,
    field: str,
    *,
    minimum: float,
    maximum: float,
    include_minimum: bool = False,
    include_maximum: bool = True,
) -> float:
    """校验数值位于 [minimum, maximum] 区间，边界是否包含由开关控制。"""
    if isinstance(raw, bool) or not isinstance(raw, (int, float)):
        raise ValueError(f"{field} 必须是数字: {raw!r}")
    value = float(raw)
    lower_ok = value >= minimum if include_minimum else value > minimum
    upper_ok = value <= maximum if include_maximum else value < maximum
    if not (lower_ok and upper_ok):
        left = "[" if include_minimum else "("
        right = "]" if include_maximum else ")"
        raise ValueError(
            f"{field} 必须位于 {left}{minimum},{maximum}{right}: {raw!r}"
        )
    return value


_TIME_PATTERN = re.compile(r"^\d{2}:\d{2}:\d{2}$")


def _time(raw: object, field: str) -> dt.time:
    if not isinstance(raw, str) or not _TIME_PATTERN.fullmatch(raw):
        raise ValueError(f"{field} 格式非法，需要 HH:MM:SS: {raw!r}")
    try:
        return dt.datetime.strptime(raw, "%H:%M:%S").time()
    except ValueError as exc:
        raise ValueError(f"{field} 格式非法，需要 HH:MM:SS: {raw!r}") from exc


# ---------------------------------------------------------------------------
# 加载与校验
# ---------------------------------------------------------------------------


def _load_window_schedule(raw: object, field: str) -> WindowScheduleConfig:
    data = _mapping(raw, field, {"trigger_at", "retry_until"})
    return WindowScheduleConfig(
        trigger_at=_time(data["trigger_at"], f"{field}.trigger_at"),
        retry_until=_time(data["retry_until"], f"{field}.retry_until"),
    )


def _validate_strategy_schedule(schedule: StrategyScheduleConfig) -> None:
    if schedule.candidate_plan.accept_until > schedule.opening_buy.admit_until:
        raise ValueError(
            "strategy_engine.schedule.candidate_plan.accept_until 必须早于或等于 "
            "strategy_engine.schedule.opening_buy.admit_until"
        )
    if schedule.opening_exit.trigger_at >= schedule.opening_exit.retry_until:
        raise ValueError(
            "strategy_engine.schedule.opening_exit.trigger_at 必须早于 "
            "strategy_engine.schedule.opening_exit.retry_until"
        )
    if schedule.opening_buy.start_at >= schedule.opening_buy.admit_until:
        raise ValueError(
            "strategy_engine.schedule.opening_buy.start_at 必须早于 "
            "strategy_engine.schedule.opening_buy.admit_until"
        )
    if schedule.hard_stop.start_at > schedule.hard_stop.last_check_at:
        raise ValueError(
            "strategy_engine.schedule.hard_stop.start_at 必须早于或等于 "
            "strategy_engine.schedule.hard_stop.last_check_at"
        )
    if schedule.morning_exit.trigger_at >= schedule.morning_exit.retry_until:
        raise ValueError(
            "strategy_engine.schedule.morning_exit.trigger_at 必须早于 "
            "strategy_engine.schedule.morning_exit.retry_until"
        )
    if schedule.afternoon_exit.trigger_at >= schedule.afternoon_exit.retry_until:
        raise ValueError(
            "strategy_engine.schedule.afternoon_exit.trigger_at 必须早于 "
            "strategy_engine.schedule.afternoon_exit.retry_until"
        )


def _load_schedule(raw: object) -> StrategyScheduleConfig:
    field = "strategy_engine.schedule"
    data = _mapping(
        raw,
        field,
        {
            "candidate_plan",
            "opening_exit",
            "opening_buy",
            "hard_stop",
            "morning_exit",
            "afternoon_exit",
        },
    )
    candidate_field = f"{field}.candidate_plan"
    candidate = _mapping(data["candidate_plan"], candidate_field, {"accept_until"})
    opening_buy_field = f"{field}.opening_buy"
    opening_buy = _mapping(
        data["opening_buy"],
        opening_buy_field,
        {"preopen_start_at", "start_at", "admit_until"},
    )
    hard_stop_field = f"{field}.hard_stop"
    hard_stop = _mapping(
        data["hard_stop"],
        hard_stop_field,
        {"start_at", "last_check_at", "interval_sec"},
    )
    preopen_start_at = _time(
        opening_buy["preopen_start_at"], f"{opening_buy_field}.preopen_start_at"
    )
    opening_buy_start_at = _time(
        opening_buy["start_at"], f"{opening_buy_field}.start_at"
    )
    if preopen_start_at > opening_buy_start_at:
        raise ValueError(
            f"{opening_buy_field}.preopen_start_at 必须不晚于 "
            f"{opening_buy_field}.start_at"
        )
    schedule = StrategyScheduleConfig(
        candidate_plan=CandidatePlanScheduleConfig(
            accept_until=_time(
                candidate["accept_until"], f"{candidate_field}.accept_until"
            )
        ),
        opening_exit=_load_window_schedule(
            data["opening_exit"], f"{field}.opening_exit"
        ),
        opening_buy=OpeningBuyScheduleConfig(
            preopen_start_at=preopen_start_at,
            start_at=opening_buy_start_at,
            admit_until=_time(
                opening_buy["admit_until"], f"{opening_buy_field}.admit_until"
            ),
        ),
        hard_stop=HardStopScheduleConfig(
            start_at=_time(hard_stop["start_at"], f"{hard_stop_field}.start_at"),
            last_check_at=_time(
                hard_stop["last_check_at"], f"{hard_stop_field}.last_check_at"
            ),
            interval_sec=_positive_float(
                hard_stop["interval_sec"], f"{hard_stop_field}.interval_sec"
            ),
        ),
        morning_exit=_load_window_schedule(
            data["morning_exit"], f"{field}.morning_exit"
        ),
        afternoon_exit=_load_window_schedule(
            data["afternoon_exit"], f"{field}.afternoon_exit"
        ),
    )
    _validate_strategy_schedule(schedule)
    return schedule


def _load_external_signals(raw: object) -> ExternalSignalsConfig:
    """strategy_engine.external_signals: 放行的聚宽卖出 purpose 白名单。

    空列表合法(表示拒绝一切外部卖出信号); 列表项必须是非空字符串且不重复。
    """
    field = "strategy_engine.external_signals"
    data = _mapping(raw, field, {"allowed_sell_purposes"})
    purposes = data["allowed_sell_purposes"]
    if not isinstance(purposes, list):
        raise ValueError(f"{field}.allowed_sell_purposes 必须是 YAML 列表")
    cleaned: list[str] = []
    for index, item in enumerate(purposes):
        if not isinstance(item, str) or not item.strip():
            raise ValueError(
                f"{field}.allowed_sell_purposes[{index}] 必须是非空字符串: {item!r}"
            )
        cleaned.append(item.strip())
    if len(cleaned) != len(set(cleaned)):
        raise ValueError(f"{field}.allowed_sell_purposes 包含重复项: {purposes!r}")
    return ExternalSignalsConfig(tuple(cleaned))


def _load_candidate(raw: object) -> CandidatePlanConfig:
    field = "strategy_engine.candidate_plan"
    data = _mapping(
        raw,
        field,
        {"enabled", "max_candidates", "max_age_sec", "subscribe_on_receive"},
    )
    return CandidatePlanConfig(
        enabled=_bool(data["enabled"], f"{field}.enabled"),
        max_candidates=_positive_int(data["max_candidates"], f"{field}.max_candidates"),
        max_age_sec=_positive_int(data["max_age_sec"], f"{field}.max_age_sec"),
        subscribe_on_receive=_bool(
            data["subscribe_on_receive"], f"{field}.subscribe_on_receive"
        ),
    )


def _load_capital_allocation(raw: object) -> CapitalAllocationConfig:
    field = "strategy_engine.opening_buy.capital_allocation"
    data = _mapping(
        raw,
        field,
        {
            "enabled",
            "method",
            "total_position_limit_pct",
            "cash_reserve_pct",
            "skip_existing_positions",
            "redistribute_failed_budget",
        },
    )
    method = _string(data["method"], f"{field}.method")
    if method != "equal":
        raise ValueError(f'{field}.method 只支持 "equal": {method!r}')
    total_pct = _pct(
        data["total_position_limit_pct"],
        f"{field}.total_position_limit_pct",
        minimum=0,
        maximum=1,
        include_minimum=True,
    )
    redistribute = _bool(
        data["redistribute_failed_budget"],
        f"{field}.redistribute_failed_budget",
    )
    if redistribute:
        raise ValueError(
            "strategy_engine.opening_buy.capital_allocation."
            "redistribute_failed_budget 当前只支持 false"
        )
    return CapitalAllocationConfig(
        enabled=_bool(data["enabled"], f"{field}.enabled"),
        method=method,
        total_position_limit_pct=total_pct,
        cash_reserve_pct=_pct(
            data["cash_reserve_pct"],
            f"{field}.cash_reserve_pct",
            minimum=0,
            maximum=0.05,
            include_minimum=True,
            include_maximum=False,
        ),
        skip_existing_positions=_bool(
            data["skip_existing_positions"], f"{field}.skip_existing_positions"
        ),
        redistribute_failed_budget=redistribute,
    )


def _load_opening_buy(raw: object) -> OpeningBuyConfig:
    field = "strategy_engine.opening_buy"
    data = _mapping(
        raw,
        field,
        {"enabled", "wait_for_opening_sells", "topup_enabled", "capital_allocation"},
    )
    return OpeningBuyConfig(
        enabled=_bool(data["enabled"], f"{field}.enabled"),
        wait_for_opening_sells=_bool(
            data["wait_for_opening_sells"], f"{field}.wait_for_opening_sells"
        ),
        topup_enabled=_bool(data["topup_enabled"], f"{field}.topup_enabled"),
        capital_allocation=_load_capital_allocation(data["capital_allocation"]),
    )


def _load_opening_exit(raw: object) -> OpeningExitConfig:
    field = "strategy_engine.opening_exit"
    data = _mapping(
        raw,
        field,
        {"enabled", "low_open_exit", "limit_down_exit"},
    )
    low_field = f"{field}.low_open_exit"
    low = _mapping(data["low_open_exit"], low_field, {"enabled", "threshold_pct"})
    limit_field = f"{field}.limit_down_exit"
    limit_down = _mapping(
        data["limit_down_exit"], limit_field, {"enabled", "require_queue_mode"}
    )
    threshold = _pct(
        low["threshold_pct"],
        f"{low_field}.threshold_pct",
        minimum=-1,
        maximum=0,
        include_minimum=False,
    )
    return OpeningExitConfig(
        enabled=_bool(data["enabled"], f"{field}.enabled"),
        low_open_exit=LowOpenExitConfig(
            enabled=_bool(low["enabled"], f"{low_field}.enabled"),
            threshold_pct=threshold,
        ),
        limit_down_exit=LimitDownExitConfig(
            enabled=_bool(limit_down["enabled"], f"{limit_field}.enabled"),
            require_queue_mode=_bool(
                limit_down["require_queue_mode"], f"{limit_field}.require_queue_mode"
            ),
        ),
    )


def _load_hard_stop(raw: object) -> IntradayHardStopConfig:
    field = "strategy_engine.intraday_hard_stop"
    data = _mapping(
        raw,
        field,
        {"enabled", "loss_pct", "skip_if_limit_up"},
    )
    return IntradayHardStopConfig(
        enabled=_bool(data["enabled"], f"{field}.enabled"),
        loss_pct=_pct(data["loss_pct"], f"{field}.loss_pct", minimum=0, maximum=1),
        skip_if_limit_up=_bool(data["skip_if_limit_up"], f"{field}.skip_if_limit_up"),
    )


def _load_trailing(raw: object) -> TrailingTakeProfitConfig:
    field = "strategy_engine.trailing_take_profit"
    data = _mapping(raw, field, {"enabled", "pullback_pct"})
    return TrailingTakeProfitConfig(
        enabled=_bool(data["enabled"], f"{field}.enabled"),
        pullback_pct=_pct(
            data["pullback_pct"],
            f"{field}.pullback_pct",
            minimum=0.001,
            maximum=1,
        ),
    )


def _load_morning_exit(raw: object) -> MorningExitConfig:
    field = "strategy_engine.morning_exit"
    data = _mapping(
        raw,
        field,
        {"enabled", "require_not_limit_up", "loss_exit", "profit_reduce"},
    )
    profit_field = f"{field}.profit_reduce"
    profit = _mapping(
        data["profit_reduce"],
        profit_field,
        {"enabled", "sell_ratio", "insufficient_lot_mode"},
    )
    insufficient = _string(
        profit["insufficient_lot_mode"], f"{profit_field}.insufficient_lot_mode"
    )
    if insufficient not in {"sell_all", "skip"}:
        raise ValueError(
            f'{profit_field}.insufficient_lot_mode 只支持 "sell_all" 或 "skip"'
        )
    sell_ratio = _pct(
        profit["sell_ratio"], f"{profit_field}.sell_ratio", minimum=0, maximum=1
    )
    if sell_ratio != 0.5:
        raise ValueError(f"{profit_field}.sell_ratio 当前只支持 0.5")
    return MorningExitConfig(
        enabled=_bool(data["enabled"], f"{field}.enabled"),
        require_not_limit_up=_bool(
            data["require_not_limit_up"], f"{field}.require_not_limit_up"
        ),
        loss_exit=_bool(data["loss_exit"], f"{field}.loss_exit"),
        profit_reduce=ProfitReduceConfig(
            enabled=_bool(profit["enabled"], f"{profit_field}.enabled"),
            sell_ratio=sell_ratio,
            insufficient_lot_mode=insufficient,
        ),
    )


def _load_afternoon_exit(raw: object) -> AfternoonExitConfig:
    field = "strategy_engine.afternoon_exit"
    data = _mapping(
        raw,
        field,
        {"enabled", "sell_if_not_limit_up", "hold_if_limit_up"},
    )
    return AfternoonExitConfig(
        enabled=_bool(data["enabled"], f"{field}.enabled"),
        sell_if_not_limit_up=_bool(
            data["sell_if_not_limit_up"], f"{field}.sell_if_not_limit_up"
        ),
        hold_if_limit_up=_bool(data["hold_if_limit_up"], f"{field}.hold_if_limit_up"),
    )


def _load_limit_detection(raw: object) -> LimitDetectionConfig:
    field = "strategy_engine.limit_detection"
    data = _mapping(raw, field, {"tolerance_pct", "require_limit_prices"})
    return LimitDetectionConfig(
        tolerance_pct=_pct(
            data["tolerance_pct"],
            f"{field}.tolerance_pct",
            minimum=0,
            maximum=0.1,
            include_minimum=True,
            include_maximum=False,
        ),
        require_limit_prices=_bool(
            data["require_limit_prices"], f"{field}.require_limit_prices"
        ),
    )


def _load_data_safety(raw: object) -> DataSafetyConfig:
    field = "strategy_engine.data_safety"
    data = _mapping(
        raw,
        field,
        {"max_tick_age_sec", "require_current_trade_date", "require_open_and_previous_close", "invalid_data_action"},
    )
    action = _string(data["invalid_data_action"], f"{field}.invalid_data_action")
    if action != "block_rule":
        raise ValueError(f'{field}.invalid_data_action 只支持 "block_rule"')
    return DataSafetyConfig(
        max_tick_age_sec=_positive_float(
            data["max_tick_age_sec"], f"{field}.max_tick_age_sec"
        ),
        require_current_trade_date=_bool(
            data["require_current_trade_date"], f"{field}.require_current_trade_date"
        ),
        require_open_and_previous_close=_bool(
            data["require_open_and_previous_close"],
            f"{field}.require_open_and_previous_close",
        ),
        invalid_data_action=action,
    )


def _validate_runtime_contract(
    config: StrategyEngineConfig, runtime: RuntimeConfig, workers: int
) -> None:
    """校验策略配置与运行时/workers 的交叉约束，冲突时阻止启动。"""
    if not config.enabled:
        raise ValueError(
            "strategy_engine.enabled 必须启用；本分支不允许静默退回普通跟单模式"
            "（如需纯跟单模式，删除 config.strategy.yaml 后重启）"
        )
    allowlist = runtime.redis.allowed_strategy_ids
    if tuple(allowlist) != (config.strategy_id,):
        raise ValueError(
            "专用账户要求 redis.allowed_strategy_ids 仅包含 strategy_engine.strategy_id"
        )
    schedule = config.schedule
    machine_schedule = runtime.machine_schedule
    if (
        schedule.opening_buy.start_at
        < machine_schedule.market_session.continuous_trading_start_at
    ):
        raise ValueError(
            "strategy_engine.schedule.opening_buy.start_at 必须晚于或等于 "
            "machine_schedule.market_session.continuous_trading_start_at"
        )
    if schedule.opening_exit.retry_until > schedule.opening_buy.admit_until:
        raise ValueError(
            "strategy_engine.schedule.opening_exit.retry_until 必须早于或等于 "
            "strategy_engine.schedule.opening_buy.admit_until"
        )
    if (
        schedule.hard_stop.last_check_at
        > machine_schedule.order_guard.strategy_sell_last_submit_at
    ):
        raise ValueError(
            "strategy_engine.schedule.hard_stop.last_check_at 必须早于或等于 "
            "machine_schedule.order_guard.strategy_sell_last_submit_at"
        )
    if (
        schedule.morning_exit.retry_until
        > machine_schedule.order_guard.strategy_sell_last_submit_at
    ):
        raise ValueError(
            "strategy_engine.schedule.morning_exit.retry_until 必须早于或等于 "
            "machine_schedule.order_guard.strategy_sell_last_submit_at"
        )
    if (
        schedule.afternoon_exit.retry_until
        > machine_schedule.order_guard.strategy_sell_last_submit_at
    ):
        raise ValueError(
            "strategy_engine.schedule.afternoon_exit.retry_until 必须早于或等于 "
            "machine_schedule.order_guard.strategy_sell_last_submit_at"
        )
    if config.opening_buy.enabled and not config.candidate_plan.enabled:
        raise ValueError("opening_buy.enabled=true 时 candidate_plan.enabled 必须为 true")
    if config.opening_buy.enabled and not config.opening_buy.capital_allocation.enabled:
        raise ValueError("opening_buy.enabled=true 时 capital_allocation.enabled 必须为 true")
    allocation = config.opening_buy.capital_allocation
    if (
        allocation.enabled
        and allocation.total_position_limit_pct > 0
        and runtime.execution.max_single_position_pct > allocation.total_position_limit_pct
    ):
        raise ValueError(
            "execution.max_single_position_pct 不能高于 "
            "strategy_engine.opening_buy.capital_allocation.total_position_limit_pct"
        )
    if not config.data_safety.require_current_trade_date:
        raise ValueError("data_safety.require_current_trade_date 当前必须为 true")
    if not config.data_safety.require_open_and_previous_close:
        raise ValueError(
            "data_safety.require_open_and_previous_close 当前必须为 true"
        )
    limit_down = config.opening_exit.limit_down_exit
    if (
        config.opening_exit.enabled
        and limit_down.enabled
        and limit_down.require_queue_mode
        and runtime.execution.effective_limit_down_sell_mode() != "queue"
    ):
        raise ValueError(
            'limit_down_exit.require_queue_mode=true 要求 execution.limit_down_sell_mode="queue"'
        )
    if isinstance(workers, bool) or workers <= 0:
        raise ValueError(f"workers 必须大于0: {workers!r}")
    execution = runtime.execution
    if workers < execution.max_concurrent_queue_sells + 2:
        raise ValueError(
            "workers 必须至少为 execution.max_concurrent_queue_sells + 2"
        )
    if workers < execution.max_concurrent_queue_buys + 2:
        raise ValueError(
            "workers 必须至少为 execution.max_concurrent_queue_buys + 2"
        )


# ---------------------------------------------------------------------------
# 对外入口
# ---------------------------------------------------------------------------


def _reject_migrated_execution_keys(main_config_path: str | Path) -> None:
    """主配置(交易机)禁止再携带已迁移到 config.strategy.yaml 的策略层执行开关。

    这些开关描述"策略想怎么交易"，属于策略层决策；交易机主配置只保留
    机器/券商/执行机制类参数。旧主配置残留这些键时直接拒绝启动，避免两层
    各写一份、互相覆盖的静默错配。
    """
    path = Path(main_config_path).resolve()
    try:
        with path.open("r", encoding="utf-8") as fh:
            raw = yaml.safe_load(fh)
    except yaml.YAMLError as exc:
        raise ValueError(f"主配置解析失败: {exc}") from exc
    if not isinstance(raw, dict):
        return
    execution_raw = raw.get("execution")
    if not isinstance(execution_raw, dict):
        return
    migrated = sorted(
        key for key in execution_raw if key in _MIGRATED_EXECUTION_KEYS
    )
    if migrated:
        fields = ", ".join(f"execution.{key}" for key in migrated)
        raise ValueError(
            f"策略层执行配置已迁移到 config.strategy.yaml, "
            f"主配置禁止再携带: {fields}（已迁移）"
        )


def _overlay_strategy_execution(runtime: RuntimeConfig, raw: dict) -> RuntimeConfig:
    """把 config.strategy.yaml 的 execution 节点注入运行时 ExecutionConfig。

    四个键严格必填、禁止未知键、取值校验与主配置同源(复用 config.py 的
    校验函数)，不提供任何代码默认值：缺失或写错都在连接 Redis/QMT 前阻止启动。
    """
    if not isinstance(raw, dict) or "execution" not in raw:
        raise ValueError(
            "config.strategy.yaml 缺少字段: execution"
            "(策略层执行开关已从主配置迁移至此, 必须显式提供)"
        )
    execution_raw = _mapping(raw["execution"], "execution", _EXECUTION_OVERLAY_FIELDS)
    execution = replace(
        runtime.execution,
        limit_down_sell_mode=_validated_limit_down_sell_mode(
            execution_raw["limit_down_sell_mode"]
        ),
        limit_up_buy_mode=_validated_limit_up_buy_mode(
            execution_raw["limit_up_buy_mode"]
        ),
        max_single_position_pct=_validated_max_single_position_pct(
            execution_raw["max_single_position_pct"]
        ),
        sell_half_insufficient_lot_mode=_validated_sell_half_insufficient_lot_mode(
            execution_raw["sell_half_insufficient_lot_mode"]
        ),
    )
    return replace(runtime, execution=execution)


def load_strategy_config(
    main_config_path: str | Path,
    runtime_config: RuntimeConfig,
    *,
    workers: int,
) -> StrategyEngineConfig:
    """从主配置同目录固定加载私有 ``config.strategy.yaml``。

    本地策略引擎模式不提供自动降级：文件缺失(纯跟单模式除外)、引擎关闭、
    字段写错或与主配置冲突时，均在连接 Redis/QMT 前直接阻止启动。
    """
    strategy_path = Path(main_config_path).resolve().parent / "config.strategy.yaml"
    if not strategy_path.is_file():
        raise ValueError(f"缺少本地策略配置文件: {strategy_path}")
    try:
        with strategy_path.open("r", encoding="utf-8") as fh:
            raw = yaml.safe_load(fh)
    except yaml.YAMLError as exc:
        raise ValueError(f"config.strategy.yaml 解析失败: {exc}") from exc
    root = _mapping(raw, "config.strategy.yaml", {"strategy_engine", "execution"})
    field = "strategy_engine"
    engine_raw = root["strategy_engine"]
    legacy_paths = []
    if isinstance(engine_raw, dict):
        for node, legacy_field in _LEGACY_SCHEDULE_FIELDS:
            node_raw = engine_raw.get(node)
            if isinstance(node_raw, dict) and legacy_field in node_raw:
                legacy_paths.append(f"{field}.{node}.{legacy_field}")
    if legacy_paths:
        migrated = ", ".join(f"{path}（已迁移）" for path in legacy_paths)
        raise ValueError(f"禁止继续使用旧日程字段: {migrated}")
    data = _mapping(
        engine_raw,
        field,
        {
            "enabled",
            "strategy_id",
            "external_signals",
            "schedule",
            "candidate_plan",
            "opening_buy",
            "opening_exit",
            "intraday_hard_stop",
            "trailing_take_profit",
            "morning_exit",
            "afternoon_exit",
            "limit_detection",
            "data_safety",
        },
    )
    config = StrategyEngineConfig(
        enabled=_bool(data["enabled"], f"{field}.enabled"),
        strategy_id=_string(data["strategy_id"], f"{field}.strategy_id"),
        external_signals=_load_external_signals(data["external_signals"]),
        schedule=_load_schedule(data["schedule"]),
        candidate_plan=_load_candidate(data["candidate_plan"]),
        opening_buy=_load_opening_buy(data["opening_buy"]),
        opening_exit=_load_opening_exit(data["opening_exit"]),
        intraday_hard_stop=_load_hard_stop(data["intraday_hard_stop"]),
        trailing_take_profit=_load_trailing(data["trailing_take_profit"]),
        morning_exit=_load_morning_exit(data["morning_exit"]),
        afternoon_exit=_load_afternoon_exit(data["afternoon_exit"]),
        limit_detection=_load_limit_detection(data["limit_detection"]),
        data_safety=_load_data_safety(data["data_safety"]),
    )
    _validate_runtime_contract(config, runtime_config, workers)
    return config


def load_runtime_with_strategy(
    path: str | Path, *, workers: int
) -> tuple[RuntimeConfig, StrategyEngineConfig | None]:
    """一次加载主配置与固定路径策略配置，供 ``python main.py`` 启动链使用。

    两种运行模式由 config.strategy.yaml 是否存在决定:

    - 文件存在 → 本地策略引擎模式(专用账户): 严格单策略契约、candidate_plan
      驱动, 策略层执行开关从该文件的 execution 节点注入 ExecutionConfig,
      主配置残留这些键时拒绝启动;
    - 文件缺失 → 纯跟单模式(泛用): 不做本地决策, 只执行 Redis 里白名单策略
      的普通信号(plan/buy/sell/sell_half/sell_all/watchlist), 返回 strategy=None,
      主配置可直接携带策略层执行开关(机器侧默认值兜底)。
    """
    strategy_path = Path(path).resolve().parent / "config.strategy.yaml"
    if not strategy_path.is_file():
        return load_config(path), None
    _reject_migrated_execution_keys(path)
    runtime = load_config(path)
    try:
        with strategy_path.open("r", encoding="utf-8") as fh:
            raw = yaml.safe_load(fh)
    except yaml.YAMLError as exc:
        raise ValueError(f"config.strategy.yaml 解析失败: {exc}") from exc
    runtime = _overlay_strategy_execution(runtime, raw)
    strategy = load_strategy_config(path, runtime, workers=workers)
    return runtime, strategy

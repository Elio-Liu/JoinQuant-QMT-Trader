"""加载并校验 YAML 配置，产出强类型的 RuntimeConfig。

解析 config.yaml 中的 Redis、交易、行情与机器日程配置，应用字段默认值并展开
${ENV_NAME} 形式的环境变量；所有校验失败一律抛出 ValueError 中止启动。

本模块约定:
- machine_schedule 所有字段必须在 YAML 中显式配置，缺失即启动失败。
- 写错的值（如定价模式、布尔开关、日程时间）硬校验报错，不静默回退。
"""

from __future__ import annotations

import datetime as dt
import os
import re
from dataclasses import dataclass
from pathlib import Path

import yaml

from miniqmt_follower.models import ExecutionConfig


@dataclass(frozen=True)
class RedisConfig:
    """Redis Stream 连接和消费组配置。"""

    host: str
    port: int
    password: str | None
    stream: str
    group: str
    consumer: str
    block_ms: int = 1000
    # strategy_id 白名单: 非空时只执行名单内策略的信号, 其余记日志后直接 ACK。
    # Stream 是多策略共享的, 只推进部分策略实盘时用它挡住其余策略的历史/测试信号。
    # 空元组 = 不过滤 (兼容旧配置)。
    allowed_strategy_ids: tuple[str, ...] = ()
    # —— 跨网络部署的连接健壮性参数 ——
    socket_connect_timeout_sec: float = 3.0
    # 读超时 = block_ms + 该余量。必须大于 BLOCK 时长, 否则阻塞读被自己的超时掐断。
    socket_timeout_margin_sec: float = 5.0
    # redis-py 定期 PING 探活间隔, 用于发现 TCP 还在但对端已消失的"半死连接"。
    health_check_interval_sec: int = 30
    # 已被旧进程取走、超过该空闲时间仍未确认的消息，允许当前进程接管核对。
    pending_claim_idle_ms: int = 60000
    pending_scan_interval_sec: float = 5.0


@dataclass(frozen=True)
class TradingConfig:
    """QMT 交易适配器配置。"""

    enabled: bool = False
    account_id: str = ""
    miniqmt_path: str = ""
    session_id: int = 0
    strategy_name: str = "jq_qmt_follower"


@dataclass(frozen=True)
class MarketDataConfig:
    """行情适配器配置。"""

    # 启动时预订阅的聚宽格式代码列表。订阅后取价走本地内存, 避免临场实时请求行情服务器。
    pre_subscribe_codes: tuple[str, ...] = ()


@dataclass(frozen=True)
class MarketSessionScheduleConfig:
    """A 股交易时段的交易机安全边界。"""

    call_auction_start_at: dt.time
    preopen_sell_start_at: dt.time
    continuous_trading_start_at: dt.time
    closing_call_auction_start_at: dt.time
    market_close_at: dt.time


@dataclass(frozen=True)
class OrderGuardScheduleConfig:
    """交易机报单与涨跌停排队撤单的硬性时限。"""

    strategy_sell_last_submit_at: dt.time
    limit_down_queue_cancel_at: dt.time
    limit_up_queue_cancel_at: dt.time


@dataclass(frozen=True)
class LifecycleScheduleConfig:
    """交易机日终生命周期时点。"""

    daily_summary_at: dt.time


@dataclass(frozen=True)
class MachineScheduleConfig:
    """交易机共享日程；所有字段都必须在 YAML 中明确配置。"""

    market_session: MarketSessionScheduleConfig
    order_guard: OrderGuardScheduleConfig
    lifecycle: LifecycleScheduleConfig


@dataclass(frozen=True)
class RuntimeConfig:
    """运行期总配置。"""

    redis: RedisConfig
    execution: ExecutionConfig
    trading: TradingConfig
    machine_schedule: MachineScheduleConfig
    state_db: Path
    market_data: MarketDataConfig = MarketDataConfig()
    log_level: str = "INFO"
    log_file_level: str = "DEBUG"
    log_dir: str = "logs"


def _validated_limit_down_sell_mode(raw_value: object) -> str:
    """校验跌停卖出模式配置; 缺省 "" 表示由旧开关 skip_sell_when_limit_down 推导。"""
    mode = str(raw_value or "").strip().lower()
    if mode not in {"", "skip", "queue", "none"}:
        raise ValueError(
            f"execution.limit_down_sell_mode 取值非法: {raw_value!r} "
            '(允许: "skip" / "queue" / "none", 或留空由旧开关推导)'
        )
    return mode


def _validated_limit_up_buy_mode(raw_value: object) -> str:
    """校验涨停买入模式配置；缺省时由旧 skip_buy_when_limit_up 推导。"""
    mode = str(raw_value or "").strip().lower()
    if mode not in {"", "skip", "queue", "none"}:
        raise ValueError(
            f"execution.limit_up_buy_mode 取值非法: {raw_value!r} "
            '(允许: "skip" / "queue" / "none", 或留空由旧开关推导)'
        )
    return mode


def _validated_plan_execute_at(raw_value: object) -> str:
    """校验 plan 执行时刻必须为 HH:MM:SS 格式。"""
    value = str(raw_value or "09:30:00").strip()
    try:
        dt.datetime.strptime(value, "%H:%M:%S")
    except ValueError:
        raise ValueError(
            f"execution.plan_execute_at 格式非法: {raw_value!r} (需要 HH:MM:SS)"
        )
    return value


def _parse_hhmmss_time(raw_value: object, field: str) -> dt.time:
    """严格解析交易机日程时间，不接受省略秒的写法。"""
    if not isinstance(raw_value, str) or not re.fullmatch(r"\d{2}:\d{2}:\d{2}", raw_value):
        raise ValueError(f"{field} 格式非法: {raw_value!r} (需要 HH:MM:SS)")
    try:
        return dt.datetime.strptime(raw_value, "%H:%M:%S").time()
    except ValueError:
        raise ValueError(f"{field} 格式非法: {raw_value!r} (需要 HH:MM:SS)")


def _required_mapping(
    raw_value: object, field: str, expected_keys: set[str]
) -> dict[str, object]:
    """日程节点必须是字段完整、没有未知键的 YAML 映射。"""
    if not isinstance(raw_value, dict):
        raise ValueError(f"{field} 必须是 YAML 映射")
    unknown_keys = set(raw_value) - expected_keys
    if unknown_keys:
        raise ValueError(f"{field} 包含未知字段: {', '.join(sorted(unknown_keys))}")
    missing_keys = expected_keys - set(raw_value)
    if missing_keys:
        raise ValueError(f"{field} 缺少必填字段: {', '.join(sorted(missing_keys))}")
    return raw_value


def _load_machine_schedule(raw_value: object) -> MachineScheduleConfig:
    """解析并校验 machine_schedule 各子节点，构造日程配置。"""
    root = _required_mapping(
        raw_value,
        "machine_schedule",
        {"market_session", "order_guard", "lifecycle"},
    )
    market_raw = _required_mapping(
        root["market_session"],
        "machine_schedule.market_session",
        {
            "call_auction_start_at",
            "preopen_sell_start_at",
            "continuous_trading_start_at",
            "closing_call_auction_start_at",
            "market_close_at",
        },
    )
    order_guard_raw = _required_mapping(
        root["order_guard"],
        "machine_schedule.order_guard",
        {
            "strategy_sell_last_submit_at",
            "limit_down_queue_cancel_at",
            "limit_up_queue_cancel_at",
        },
    )
    lifecycle_raw = _required_mapping(
        root["lifecycle"],
        "machine_schedule.lifecycle",
        {"daily_summary_at"},
    )
    schedule = MachineScheduleConfig(
        market_session=MarketSessionScheduleConfig(
            call_auction_start_at=_parse_hhmmss_time(
                market_raw["call_auction_start_at"],
                "machine_schedule.market_session.call_auction_start_at",
            ),
            preopen_sell_start_at=_parse_hhmmss_time(
                market_raw["preopen_sell_start_at"],
                "machine_schedule.market_session.preopen_sell_start_at",
            ),
            continuous_trading_start_at=_parse_hhmmss_time(
                market_raw["continuous_trading_start_at"],
                "machine_schedule.market_session.continuous_trading_start_at",
            ),
            closing_call_auction_start_at=_parse_hhmmss_time(
                market_raw["closing_call_auction_start_at"],
                "machine_schedule.market_session.closing_call_auction_start_at",
            ),
            market_close_at=_parse_hhmmss_time(
                market_raw["market_close_at"],
                "machine_schedule.market_session.market_close_at",
            ),
        ),
        order_guard=OrderGuardScheduleConfig(
            strategy_sell_last_submit_at=_parse_hhmmss_time(
                order_guard_raw["strategy_sell_last_submit_at"],
                "machine_schedule.order_guard.strategy_sell_last_submit_at",
            ),
            limit_down_queue_cancel_at=_parse_hhmmss_time(
                order_guard_raw["limit_down_queue_cancel_at"],
                "machine_schedule.order_guard.limit_down_queue_cancel_at",
            ),
            limit_up_queue_cancel_at=_parse_hhmmss_time(
                order_guard_raw["limit_up_queue_cancel_at"],
                "machine_schedule.order_guard.limit_up_queue_cancel_at",
            ),
        ),
        lifecycle=LifecycleScheduleConfig(
            daily_summary_at=_parse_hhmmss_time(
                lifecycle_raw["daily_summary_at"],
                "machine_schedule.lifecycle.daily_summary_at",
            ),
        ),
    )
    _validate_machine_schedule(schedule)
    return schedule


def _validate_machine_schedule(schedule: MachineScheduleConfig) -> None:
    """校验各交易时段与撤单时限严格有序，避免日程错配。"""
    market = schedule.market_session
    guard = schedule.order_guard
    lifecycle = schedule.lifecycle
    if not (
        market.call_auction_start_at
        < market.preopen_sell_start_at
        < market.continuous_trading_start_at
        < market.closing_call_auction_start_at
        < market.market_close_at
        < lifecycle.daily_summary_at
    ):
        raise ValueError("machine_schedule 市场时段必须严格按集合竞价、盘前卖出、连续竞价、收盘竞价、收盘、日结排序")
    for field, cancel_at in (
        ("limit_down_queue_cancel_at", guard.limit_down_queue_cancel_at),
        ("limit_up_queue_cancel_at", guard.limit_up_queue_cancel_at),
    ):
        if not (
            market.continuous_trading_start_at
            < guard.strategy_sell_last_submit_at
            < cancel_at
            < market.closing_call_auction_start_at
        ):
            raise ValueError(
                "machine_schedule.order_guard."
                f"{field} 必须满足 continuous_trading_start_at < "
                "strategy_sell_last_submit_at < cancel_at < "
                "closing_call_auction_start_at"
            )
    if lifecycle.daily_summary_at <= max(
        market.market_close_at,
        guard.limit_down_queue_cancel_at,
        guard.limit_up_queue_cancel_at,
    ):
        raise ValueError("machine_schedule.lifecycle.daily_summary_at 必须晚于收盘和全部排队撤单时间")


def _validated_max_single_position_pct(raw_value: object) -> float:
    """校验单票仓位上限必须位于 (0, 1] 区间。"""
    pct = float(raw_value)
    if not (0.0 < pct <= 1.0):
        raise ValueError(f"execution.max_single_position_pct 必须位于 (0,1]: {raw_value!r}")
    return pct


def _validated_pricing_mode(raw_value: object) -> str:
    """校验定价模式。写错任何值都静默回退 slippage 是很贵的误会 —— 开盘时会用
    完全不同的定价逻辑, 而日志里没有任何提示, 所以这里必须硬校验。"""
    mode = str(raw_value or "slippage").strip().lower()
    if mode not in {"slippage", "book"}:
        raise ValueError(
            f'execution.pricing_mode 取值非法: {raw_value!r} (允许: "slippage" / "book")'
        )
    return mode


def _resolve_env_placeholder(value: object) -> object:
    """把 "${ENV_NAME}" 形式的值替换成环境变量内容; 其余原样返回。"""
    if isinstance(value, str) and value.startswith("${") and value.endswith("}"):
        return os.environ.get(value[2:-1])
    return value


def _validated_bool(raw_value: object, field: str) -> bool:
    """YAML 开关必须是真布尔值，避免 bool("false") 反而得到 True。"""
    if not isinstance(raw_value, bool):
        raise ValueError(f"{field} 必须写成 YAML 布尔值 true/false: {raw_value!r}")
    return raw_value


def _validated_string_list(raw_value: object, field: str) -> tuple[str, ...]:
    """把 YAML 列表转换为字符串元组；None 视为空列表。"""
    if raw_value is None:
        return ()
    if not isinstance(raw_value, list):
        raise ValueError(f"{field} 必须是 YAML 列表: {raw_value!r}")
    return tuple(str(item) for item in raw_value)


def _validated_positive_number(raw_value: object, field: str, cast):
    """按给定类型转换并校验数值必须大于 0。"""
    value = cast(raw_value)
    if value <= 0:
        raise ValueError(f"{field} 必须大于0: {raw_value!r}")
    return value


def _validated_cash_fee_buffer_pct(raw_value: object) -> float:
    """校验买入资金的手续费缓冲比例, 允许 [0, 0.05)。"""
    pct = float(raw_value)
    if not (0.0 <= pct < 0.05):
        raise ValueError(
            f"execution.cash_fee_buffer_pct 必须位于 [0, 0.05): {raw_value!r}"
        )
    return pct


def _validated_sell_half_insufficient_lot_mode(raw_value: object) -> str:
    """校验 sell_half 半仓不足一手时的处理模式。"""
    mode = str(raw_value or "sell_all").strip().lower()
    if mode not in {"sell_all", "skip"}:
        raise ValueError(
            f"execution.sell_half_insufficient_lot_mode 取值非法: {raw_value!r} "
            '(允许: "sell_all" / "skip")'
        )
    return mode


def _validated_signal_expire_seconds(raw_value: object) -> int:
    """校验信号过期秒数; 0 表示不过期。"""
    seconds = int(raw_value)
    if seconds < 0:
        raise ValueError(f"execution.signal_expire_seconds 不能为负数: {raw_value!r}")
    return seconds


def _validated_log_level(raw_value: object) -> str:
    """日志级别白名单校验, 写错直接启动失败而不是静默丢日志。"""
    level = str(raw_value or "DEBUG").strip().upper()
    if level not in {"DEBUG", "INFO", "WARNING", "ERROR"}:
        raise ValueError(
            f"log_file_level 取值非法: {raw_value!r} (允许: DEBUG/INFO/WARNING/ERROR)"
        )
    return level


def _validated_non_negative_float(raw_value: object, field: str) -> float:
    """非负浮点校验, 用于"0 表示关闭"的开关类参数。"""
    value = float(raw_value)
    if value < 0:
        raise ValueError(f"{field} 不能为负数: {raw_value!r}")
    return value


def load_config(path: str | Path) -> RuntimeConfig:
    """从 YAML 配置文件加载运行参数。

    password 支持 "${ENV_NAME}" 形式, 便于把密码放到环境变量里。
    """
    config_path = Path(path)
    if config_path.suffix.lower() not in {".yaml", ".yml"}:
        raise ValueError(f"配置文件必须使用 YAML 格式（.yaml 或 .yml）: {config_path}")
    with config_path.open("r", encoding="utf-8") as fh:
        raw = yaml.safe_load(fh)
    if not isinstance(raw, dict):
        raise ValueError(f"YAML 配置根节点必须是映射: {config_path}")

    redis_raw = raw["redis"]
    execution_raw = raw.get("execution", {})
    trading_raw = raw.get("trading", {})
    market_data_raw = raw.get("market_data", {})
    legacy_paths = [
        f"execution.{legacy_key}"
        for legacy_key in ("queue_sell_deadline", "queue_buy_deadline")
        if isinstance(execution_raw, dict) and legacy_key in execution_raw
    ]
    if legacy_paths:
        migrated = ", ".join(f"{field}（已迁移）" for field in legacy_paths)
        raise ValueError(f"禁止继续使用旧日程字段: {migrated}")
    if "machine_schedule" not in raw:
        raise ValueError("缺少必填配置: machine_schedule")
    machine_schedule = _load_machine_schedule(raw["machine_schedule"])
    # 例如 "${REDIS_PASSWORD}" 会读取环境变量 REDIS_PASSWORD。
    password = _resolve_env_placeholder(redis_raw.get("password"))

    # 配置文件缺省时使用保守默认值, 方便先跑通 MVP。
    return RuntimeConfig(
        redis=RedisConfig(
            host=str(redis_raw["host"]),
            port=int(redis_raw.get("port", 6379)),
            password=password,
            stream=str(redis_raw.get("stream", "jq_qmt_signals")),
            group=str(redis_raw.get("group", "qmt_executors")),
            consumer=str(redis_raw.get("consumer", "win-qmt-01")),
            block_ms=int(redis_raw.get("block_ms", 1000)),
            allowed_strategy_ids=_validated_string_list(
                redis_raw.get("allowed_strategy_ids", []),
                "redis.allowed_strategy_ids",
            ),
            socket_connect_timeout_sec=float(
                redis_raw.get("socket_connect_timeout_sec", 3.0)
            ),
            socket_timeout_margin_sec=float(
                redis_raw.get("socket_timeout_margin_sec", 5.0)
            ),
            health_check_interval_sec=int(
                redis_raw.get("health_check_interval_sec", 30)
            ),
            pending_claim_idle_ms=_validated_positive_number(
                redis_raw.get("pending_claim_idle_ms", 60000),
                "redis.pending_claim_idle_ms",
                int,
            ),
            pending_scan_interval_sec=_validated_positive_number(
                redis_raw.get("pending_scan_interval_sec", 5.0),
                "redis.pending_scan_interval_sec",
                float,
            ),
        ),
        execution=ExecutionConfig(
            quote_band_pct=_validated_non_negative_float(
                execution_raw.get("quote_band_pct", 0.015),
                "execution.quote_band_pct",
            ),
            order_timeout_sec=float(execution_raw.get("order_timeout_sec", 3.0)),
            max_attempts=int(execution_raw.get("max_attempts", 3)),
            max_total_duration_sec=float(execution_raw.get("max_total_duration_sec", 15.0)),
            cancel_confirm_timeout_sec=float(
                execution_raw.get("cancel_confirm_timeout_sec", 30.0)
            ),
            poll_interval_sec=float(execution_raw.get("poll_interval_sec", 0.2)),
            pricing_mode=_validated_pricing_mode(
                execution_raw.get("pricing_mode", "slippage")
            ),
            book_tick_offset=int(execution_raw.get("book_tick_offset", 2)),
            skip_sell_when_limit_down=_validated_bool(
                execution_raw.get("skip_sell_when_limit_down", False),
                "execution.skip_sell_when_limit_down",
            ),
            skip_buy_when_limit_up=_validated_bool(
                execution_raw.get("skip_buy_when_limit_up", False),
                "execution.skip_buy_when_limit_up",
            ),
            limit_down_sell_mode=_validated_limit_down_sell_mode(
                execution_raw.get("limit_down_sell_mode", "")
            ),
            queue_sell_poll_interval_sec=float(
                execution_raw.get("queue_sell_poll_interval_sec", 3.0)
            ),
            max_concurrent_queue_sells=int(
                execution_raw.get("max_concurrent_queue_sells", 2)
            ),
            limit_up_buy_mode=_validated_limit_up_buy_mode(
                execution_raw.get("limit_up_buy_mode", "")
            ),
            max_concurrent_queue_buys=int(
                execution_raw.get("max_concurrent_queue_buys", 5)
            ),
            plan_enabled=_validated_bool(
                execution_raw.get("plan_enabled", True), "execution.plan_enabled"
            ),
            plan_execute_at=_validated_plan_execute_at(
                execution_raw.get("plan_execute_at", "09:30:00")
            ),
            max_single_position_pct=_validated_max_single_position_pct(
                execution_raw.get("max_single_position_pct", 0.5)
            ),
            cash_fee_buffer_pct=_validated_cash_fee_buffer_pct(
                execution_raw.get("cash_fee_buffer_pct", 0.003)
            ),
            sell_half_insufficient_lot_mode=_validated_sell_half_insufficient_lot_mode(
                execution_raw.get("sell_half_insufficient_lot_mode", "sell_all")
            ),
            signal_expire_seconds=_validated_signal_expire_seconds(
                execution_raw.get("signal_expire_seconds", 600)
            ),
            quote_max_age_sec=_validated_non_negative_float(
                execution_raw.get("quote_max_age_sec", 0.0),
                "execution.quote_max_age_sec",
            ),
            opening_aggressive_window_sec=_validated_non_negative_float(
                execution_raw.get("opening_aggressive_window_sec", 60.0),
                "execution.opening_aggressive_window_sec",
            ),
            opening_order_timeout_sec=_validated_non_negative_float(
                execution_raw.get("opening_order_timeout_sec", 0.0),
                "execution.opening_order_timeout_sec",
            ),
            opening_price_gap_wait_pct=_validated_non_negative_float(
                execution_raw.get("opening_price_gap_wait_pct", 0.0),
                "execution.opening_price_gap_wait_pct",
            ),
            opening_price_gap_wait_max_sec=_validated_non_negative_float(
                execution_raw.get("opening_price_gap_wait_max_sec", 0.0),
                "execution.opening_price_gap_wait_max_sec",
            ),
            opening_price_gap_wait_poll_sec=_validated_non_negative_float(
                execution_raw.get("opening_price_gap_wait_poll_sec", 0.2),
                "execution.opening_price_gap_wait_poll_sec",
            ),
            ghost_order_detect_grace_sec=_validated_non_negative_float(
                execution_raw.get("ghost_order_detect_grace_sec", 0.0),
                "execution.ghost_order_detect_grace_sec",
            ),
            ghost_order_auto_resubmit=_validated_bool(
                execution_raw.get("ghost_order_auto_resubmit", True),
                "execution.ghost_order_auto_resubmit",
            ),
        ),
        trading=TradingConfig(
            enabled=_validated_bool(
                trading_raw.get("enabled", False), "trading.enabled"
            ),
            account_id=str(trading_raw.get("account_id", "")),
            miniqmt_path=str(trading_raw.get("miniqmt_path", "")),
            session_id=int(trading_raw.get("session_id", 0)),
            strategy_name=str(trading_raw.get("strategy_name", "jq_qmt_follower")),
        ),
        machine_schedule=machine_schedule,
        market_data=MarketDataConfig(
            pre_subscribe_codes=_validated_string_list(
                market_data_raw.get("pre_subscribe_codes", []),
                "market_data.pre_subscribe_codes",
            ),
        ),
        state_db=Path(raw.get("state_db", "data/miniqmt_follower.db")),
        log_level=str(raw.get("log_level", "INFO")),
        log_file_level=_validated_log_level(raw.get("log_file_level", "DEBUG")),
        log_dir=str(raw.get("log_dir", "logs")),
    )

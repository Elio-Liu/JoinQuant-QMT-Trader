from __future__ import annotations

import datetime as dt
import os
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
    strategy_name: str = "tidal_quant"


@dataclass(frozen=True)
class MarketDataConfig:
    """行情适配器配置。"""

    # 启动时预订阅的聚宽格式代码列表。订阅后取价走本地内存, 避免临场实时请求行情服务器。
    pre_subscribe_codes: tuple[str, ...] = ()


@dataclass(frozen=True)
class RuntimeConfig:
    """运行期总配置。"""

    redis: RedisConfig
    execution: ExecutionConfig
    trading: TradingConfig
    state_db: Path
    market_data: MarketDataConfig = MarketDataConfig()
    log_level: str = "INFO"
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
    if raw_value is None:
        return ()
    if not isinstance(raw_value, list):
        raise ValueError(f"{field} 必须是 YAML 列表: {raw_value!r}")
    return tuple(str(item) for item in raw_value)


def _validated_positive_number(raw_value: object, field: str, cast):
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
    # 例如 "${REDIS_PASSWORD}" 会读取环境变量 REDIS_PASSWORD。
    password = _resolve_env_placeholder(redis_raw.get("password"))

    # 配置文件缺省时使用保守默认值, 方便先跑通 MVP。
    return RuntimeConfig(
        redis=RedisConfig(
            host=str(redis_raw["host"]),
            port=int(redis_raw.get("port", 6379)),
            password=password,
            stream=str(redis_raw.get("stream", "tidal_quant_signals")),
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
            buy_slippage_pct=float(execution_raw.get("buy_slippage_pct", 0.003)),
            sell_slippage_pct=float(execution_raw.get("sell_slippage_pct", 0.003)),
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
            auction_aggressive_pct=float(
                execution_raw.get("auction_aggressive_pct", 0.02)
            ),
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
            queue_sell_deadline=str(
                execution_raw.get("queue_sell_deadline", "14:56:30")
            ),
            max_concurrent_queue_sells=int(
                execution_raw.get("max_concurrent_queue_sells", 2)
            ),
            limit_up_buy_mode=_validated_limit_up_buy_mode(
                execution_raw.get("limit_up_buy_mode", "")
            ),
            queue_buy_deadline=str(
                execution_raw.get("queue_buy_deadline", "14:56:30")
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
        ),
        trading=TradingConfig(
            enabled=_validated_bool(
                trading_raw.get("enabled", False), "trading.enabled"
            ),
            account_id=str(trading_raw.get("account_id", "")),
            miniqmt_path=str(trading_raw.get("miniqmt_path", "")),
            session_id=int(trading_raw.get("session_id", 0)),
            strategy_name=str(trading_raw.get("strategy_name", "tidal_quant")),
        ),
        market_data=MarketDataConfig(
            pre_subscribe_codes=_validated_string_list(
                market_data_raw.get("pre_subscribe_codes", []),
                "market_data.pre_subscribe_codes",
            ),
        ),
        state_db=Path(raw.get("state_db", "data/miniqmt_follower.db")),
        log_level=str(raw.get("log_level", "INFO")),
        log_dir=str(raw.get("log_dir", "logs")),
    )

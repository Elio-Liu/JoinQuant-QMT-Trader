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
    block_ms: int = 20
    # strategy_id 白名单: 非空时只执行名单内策略的信号, 其余记日志后直接 ACK。
    # Stream 是多策略共享的, 只推进部分策略实盘时用它挡住其余策略的历史/测试信号。
    # 空元组 = 不过滤 (兼容旧配置)。
    allowed_strategy_ids: tuple[str, ...] = ()


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
    password = redis_raw.get("password")
    # 例如 "${REDIS_PASSWORD}" 会读取环境变量 REDIS_PASSWORD。
    if isinstance(password, str) and password.startswith("${") and password.endswith("}"):
        password = os.environ.get(password[2:-1])

    # 配置文件缺省时使用保守默认值, 方便先跑通 MVP。
    return RuntimeConfig(
        redis=RedisConfig(
            host=str(redis_raw["host"]),
            port=int(redis_raw.get("port", 6379)),
            password=password,
            stream=str(redis_raw.get("stream", "tidal_quant_signals")),
            group=str(redis_raw.get("group", "qmt_executors")),
            consumer=str(redis_raw.get("consumer", "win-qmt-01")),
            block_ms=int(redis_raw.get("block_ms", 20)),
            allowed_strategy_ids=tuple(
                str(sid) for sid in redis_raw.get("allowed_strategy_ids", []) or []
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
            pricing_mode=str(execution_raw.get("pricing_mode", "slippage")),
            book_tick_offset=int(execution_raw.get("book_tick_offset", 2)),
            auction_aggressive_pct=float(
                execution_raw.get("auction_aggressive_pct", 0.02)
            ),
            skip_sell_when_limit_down=bool(
                execution_raw.get("skip_sell_when_limit_down", False)
            ),
            skip_buy_when_limit_up=bool(
                execution_raw.get("skip_buy_when_limit_up", False)
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
            plan_enabled=bool(execution_raw.get("plan_enabled", True)),
            plan_execute_at=_validated_plan_execute_at(
                execution_raw.get("plan_execute_at", "09:30:00")
            ),
            max_single_position_pct=_validated_max_single_position_pct(
                execution_raw.get("max_single_position_pct", 0.2)
            ),
        ),
        trading=TradingConfig(
            enabled=bool(trading_raw.get("enabled", False)),
            account_id=str(trading_raw.get("account_id", "")),
            miniqmt_path=str(trading_raw.get("miniqmt_path", "")),
            session_id=int(trading_raw.get("session_id", 0)),
            strategy_name=str(trading_raw.get("strategy_name", "tidal_quant")),
        ),
        market_data=MarketDataConfig(
            pre_subscribe_codes=tuple(
                str(code) for code in market_data_raw.get("pre_subscribe_codes", [])
            ),
        ),
        state_db=Path(raw.get("state_db", "data/miniqmt_follower.db")),
        log_level=str(raw.get("log_level", "INFO")),
        log_dir=str(raw.get("log_dir", "logs")),
    )

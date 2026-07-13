from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

import yaml

from qmt_follower.models import ExecutionConfig


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
        ),
        execution=ExecutionConfig(
            buy_slippage_pct=float(execution_raw.get("buy_slippage_pct", 0.003)),
            sell_slippage_pct=float(execution_raw.get("sell_slippage_pct", 0.003)),
            order_timeout_sec=float(execution_raw.get("order_timeout_sec", 3.0)),
            max_attempts=int(execution_raw.get("max_attempts", 3)),
            max_total_duration_sec=float(execution_raw.get("max_total_duration_sec", 15.0)),
            max_deviation_from_signal_price_pct=float(
                execution_raw.get("max_deviation_from_signal_price_pct", 0.02)
            ),
            poll_interval_sec=float(execution_raw.get("poll_interval_sec", 0.2)),
            pricing_mode=str(execution_raw.get("pricing_mode", "slippage")),
            book_tick_offset=int(execution_raw.get("book_tick_offset", 2)),
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
        state_db=Path(raw.get("state_db", "data/qmt_follower.db")),
        log_level=str(raw.get("log_level", "INFO")),
        log_dir=str(raw.get("log_dir", "logs")),
    )

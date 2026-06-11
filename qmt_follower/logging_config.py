"""集中日志配置。

提供 setup_logging() 作为整个项目的日志统一入口。
控制台输出简洁格式，文件输出详细格式（按天轮转）。
"""

from __future__ import annotations

import logging
import os
from datetime import datetime
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path
from typing import Any


class _ColoredFormatter(logging.Formatter):
    """控制台日志格式器 —— 带中文标签和简洁时间。

    格式: [HH:MM:SS] [级别标签] [模块] 消息
    """

    # 级别 → 中文短标签
    LEVEL_LABELS: dict[int, str] = {
        logging.DEBUG: "调试",
        logging.INFO: "信息",
        logging.WARNING: "警告",
        logging.ERROR: "错误",
        logging.CRITICAL: "严重",
    }

    def format(self, record: logging.LogRecord) -> str:
        label = self.LEVEL_LABELS.get(record.levelno, record.levelname)
        ts = datetime.fromtimestamp(record.created).strftime("%H:%M:%S")
        module = record.name.rsplit(".", 1)[-1] if "." in record.name else record.name
        return f"[{ts}] [{label}] [{module}] {record.getMessage()}"


class _FileFormatter(logging.Formatter):
    """文件日志格式器 —— 完整时间戳、毫秒、模块路径、行号。

    格式: [YYYY-MM-DD HH:MM:SS.fff] [级别] [模块:行号] 消息
    """

    def format(self, record: logging.LogRecord) -> str:
        ts = datetime.fromtimestamp(record.created).strftime("%Y-%m-%d %H:%M:%S")
        ms = f"{record.created % 1:.3f}"[1:]  # ".123"
        return (
            f"[{ts}{ms}] [{record.levelname}] [{record.name}:{record.lineno}] {record.getMessage()}"
        )


def setup_logging(level: int | str = logging.INFO, log_dir: str | Path = "logs") -> None:
    """初始化项目的日志系统。

    Args:
        level: 日志级别，DEBUG/INFO/WARNING/ERROR，默认 INFO。
        log_dir: 日志文件目录，自动创建；默认 "logs"。
    """
    if isinstance(level, str):
        level = getattr(logging, level.upper(), logging.INFO)

    root = logging.getLogger()
    root.setLevel(level)

    # 清空已有 handler，避免重复配置（比如测试中多次调用 setup_logging）
    root.handlers.clear()

    # ---- 控制台 handler ----
    console = logging.StreamHandler()
    console.setLevel(level)
    console.setFormatter(_ColoredFormatter())
    root.addHandler(console)

    # ---- 文件 handler: 按天轮转，保留 30 天 ----
    log_path = Path(log_dir)
    log_path.mkdir(parents=True, exist_ok=True)
    file_handler = TimedRotatingFileHandler(
        filename=log_path / "qmt_follower.log",
        when="midnight",
        interval=1,
        backupCount=30,
        encoding="utf-8",
    )
    file_handler.setLevel(logging.DEBUG)  # 文件始终记录 DEBUG 及以上，便于事后排查
    file_handler.setFormatter(_FileFormatter())
    root.addHandler(file_handler)

    # ---- 降低第三方库日志噪音 ----
    _silence_noisy_libraries()


def _silence_noisy_libraries() -> None:
    """将常见第三方库的日志级别提高到 WARNING，减少噪音。"""
    noisy: list[str] = [
        "redis",
        "urllib3",
        "requests",
        "websocket",
    ]
    for name in noisy:
        logging.getLogger(name).setLevel(logging.WARNING)


def get_logger(name: str) -> logging.Logger:
    """获取模块级 logger 的便捷函数。等价于 logging.getLogger(name)。"""
    return logging.getLogger(name)

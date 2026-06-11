from __future__ import annotations

import json
import logging
import threading
from collections.abc import Iterator
from dataclasses import dataclass
from typing import Any

from qmt_follower.config import RedisConfig
from qmt_follower.models import TradeSignal

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class StreamMessage:
    """Redis Stream 中的一条消息。

    message_id 是 Redis 生成的流 ID, signal 是解析后的交易信号。
    """

    message_id: str
    signal: TradeSignal


class RedisStreamClient:
    """Redis Stream 客户端封装。

    Stream 用来替代 Pub/Sub, 因为订单信号不能因为 Windows 程序离线而丢失。
    """

    def __init__(self, config: RedisConfig):
        try:
            import redis
        except ImportError as exc:
            raise RuntimeError("Install redis package before using RedisStreamClient") from exc

        self.config = config
        self.client = redis.Redis(
            host=config.host,
            port=config.port,
            password=config.password,
            decode_responses=True,
            socket_connect_timeout=1,
        )

    def ensure_group(self) -> None:
        """确保消费组存在。

        BUSYGROUP 表示组已经存在, 属于正常情况, 其他异常继续抛出。
        """
        try:
            self.client.xgroup_create(self.config.stream, self.config.group, id="0", mkstream=True)
            logger.info("📡 Redis消费组已创建 | stream=%s group=%s", self.config.stream, self.config.group)
        except Exception as exc:
            if "BUSYGROUP" in str(exc):
                logger.debug("📡 Redis消费组已存在 | stream=%s group=%s", self.config.stream, self.config.group)
                return
            logger.error("❌ Redis消费组创建失败 | stream=%s group=%s 错误=%s",
                          self.config.stream, self.config.group, exc)
            raise

    def publish_signal(self, payload: dict[str, Any]) -> str:
        """写入一条信号到 Stream。主要用于本地测试或未来工具脚本。"""
        return self.client.xadd(
            self.config.stream,
            {"payload": json.dumps(payload, ensure_ascii=False)},
            maxlen=10000,
            approximate=True,
        )

    def read_forever(
        self,
        block_ms: int | None = None,
        count: int = 10,
        stop_event: threading.Event | None = None,
    ) -> Iterator[StreamMessage]:
        """持续消费新消息。

        使用 XREADGROUP 的 ">" 只读取当前消费者组尚未投递的新消息。
        后续如要处理 pending 未确认消息, 可以在这里增加 XPENDING/XCLAIM 逻辑。

        stop_event 用于优雅退出: 当 event 被 set 时，内部循环会立即退出，
        不再阻塞在 xreadgroup 上。调用方应在信号处理器中 set 该 event。
        """
        self.ensure_group()
        effective_block_ms = self.config.block_ms if block_ms is None else block_ms
        while True:
            if stop_event and stop_event.is_set():
                logger.debug("🛑 read_forever 收到停止信号，退出内部循环")
                return
            response = self.client.xreadgroup(
                self.config.group,
                self.config.consumer,
                {self.config.stream: ">"},
                count=count,
                block=effective_block_ms,
            )
            for _, messages in response:
                for message_id, fields in messages:
                    signal = _parse_signal(fields)
                    logger.debug(
                        "📨 Redis消息已解析 | msg_id=%s signal_id=%s",
                        message_id, signal.signal_id,
                    )
                    yield StreamMessage(message_id=message_id, signal=signal)

    def ack(self, message_id: str) -> None:
        """确认消息已处理。

        当前设计是在执行引擎进入终态后再 XACK, 这样程序中途崩溃时消息仍可恢复处理。
        """
        self.client.xack(self.config.stream, self.config.group, message_id)
        logger.debug("✅ Redis消息已确认 | msg_id=%s", message_id)


def _parse_signal(fields: dict[str, str]) -> TradeSignal:
    """兼容两种格式: payload JSON 字符串, 或直接把字段写在 Stream entry 里。"""
    if "payload" in fields:
        raw = json.loads(fields["payload"])
    else:
        raw = fields
    return TradeSignal.from_dict(raw)

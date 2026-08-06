from __future__ import annotations

import json
import logging
import threading
from collections.abc import Iterator
from dataclasses import dataclass
from typing import Any

from miniqmt_follower.config import RedisConfig
from miniqmt_follower.models import DailyPlan, TradeSignal

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class WatchlistCommand:
    """预订阅指令: 策略选股完成后提前推送股票池, 执行端立即订阅行情。

    不触发任何下单, 只是让开盘时的第一次取价读本地内存而不是临场拉行情。
    """

    codes: tuple[str, ...]
    strategy_id: str = ""


@dataclass(frozen=True)
class StreamMessage:
    """Redis Stream 中的一条消息。

    message_id 是 Redis 生成的流 ID。signal 与 watchlist/plan 三选一:
    交易信号填 signal, 预订阅指令(action=subscribe)填 watchlist,
    日计划(action=plan)填 plan。
    """

    message_id: str
    signal: TradeSignal | None = None
    watchlist: WatchlistCommand | None = None
    plan: DailyPlan | None = None


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

        新组从 Stream 最新位置（$）开始消费：多台交易机各用独立 group 时,
        盘中新加入的机器不会重放历史消息（早上的 plan 没有 expire_at,
        重放会导致误建仓）。错过 plan 的机器当日不买, 符合"宁可少买不盲买"。
        已存在的组不受影响（BUSYGROUP 直接忽略, 各自游标继续推进）。
        BUSYGROUP 表示组已经存在, 属于正常情况, 其他异常继续抛出。
        """
        try:
            self.client.xgroup_create(
                self.config.stream, self.config.group, id="$", mkstream=True
            )
            logger.info("【Redis】📡 消费组已创建 | stream=%s | group=%s", self.config.stream, self.config.group)
        except Exception as exc:
            if "BUSYGROUP" in str(exc):
                logger.debug("📡 Redis消费组已存在 | stream=%s group=%s", self.config.stream, self.config.group)
                return
            logger.error("【Redis】❌ 消费组创建失败 | stream=%s | group=%s | %s",
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
    ) -> Iterator[StreamMessage | None]:
        """持续消费新消息。

        使用 XREADGROUP 的 ">" 只读取当前消费者组尚未投递的新消息。
        后续如要处理 pending 未确认消息, 可以在这里增加 XPENDING/XCLAIM 逻辑。

        stop_event 用于优雅退出: 当 event 被 set 时，内部循环会立即退出，
        不再阻塞在 xreadgroup 上。调用方应在信号处理器中 set 该 event。

        没有新消息时也会 yield None（每次轮询一次, 间隔 block_ms）。这是为了让
        调用方能在没有新信号时仍持续 reap 已完成的任务并 ACK —— 否则已执行完的
        信号只能等到下一条新消息到达才会被记终态日志和 ACK, 行情安静时会造成
        日志时间线严重失真、ACK 被无限期延迟。
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
            if not response:
                yield None
                continue
            for _, messages in response:
                for message_id, fields in messages:
                    yield _parse_message(message_id, fields)

    def ack(self, message_id: str) -> None:
        """确认消息已处理。

        当前设计是在执行引擎进入终态后再 XACK, 这样程序中途崩溃时消息仍可恢复处理。
        """
        self.client.xack(self.config.stream, self.config.group, message_id)
        logger.debug("✅ Redis消息已确认 | msg_id=%s", message_id)


def _parse_message(message_id: str, fields: dict[str, str]) -> StreamMessage:
    """把一条 Stream entry 解析为交易信号或预订阅指令。"""
    if "payload" in fields:
        raw = json.loads(fields["payload"])
    else:
        raw = fields

    if str(raw.get("action", "")).lower() == "subscribe":
        watchlist = WatchlistCommand(
            codes=tuple(str(code) for code in raw.get("codes", [])),
            strategy_id=str(raw.get("strategy_id", "")),
        )
        logger.debug("📨 预订阅指令已解析 | msg_id=%s 数量=%s", message_id, len(watchlist.codes))
        return StreamMessage(message_id=message_id, watchlist=watchlist)

    if str(raw.get("action", "")).lower() == "plan":
        plan = DailyPlan.from_dict(raw)
        logger.debug("📨 日计划已解析 | msg_id=%s plan_id=%s", message_id, plan.signal_id)
        return StreamMessage(message_id=message_id, plan=plan)

    signal = TradeSignal.from_dict(raw)
    logger.debug("📨 Redis消息已解析 | msg_id=%s signal_id=%s", message_id, signal.signal_id)
    return StreamMessage(message_id=message_id, signal=signal)


def _parse_signal(fields: dict[str, str]) -> TradeSignal:
    """兼容两种格式: payload JSON 字符串, 或直接把字段写在 Stream entry 里。"""
    if "payload" in fields:
        raw = json.loads(fields["payload"])
    else:
        raw = fields
    return TradeSignal.from_dict(raw)

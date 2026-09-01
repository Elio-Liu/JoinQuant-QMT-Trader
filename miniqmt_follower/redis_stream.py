"""Redis Stream 消费组封装: 收信号、确认消息与接管遗留待办。

用消费组替代 Pub/Sub, 保证 Windows 程序离线期间订单信号不丢失;
提供 read_forever 持续消费、ack 终态确认、claim_stale_pending 重启接管,
以及 publish_signal 本地测试写入。收信循环对网络故障按退避重连自愈。

本模块约定:
- 消息解析失败收敛为 rejected 消息并 ACK, 绝不因一条坏消息打崩消费循环;
- ACK 只发生在执行引擎写入终态之后, 崩溃未确认的消息可由 pending 扫描恢复。
"""

from __future__ import annotations

import json
import logging
import threading
import time
from collections.abc import Iterator
from dataclasses import dataclass, replace
from collections.abc import Callable
from typing import Any

from miniqmt_follower.config import RedisConfig
from miniqmt_follower.models import DailyPlan, TradeSignal
from miniqmt_follower.strategy_models import CandidatePlan

logger = logging.getLogger(__name__)

# 断线重连的退避序列(秒)。开盘时段要快速恢复, 长时间故障时又不能把 Redis 打满。
_RECONNECT_BACKOFF_SEC = (0.5, 1.0, 2.0, 5.0, 10.0, 30.0)


@dataclass(frozen=True)
class WatchlistCommand:
    """预订阅指令: 策略选股完成后提前推送股票池, 执行端立即订阅行情。

    不触发任何下单, 只是让开盘时的第一次取价读本地内存而不是临场拉行情。
    """

    codes: tuple[str, ...]
    strategy_id: str = ""
    mode: str = "live"


@dataclass(frozen=True)
class StreamMessage:
    """Redis Stream 中的一条消息。

    message_id 是 Redis 生成的流 ID。signal 与 watchlist/plan/candidate_plan 四选一:
    交易信号填 signal, 预订阅指令(action=subscribe)填 watchlist,
    日计划(action=plan)填 plan。

    rejected 非空表示这条消息无法处理(JSON 畸形 / 字段缺失 / 类型不对),
    调用方应记日志后直接 ACK —— 一条坏消息不能卡住消费组, 更不能把消费循环
    打崩。Stream 是多策略共享的, 别的策略换个 schema 就可能产生这类消息。
    """

    message_id: str
    signal: TradeSignal | None = None
    watchlist: WatchlistCommand | None = None
    plan: DailyPlan | None = None
    candidate_plan: CandidatePlan | None = None
    rejected: str | None = None
    recovered: bool = False


class RedisStreamClient:
    """Redis Stream 客户端封装。

    Stream 用来替代 Pub/Sub, 因为订单信号不能因为 Windows 程序离线而丢失。
    """

    def __init__(self, config: RedisConfig):
        try:
            import redis
        except ImportError as exc:
            raise RuntimeError("Install redis package before using RedisStreamClient") from exc

        self._redis_module = redis
        self.config = config
        self.client = self._build_client()

    def _build_client(self):
        """构造 Redis 客户端。

        跨网络部署(Redis 在独立服务器)时, 只设 socket_connect_timeout 是不够的:
        它只管建连那一下。真正常见的故障是"半死连接" —— TCP 会话还在、对端已经
        没了(NAT 超时 / 链路切换 / 防火墙静默丢包), 此时 xreadgroup 会永久挂起,
        进程既不报错也不干活, 比崩溃更难发现。socket_timeout + keepalive +
        health_check_interval 三件套才能让这种故障变成一个可捕获的异常。

        socket_timeout 必须大于 BLOCK 时长, 否则阻塞读每次都被自己的超时掐断。
        """
        block_sec = self.config.block_ms / 1000.0
        return self._redis_module.Redis(
            host=self.config.host,
            port=self.config.port,
            password=self.config.password,
            decode_responses=True,
            socket_connect_timeout=self.config.socket_connect_timeout_sec,
            socket_timeout=block_sec + self.config.socket_timeout_margin_sec,
            socket_keepalive=True,
            health_check_interval=self.config.health_check_interval_sec,
            retry_on_timeout=True,
        )

    def _fatal_reason(self, exc: Exception) -> str | None:
        """区分"重试永远好不了的配置错误"与"重试就能恢复的网络故障"。

        口令写错、REDIS_PASSWORD 环境变量没设、Redis 版本没有 Stream —— 这些
        退避重连一万次也不会自愈, 而进程照常打出 🟢 启动 banner, 看着像在跑却
        永远收不到信号, 直到收盘才发现当天一单没跟。这类必须立刻抛出去。

        反过来, "Redis 服务还没起来 / 链路抖动" 必须继续重试: Windows 上跟单
        服务可能比 Redis 服务先启动, 一次启动竞态不该让当天的跟单起不来。
        """
        exceptions = getattr(self._redis_module, "exceptions", None)
        auth_error = getattr(exceptions, "AuthenticationError", None)
        if auth_error is not None and isinstance(exc, auth_error):
            return f"Redis 认证失败(检查 REDIS_PASSWORD 环境变量) | {exc}"
        text = str(exc).upper()
        for token, reason in (
            ("NOAUTH", "Redis 要求口令, 但本地没有配置口令"),
            ("WRONGPASS", "Redis 口令不正确"),
            ("INVALID PASSWORD", "Redis 口令不正确"),
            ("WITHOUT ANY PASSWORD", "Redis 未设口令, 但本地配了口令"),
            ("UNKNOWN COMMAND", "该 Redis 不支持 Stream 命令(需要 5.0+)"),
        ):
            if token in text:
                return f"{reason} | {exc}"
        return None

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
        """写入一条信号到 Stream。主要用于本地测试或工具脚本。"""
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
        active_message_ids_provider: Callable[[], set[str]] | None = None,
    ) -> Iterator[StreamMessage | None]:
        """持续消费新消息。

        使用 XREADGROUP 的 ">" 只读取当前消费者组尚未投递的新消息。

        stop_event 用于优雅退出: 当 event 被 set 时，内部循环会立即退出，
        不再阻塞在 xreadgroup 上。调用方应在信号处理器中 set 该 event。

        没有新消息时也会 yield None（每次轮询一次, 间隔 block_ms）, 让调用方
        有机会做周期性维护。注意 ACK 已经由完成回调驱动, 不再依赖这里的节奏,
        所以 block_ms 可以放心调大 —— Redis 的阻塞读是服务端推送, BLOCK 调大
        不增加收信延迟, 只减少空轮询(跨公网时这是实打实的流量和连接开销)。

        网络异常不再向上抛出: 独立 Redis 服务器意味着链路故障是常态而非意外,
        一次抖动不该让当天的跟单直接收工。捕获后按退避重连, 恢复即继续消费。
        """
        effective_block_ms = self.config.block_ms if block_ms is None else block_ms
        attempt = 0
        connected = False
        next_pending_scan = 0.0
        while True:
            if stop_event and stop_event.is_set():
                logger.debug("🛑 read_forever 收到停止信号，退出内部循环")
                return
            try:
                if not connected:
                    self.ensure_group()
                    connected = True
                    if attempt:
                        logger.info("【Redis】🔄 连接已恢复 | 第 %s 次重试后重新消费", attempt)
                    attempt = 0
                now = time.monotonic()
                if (
                    active_message_ids_provider is not None
                    and now >= next_pending_scan
                ):
                    active_ids = active_message_ids_provider()
                    # XPENDING 是从最旧消息开始取固定条数。若前面
                    # 全是正在执行的消息，只查 count 条会让后面的失败
                    # 消息永远露不出来，因此查询窗口要包住已排除数。
                    pending_scan_count = max(count, len(active_ids) + 100)
                    # 先立即捡回本 consumer 自己留下的消息。consumer 名在每台
                    # 交易机上是稳定配置，进程重启后仍相同，因此无需白等 60 秒。
                    owned = self.claim_stale_pending(
                        exclude_message_ids=active_ids,
                        count=pending_scan_count,
                        current_consumer_only=True,
                    )
                    owned_ids = {message.message_id for message in owned}
                    # 再接管同一账号组内由旧 consumer 留下、已经长期无人处理的
                    # 消息。独立账号使用独立 group，不会把另一账号的消息拿走。
                    stale = self.claim_stale_pending(
                        exclude_message_ids=active_ids | owned_ids,
                        count=pending_scan_count + len(owned_ids),
                    )
                    next_pending_scan = now + self.config.pending_scan_interval_sec
                    for recovered_message in (*owned, *stale):
                        yield recovered_message
                response = self.client.xreadgroup(
                    self.config.group,
                    self.config.consumer,
                    {self.config.stream: ">"},
                    count=count,
                    block=effective_block_ms,
                )
            except Exception as exc:
                connected = False
                fatal = self._fatal_reason(exc)
                if fatal is not None:
                    # 退避重连救不了配置错误, 只会把它藏成一行行看起来像网络
                    # 抖动的日志。直接抛出去让服务起不来, 运维一眼就能看见。
                    logger.critical("【Redis】🛑 配置错误, 重连不会自愈 | %s", fatal)
                    raise
                delay = _RECONNECT_BACKOFF_SEC[min(attempt, len(_RECONNECT_BACKOFF_SEC) - 1)]
                attempt += 1
                logger.error(
                    "【Redis】❌ 连接异常 | 第 %s 次重试将在 %.1fs 后 | %s",
                    attempt, delay, exc,
                )
                try:
                    self.client.close()
                except Exception:
                    pass
                self.client = self._build_client()
                if stop_event is not None:
                    if stop_event.wait(delay):
                        return
                else:
                    time.sleep(delay)
                yield None
                continue

            if not response:
                yield None
                continue
            for _, messages in response:
                for message_id, fields in messages:
                    yield _parse_message(message_id, fields)

    def ack(self, message_id: str) -> None:
        """确认消息已处理。

        当前设计是在执行引擎进入终态后再 XACK, 这样程序中途崩溃时消息仍可恢复处理。
        ACK 失败只告警: 消息会留在 pending 里, 比让整个循环崩掉安全。
        """
        try:
            self.client.xack(self.config.stream, self.config.group, message_id)
            logger.debug("✅ Redis消息已确认 | msg_id=%s", message_id)
        except Exception as exc:
            logger.error("【Redis】❌ 消息确认失败 | msg_id=%s | %s", message_id, exc)

    def claim_stale_pending(
        self,
        *,
        exclude_message_ids: set[str] | None = None,
        count: int = 100,
        current_consumer_only: bool = False,
    ) -> list[StreamMessage]:
        """接管本消费组中长期未确认的消息；正在本进程执行的消息必须排除。"""
        excluded = exclude_message_ids or set()
        idle_ms = 0 if current_consumer_only else self.config.pending_claim_idle_ms
        pending_kwargs = {
            "min": "-",
            "max": "+",
            "count": count,
        }
        if current_consumer_only:
            pending_kwargs["consumername"] = self.config.consumer
        pending = self.client.xpending_range(
            self.config.stream,
            self.config.group,
            **pending_kwargs,
        )
        message_ids = []
        for entry in pending:
            if isinstance(entry, dict):
                message_id = entry.get("message_id")
                delivered_idle_ms = int(entry.get("time_since_delivered", 0))
            else:
                message_id = entry[0]
                delivered_idle_ms = int(entry[2])
            message_id = str(message_id)
            if (
                message_id not in excluded
                and (current_consumer_only or delivered_idle_ms >= idle_ms)
            ):
                message_ids.append(message_id)
        if not message_ids:
            return []
        claimed = self.client.xclaim(
            self.config.stream,
            self.config.group,
            self.config.consumer,
            idle_ms,
            message_ids,
        )
        recovered = [
            replace(_parse_message(message_id, fields), recovered=True)
            for message_id, fields in claimed
        ]
        if recovered:
            logger.warning(
                "【Redis】🔄 已接管遗留消息 | group=%s | %s条",
                self.config.group, len(recovered),
            )
        return recovered


def _parse_message(message_id: str, fields: dict[str, str]) -> StreamMessage:
    """把一条 Stream entry 解析为交易信号、预订阅指令或计划。

    任何解析失败都收敛成 rejected 消息而不是抛异常: 这个生成器驱动着整个消费
    循环, 一条畸形消息不能把当天的跟单打掉。Stream 是多策略共享的, 别的策略
    换个 schema 就可能产生本执行端不认识的消息。
    """
    try:
        raw_payload = fields.get("payload")
        if raw_payload is not None:
            raw = json.loads(raw_payload)
        else:
            raw = fields
        if not isinstance(raw, dict):
            return StreamMessage(message_id=message_id, rejected="payload 不是 JSON 对象")

        action = str(raw.get("action", "")).lower()
        if action == "subscribe":
            raw_codes = raw.get("codes", [])
            if not isinstance(raw_codes, (list, tuple)):
                raise ValueError("codes 必须是列表")
            watchlist = WatchlistCommand(
                codes=tuple(str(code) for code in raw_codes),
                strategy_id=str(raw.get("strategy_id", "")),
                mode=str(raw.get("mode", "live")),
            )
            logger.debug("📨 预订阅指令已解析 | msg_id=%s 数量=%s", message_id, len(watchlist.codes))
            return StreamMessage(message_id=message_id, watchlist=watchlist)

        if action == "candidate_plan":
            candidate_plan = CandidatePlan.from_dict(raw)
            logger.debug(
                "📨 候选计划已解析 | msg_id=%s plan_id=%s 数量=%s",
                message_id,
                candidate_plan.plan_id,
                len(candidate_plan.candidates),
            )
            return StreamMessage(message_id=message_id, candidate_plan=candidate_plan)

        if action == "plan":
            plan = DailyPlan.from_dict(raw)
            logger.debug("📨 日计划已解析 | msg_id=%s plan_id=%s", message_id, plan.signal_id)
            return StreamMessage(message_id=message_id, plan=plan)

        signal = TradeSignal.from_dict(raw)
        logger.debug("📨 Redis消息已解析 | msg_id=%s signal_id=%s", message_id, signal.signal_id)
        return StreamMessage(message_id=message_id, signal=signal)
    except Exception as exc:
        return StreamMessage(
            message_id=message_id, rejected=f"{type(exc).__name__}: {exc}",
        )


def _parse_signal(fields: dict[str, str]) -> TradeSignal:
    """兼容两种格式: payload JSON 字符串, 或直接把字段写在 Stream entry 里。"""
    if "payload" in fields:
        raw = json.loads(fields["payload"])
    else:
        raw = fields
    return TradeSignal.from_dict(raw)

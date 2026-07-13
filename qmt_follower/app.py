"""QMT 跟单助手 —— 应用组装入口。

负责 DI 依赖注入: 配置 → Redis → SQLite → 行情/交易适配器 → 执行引擎 → 主循环。
Redis 收信和行情预订阅保持响应，真实交易信号由单工作线程按 FIFO 顺序执行。
"""

from __future__ import annotations

import argparse
import datetime as dt
import logging
import signal as _signal
import threading
import time
from concurrent.futures import Future, ThreadPoolExecutor, as_completed

from qmt_follower.adapters.qmt import QmtBrokerAdapter, QmtMarketDataAdapter
from qmt_follower.config import load_config
from qmt_follower.executor import OrderExecutionEngine
from qmt_follower.logging_config import setup_logging
from qmt_follower.models import ExecutionResult
from qmt_follower.redis_stream import RedisStreamClient, StreamMessage
from qmt_follower.store import SQLiteExecutionStore

logger = logging.getLogger(__name__)

TRADE_EXECUTION_WORKERS = 1
ScheduledTrade = tuple[dt.datetime, str, StreamMessage]

# 优雅退出事件: 信号处理器 set, read_forever 内部循环检查并退出。
_shutdown_event = threading.Event()


def _on_shutdown(signum, _frame):
    _shutdown_event.set()
    logger.info("【系统】🛑 收到退出信号 %s | 正在结束消费循环", signum)


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="QMT跟单助手 —— Redis Stream → miniQMT 实盘跟单")
    parser.add_argument("--config", default="config.yaml", help="YAML 配置文件路径")
    parser.add_argument(
        "--workers",
        type=int,
        choices=(TRADE_EXECUTION_WORKERS,),
        default=TRADE_EXECUTION_WORKERS,
        help="交易执行线程数，固定为 1 以保证全账户 FIFO",
    )
    return parser.parse_args(argv)


def main() -> None:
    """后端服务入口。

    把配置、Redis、SQLite、行情适配器、券商适配器组装起来，
    然后用单工作线程按 FIFO 顺序处理交易信号。
    """
    args = _parse_args()

    config = load_config(args.config)

    # 初始化日志系统: 控制台 + 文件双输出
    setup_logging(level=config.log_level, log_dir=config.log_dir)
    logger.info("【系统】🚀 QMT跟单助手启动中 | 交易线程 %s | FIFO", args.workers)

    # Redis 负责收信号; SQLite 负责记录本机执行状态; Engine 负责下单状态机。
    stream = RedisStreamClient(config.redis)
    store = SQLiteExecutionStore(config.state_db)
    market_data = QmtMarketDataAdapter(pre_subscribe_codes=config.market_data.pre_subscribe_codes)
    engine = OrderExecutionEngine(
        store=store,
        market_data=market_data,
        broker=QmtBrokerAdapter(config.trading),
        config=config.execution,
    )

    # 注册优雅退出
    _signal.signal(_signal.SIGINT, _on_shutdown)
    _signal.signal(_signal.SIGTERM, _on_shutdown)

    logger.info(
        "【系统】🟢 Redis监听已启动 | stream=%s | group=%s | consumer=%s",
        config.redis.stream, config.redis.group, config.redis.consumer,
    )

    with ThreadPoolExecutor(max_workers=TRADE_EXECUTION_WORKERS, thread_name_prefix="qmt-worker") as pool:
        pending: dict[Future[ExecutionResult], StreamMessage] = {}
        scheduled: list[ScheduledTrade] = []

        try:
            for message in stream.read_forever(stop_event=_shutdown_event):
                if _shutdown_event.is_set():
                    break

                # 清理已完成的任务并 ACK。即使本轮轮询没有新消息(message 为 None)
                # 也要执行, 否则已执行完的信号只能等到"下一条信号到达"才会被
                # 记终态日志和 ACK —— 行情安静时会造成日志时间线严重失真。
                _reap_completed(pending, stream)
                _submit_due_scheduled(
                    scheduled, pending, pool, engine, dt.datetime.now(),
                )

                if message is None:
                    continue

                # 预订阅指令: 只订阅行情, 不进执行引擎。
                if message.watchlist is not None:
                    logger.info(
                        "【行情】📡 预订阅 | 策略=%s | %s只 | %s",
                        message.watchlist.strategy_id,
                        len(message.watchlist.codes),
                        ",".join(message.watchlist.codes),
                    )
                    try:
                        market_data.subscribe(message.watchlist.codes)
                    except Exception as exc:
                        logger.exception(
                            "【行情】❌ 预订阅失败 | 策略=%s | %s",
                            message.watchlist.strategy_id, exc,
                        )
                    stream.ack(message.message_id)
                    continue

                sig = message.signal
                logger.debug("📨 收到信号原始ID | signal_id=%s", sig.signal_id)
                logger.info(
                    "%s | %s | %s股 | 参考 %.3f | 策略=%s | 前方 %s单%s",
                    sig.console_prefix, sig.code.split(".", 1)[0], sig.amount,
                    sig.reference_price, sig.strategy_id, len(pending),
                    _transport_latency_label(sig.sent_at_ms),
                )

                try:
                    _queue_or_submit_trade(
                        message, scheduled, pending, pool, engine, dt.datetime.now(),
                    )
                except ValueError as exc:
                    logger.exception(
                        "%s | %s | 预约时间非法 | %s",
                        sig.console_event("失败"), sig.code.split(".", 1)[0], exc,
                    )

        finally:
            if _shutdown_event.is_set():
                logger.info("【系统】⏳ 等待 %s 个进行中的任务完成", len(pending))
            else:
                logger.info("【系统】⏳ 消费循环退出 | 等待 %s 个任务完成", len(pending))
            if scheduled:
                logger.warning("【系统】⏸️ 保留 %s 个未到期预约信号 | 未提前执行或ACK", len(scheduled))
            # 等待所有进行中的任务
            for future in as_completed(pending):
                _reap_one(future, pending, stream)
            # 关闭各线程的数据库连接
            store.close()
            logger.info("【系统】👋 QMT跟单助手已退出")


def _execute_safe(engine: OrderExecutionEngine, signal) -> ExecutionResult:
    """在线程中安全执行信号，捕获异常防止线程崩溃。"""
    try:
        return engine.execute(signal)
    except Exception as exc:
        logger.exception(
            "%s | %s | 未处理异常 | %s",
            signal.console_event("失败"), signal.code.split(".", 1)[0], exc,
        )
        # 返回一个失败结果让上层能 ACK
        from qmt_follower.models import ExecutionStatus
        return ExecutionResult(
            signal_id=signal.signal_id,
            status=ExecutionStatus.FAILED_BROKER,
            requested_qty=signal.amount,
            filled_qty=0,
            attempts=0,
            message="unhandled exception in worker thread",
        )


def _queue_or_submit_trade(
    message: StreamMessage,
    scheduled: list[ScheduledTrade],
    pending: dict[Future[ExecutionResult], StreamMessage],
    pool: ThreadPoolExecutor,
    engine: OrderExecutionEngine,
    now: dt.datetime,
) -> None:
    """预约信号留在应用层等待，即时信号直接进入单线程 FIFO。"""
    signal = message.signal
    if signal.execute_at:
        execute_at = dt.datetime.strptime(signal.execute_at, "%Y-%m-%d %H:%M:%S")
        if execute_at > now:
            scheduled.append((execute_at, message.message_id, message))
            scheduled.sort(key=lambda item: (item[0], item[1]))
            logger.info(
                "%s | %s | 预约 %s | 等待 %.0fms",
                signal.console_event("等待"), signal.code.split(".", 1)[0],
                signal.execute_at, (execute_at - now).total_seconds() * 1000,
            )
            return
    _submit_trade(message, pending, pool, engine)


def _submit_due_scheduled(
    scheduled: list[ScheduledTrade],
    pending: dict[Future[ExecutionResult], StreamMessage],
    pool: ThreadPoolExecutor,
    engine: OrderExecutionEngine,
    now: dt.datetime,
) -> None:
    """按预约时间和 Redis 消息 ID 的稳定顺序释放到期信号。"""
    while scheduled and scheduled[0][0] <= now:
        execute_at, _, message = scheduled.pop(0)
        signal = message.signal
        logger.info(
            "%s | %s | 预约到期 | 调度偏差 %.0fms",
            signal.console_event("执行"), signal.code.split(".", 1)[0],
            (now - execute_at).total_seconds() * 1000,
        )
        _submit_trade(message, pending, pool, engine)


def _submit_trade(
    message: StreamMessage,
    pending: dict[Future[ExecutionResult], StreamMessage],
    pool: ThreadPoolExecutor,
    engine: OrderExecutionEngine,
) -> None:
    """把交易信号提交给现有单工作线程。"""
    future = pool.submit(_execute_safe, engine, message.signal)
    pending[future] = message


def _reap_completed(
    pending: dict[Future[ExecutionResult], StreamMessage],
    stream: RedisStreamClient,
) -> None:
    """清理所有已完成的任务，ACK 对应的 Redis 消息。"""
    done = [f for f in pending if f.done()]
    for future in done:
        _reap_one(future, pending, stream)


def _reap_one(
    future: Future[ExecutionResult],
    pending: dict[Future[ExecutionResult], StreamMessage],
    stream: RedisStreamClient,
) -> None:
    """处理单个已完成任务：ACK + 日志。"""
    message = pending.pop(future)
    try:
        result = future.result()
        logger.debug(
            "🏁 信号处理完成 | %s 状态=%s 成交=%s/%s股 尝试=%s次%s",
            message.signal.label,
            result.status.value,
            result.filled_qty,
            result.requested_qty,
            result.attempts,
            _end_to_end_latency_label(message.signal.sent_at_ms),
        )
    except Exception as exc:
        logger.exception(
            "%s | %s | 结果处理异常 | %s",
            message.signal.console_event("失败"), message.signal.code.split(".", 1)[0], exc,
        )

    # 只有执行进入终态后才确认 Redis 消息, 避免处理中崩溃导致消息丢失。
    stream.ack(message.message_id)


def _transport_latency_label(sent_at_ms: int | None) -> str:
    """聚宽发出 → 本机收到的传输延迟标签。依赖两端 NTP 时间同步, 仅供量级参考。"""
    if sent_at_ms is None:
        return ""
    return " | 传输 %.0fms" % (time.time() * 1000 - sent_at_ms)


def _end_to_end_latency_label(sent_at_ms: int | None) -> str:
    """聚宽发出 → 执行终态的端到端延迟标签。"""
    if sent_at_ms is None:
        return ""
    return " | 端到端 %.0fms" % (time.time() * 1000 - sent_at_ms)


if __name__ == "__main__":
    main()

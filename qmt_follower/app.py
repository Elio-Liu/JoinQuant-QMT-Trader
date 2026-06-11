"""QMT 跟单助手 —— 应用组装入口。

负责 DI 依赖注入: 配置 → Redis → SQLite → 行情/交易适配器 → 执行引擎 → 主循环。
使用线程池并行处理信号: 一个信号在轮询等待成交时，其他信号可并行执行。
"""

from __future__ import annotations

import argparse
import logging
import signal as _signal
import threading
from concurrent.futures import Future, ThreadPoolExecutor, as_completed

from qmt_follower.adapters.qmt import QmtBrokerAdapter, QmtMarketDataAdapter
from qmt_follower.config import load_config
from qmt_follower.executor import OrderExecutionEngine
from qmt_follower.logging_config import setup_logging
from qmt_follower.models import ExecutionResult
from qmt_follower.redis_stream import RedisStreamClient, StreamMessage
from qmt_follower.store import SQLiteExecutionStore

logger = logging.getLogger(__name__)

# 优雅退出事件: 信号处理器 set, read_forever 内部循环检查并退出。
_shutdown_event = threading.Event()


def _on_shutdown(signum, _frame):
    _shutdown_event.set()
    logger.info("🛑 收到退出信号 (signal=%s)，通知消费循环停止...", signum)


def main() -> None:
    """后端服务入口。

    把配置、Redis、SQLite、行情适配器、券商适配器组装起来，
    然后用线程池并行处理信号。
    """
    parser = argparse.ArgumentParser(description="QMT跟单助手 —— Redis Stream → miniQMT 实盘跟单")
    parser.add_argument("--config", default="config.json", help="JSON 配置文件路径")
    parser.add_argument("--workers", type=int, default=4, help="并行处理信号的最大线程数 (默认 4)")
    args = parser.parse_args()

    config = load_config(args.config)

    # 初始化日志系统: 控制台 + 文件双输出
    setup_logging(level=config.log_level, log_dir=config.log_dir)
    logger.info("🚀 QMT跟单助手启动 | 日志目录=%s | 并行线程数=%s", config.log_dir, args.workers)

    # Redis 负责收信号; SQLite 负责记录本机执行状态; Engine 负责下单状态机。
    stream = RedisStreamClient(config.redis)
    store = SQLiteExecutionStore(config.state_db)
    engine = OrderExecutionEngine(
        store=store,
        market_data=QmtMarketDataAdapter(),
        broker=QmtBrokerAdapter(config.trading),
        config=config.execution,
    )

    # 注册优雅退出
    _signal.signal(_signal.SIGINT, _on_shutdown)
    _signal.signal(_signal.SIGTERM, _on_shutdown)

    logger.info("🎧 开始监听 Redis Stream | stream=%s group=%s consumer=%s",
                 config.redis.stream, config.redis.group, config.redis.consumer)

    with ThreadPoolExecutor(max_workers=args.workers, thread_name_prefix="qmt-worker") as pool:
        pending: dict[Future[ExecutionResult], StreamMessage] = {}

        try:
            for message in stream.read_forever(stop_event=_shutdown_event):
                if _shutdown_event.is_set():
                    break

                # 清理已完成的任务并 ACK
                _reap_completed(pending, stream)

                sig = message.signal
                logger.info(
                    "📥 收到交易信号 | signal_id=%s 策略=%s 代码=%s 方向=%s 数量=%s 参考价=%.2f",
                    sig.signal_id, sig.strategy_id, sig.code, sig.action.value, sig.amount, sig.reference_price,
                )

                # 提交到线程池并行执行
                future = pool.submit(_execute_safe, engine, sig)
                pending[future] = message

        finally:
            if _shutdown_event.is_set():
                logger.info("🛑 等待 %s 个进行中的任务完成...", len(pending))
            else:
                logger.info("🛑 主循环退出，等待 %s 个进行中的任务完成...", len(pending))
            # 等待所有进行中的任务
            for future in as_completed(pending):
                _reap_one(future, pending, stream)
            # 关闭各线程的数据库连接
            store.close()
            logger.info("👋 QMT跟单助手已退出")


def _execute_safe(engine: OrderExecutionEngine, signal) -> ExecutionResult:
    """在线程中安全执行信号，捕获异常防止线程崩溃。"""
    try:
        return engine.execute(signal)
    except Exception:
        logger.exception("💥 信号执行异常 | signal_id=%s", signal.signal_id)
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
        logger.info(
            "🏁 信号处理完成 | signal_id=%s 状态=%s 成交=%s/%s股 尝试=%s次",
            result.signal_id,
            result.status.value,
            result.filled_qty,
            result.requested_qty,
            result.attempts,
        )
    except Exception:
        logger.exception("💥 信号处理异常 | signal_id=%s", message.signal.signal_id)

    # 只有执行进入终态后才确认 Redis 消息, 避免处理中崩溃导致消息丢失。
    stream.ack(message.message_id)


if __name__ == "__main__":
    main()

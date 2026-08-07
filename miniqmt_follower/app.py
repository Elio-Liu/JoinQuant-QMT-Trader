"""QMT 跟单助手 —— 应用组装入口。

负责 DI 依赖注入: 配置 → Redis → SQLite → 行情/交易适配器 → 执行引擎 → 主循环。
Redis 收信和行情预订阅保持响应; 交易信号按买卖方向进独立线程池并发执行:
- 09:25~09:30 只预挂卖单; 09:30 买单等待普通盘前卖单完成或明确终止;
- 池内并发 (--workers, 默认 8): 多只盘前卖单必须并行提交才能及时排队，
  买单过闸后也并发抢单。涨跌停排队单会占用 worker 直到成交或截止，故每侧
  需给普通信号预留 worker。
  同一 signal_id 由 SQLite 幂等去重兜底, 跨代码乱序无业务影响。
"""

from __future__ import annotations

import argparse
import logging
import signal as _signal
import threading
import time
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from dataclasses import replace
from typing import Any

from miniqmt_follower.adapters.qmt import QmtBrokerAdapter, QmtMarketDataAdapter
from miniqmt_follower.config import load_config
from miniqmt_follower.executor import OrderExecutionEngine
from miniqmt_follower.logging_config import setup_logging
from miniqmt_follower.models import Action, ExecutionResult, ExecutionStatus, TradeSignal
from miniqmt_follower.opening import OpeningSellBarrier, seconds_until_market_open
from miniqmt_follower.plan_executor import PlanExecutor, submit_plan_tasks
from miniqmt_follower.redis_stream import RedisStreamClient, StreamMessage
from miniqmt_follower.store import SQLiteExecutionStore

logger = logging.getLogger(__name__)

TRADE_EXECUTION_WORKERS = 8
TradePools = dict[Action, ThreadPoolExecutor]

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
        default=TRADE_EXECUTION_WORKERS,
        help="每个买卖方向的交易执行线程数 (默认 %(default)s)。开盘多票抢单与"
        "盘中多票止损需要并行, 至少要 ≥ 策略单日目标股数(harvester 为 5), 否则"
        "第 5 只买单要等前面的 worker 空出来才提交, 错过开盘排位; "
        "涨跌停排队模式还需给普通信号至少预留 2 个 worker",
    )
    args = parser.parse_args(argv)
    if args.workers < 1:
        parser.error("--workers 必须 ≥ 1")
    return args


def main() -> None:
    """后端服务入口。

    把配置、Redis、SQLite、行情适配器、券商适配器组装起来，
    然后用买入、卖出两个独立线程池 (各 --workers 线程) 并发处理交易信号。
    """
    args = _parse_args()

    config = load_config(args.config)

    # 初始化日志系统: 控制台 + 文件双输出
    setup_logging(level=config.log_level, log_dir=config.log_dir)
    logger.info("【系统】🚀 QMT跟单助手启动中 | 买卖各 %s 线程并发", args.workers)
    allowed_strategies = set(config.redis.allowed_strategy_ids)
    if allowed_strategies:
        logger.info("【系统】🛂 策略白名单已启用 | %s", ",".join(sorted(allowed_strategies)))

    # Redis 负责收信号; SQLite 负责记录本机执行状态; Engine 负责下单状态机。
    stream = RedisStreamClient(config.redis)
    store = SQLiteExecutionStore(config.state_db)
    market_data = QmtMarketDataAdapter(pre_subscribe_codes=config.market_data.pre_subscribe_codes)
    opening_barrier = OpeningSellBarrier()
    engine = OrderExecutionEngine(
        store=store,
        market_data=market_data,
        broker=QmtBrokerAdapter(config.trading),
        config=config.execution,
        on_limit_down_queued=opening_barrier.release_all,
    )
    plan_executor = PlanExecutor(store=store, broker=engine.broker)

    # 注册优雅退出
    _signal.signal(_signal.SIGINT, _on_shutdown)
    _signal.signal(_signal.SIGTERM, _on_shutdown)
    # Windows 下 supervisor 用 CTRL_BREAK 定向通知本进程优雅退出（对端收到的是 SIGBREAK）
    _sigbreak = getattr(_signal, "SIGBREAK", None)
    if _sigbreak is not None:
        _signal.signal(_sigbreak, _on_shutdown)

    # 启动 banner: 多机部署时配错 group/consumer 是最常见也最隐蔽的事故 ——
    # 同 group 会让两台机器瓜分消息(每台只买到一部分), 而日志里没有任何异常。
    # 把身份信息打出来, 配错一眼能看出来。
    logger.info(
        "【系统】🟢 Redis监听已启动 | stream=%s | group=%s | consumer=%s | "
        "账户=%s | 账本=%s",
        config.redis.stream, config.redis.group, config.redis.consumer,
        config.trading.account_id, config.state_db,
    )
    # 排队单会一直占着 worker 直到成交或截止, 必须给普通信号留出线程,
    # 否则盘中止损会排在涨跌停排队单后面进不去。这个不变量原先只写在注释里。
    for label, need in (
        ("卖出", config.execution.max_concurrent_queue_sells),
        ("买入", config.execution.max_concurrent_queue_buys),
    ):
        if args.workers < need + 2:
            logger.warning(
                "【系统】⚠️ %s线程数不足 | --workers=%s < 排队上限%s + 预留2 | "
                "排队单占满后普通信号会被饿死",
                label, args.workers, need,
            )

    with (
        ThreadPoolExecutor(
            max_workers=args.workers, thread_name_prefix="qmt-sell"
        ) as sell_pool,
        ThreadPoolExecutor(
            max_workers=args.workers, thread_name_prefix="qmt-buy"
        ) as buy_pool,
    ):
        pools = {Action.SELL: sell_pool, Action.BUY: buy_pool}
        pending: dict[Future[Any], StreamMessage] = {}

        try:
            for message in stream.read_forever(stop_event=_shutdown_event):
                if _shutdown_event.is_set():
                    break

                # 兜底收割: ACK 主要由任务完成回调驱动(见 _submit_trade),
                # 这里只是防止回调因异常漏掉。因为不再靠它保证 ACK 及时性,
                # block_ms 可以放心调大 —— Redis 阻塞读是服务端推送, BLOCK
                # 调大不增加收信延迟, 只减少跨网络的空轮询。
                _reap_completed(pending, stream)

                if message is None:
                    continue

                # 无法解析的消息: 记日志后直接 ACK, 绝不让它卡住消费组, 也绝不
                # 让它把消费循环打崩。Stream 是多策略共享的, 别的策略换个 schema
                # 就可能产生本执行端不认识的消息。
                if message.rejected is not None:
                    logger.warning(
                        "【Redis】⛔ 消息已丢弃 | msg_id=%s | %s",
                        message.message_id, message.rejected,
                    )
                    stream.ack(message.message_id)
                    continue

                # 日计划: 展开为清仓/待买派生信号, 全部执行完才 ACK。
                if message.plan is not None:
                    if not config.execution.plan_enabled:
                        logger.warning(
                            "【计划】🚫 日计划执行已关闭 | plan_enabled=false | %s",
                            message.plan.signal_id,
                        )
                        stream.ack(message.message_id)
                        continue
                    if not _strategy_allowed(message.plan.strategy_id, allowed_strategies):
                        logger.info(
                            "【计划】🛂 日计划忽略 | 策略 %s 不在白名单",
                            message.plan.strategy_id,
                        )
                        stream.ack(message.message_id)
                        continue
                    if not plan_executor.record(message.plan):
                        logger.info(
                            "【计划】⏭️ 重复日计划 | %s", message.plan.signal_id,
                        )
                    combined = submit_plan_tasks(
                        message.plan,
                        plan_executor,
                        pools,
                        engine,
                        _execute_safe,
                        opening_barrier,
                    )
                    if combined is None:
                        logger.info(
                            "【计划】📋 日计划无可执行派生信号 | %s",
                            message.plan.signal_id,
                        )
                        stream.ack(message.message_id)
                    else:
                        pending[combined] = message
                    continue

                # 预订阅指令: 只订阅行情, 不进执行引擎。
                if message.watchlist is not None:
                    if not _strategy_allowed(message.watchlist.strategy_id, allowed_strategies):
                        logger.info(
                            "【行情】🛂 预订阅忽略 | 策略 %s 不在白名单",
                            message.watchlist.strategy_id or "<空>",
                        )
                        stream.ack(message.message_id)
                        continue
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
                # 日志展示用中文名: 收单时解析一次, 后续执行/终态日志共用同一名字。
                if not sig.stock_name:
                    sig = sig.with_stock_name(market_data.instrument_name(sig.code))
                    message = replace(message, signal=sig)
                if not _strategy_allowed(sig.strategy_id, allowed_strategies):
                    logger.warning(
                        "%s | %s | 策略 %s 不在白名单 | 忽略并ACK",
                        sig.console_event("跳过"), sig.display_code, sig.strategy_id,
                    )
                    stream.ack(message.message_id)
                    continue
                logger.debug("📨 收到信号原始ID | signal_id=%s", sig.signal_id)
                logger.info(
                    "%s | %s | %s股 | 参考 %.3f | 策略=%s | 前方 %s单%s",
                    sig.console_prefix, sig.display_code, sig.amount,
                    sig.reference_price, sig.strategy_id, len(pending),
                    _transport_latency_label(sig.sent_at_ms),
                )

                _submit_trade(message, pending, pools, engine, opening_barrier, stream)

        finally:
            if _shutdown_event.is_set():
                logger.info("【系统】⏳ 等待 %s 个进行中的任务完成", len(pending))
            else:
                logger.info("【系统】⏳ 消费循环退出 | 等待 %s 个任务完成", len(pending))
            # 等待所有进行中的任务。先快照: as_completed 会遍历传入的容器,
            # 而工作线程的完成回调正在并发 pop 同一个 pending。
            for future in as_completed(list(pending)):
                _reap_one(future, pending, stream)
            # 关闭各线程的数据库连接
            store.close()
            logger.info("【系统】👋 QMT跟单助手已退出")


def _strategy_allowed(strategy_id: str, allowed: set[str]) -> bool:
    """strategy_id 白名单判定; 白名单为空表示不过滤 (兼容旧配置)。"""
    return not allowed or strategy_id in allowed


def _execute_safe(
    engine: OrderExecutionEngine,
    signal: TradeSignal,
    opening_barrier: OpeningSellBarrier,
) -> ExecutionResult:
    """在线程中安全执行信号，捕获异常防止线程崩溃。"""
    try:
        if signal.action == Action.BUY:
            preopen_wait = seconds_until_market_open()
            if preopen_wait > 0:
                logger.info(
                    "%s | %s | 距开盘 %.0f秒 | 买单等待连续竞价",
                    signal.console_event("竞价"),
                    signal.display_code,
                    preopen_wait,
                )
                time.sleep(preopen_wait)
            blocked = opening_barrier.pending_count()
            if blocked:
                logger.info(
                    "%s | %s | 等待 %s 笔开盘卖单释放后再买入",
                    signal.console_event("竞价"),
                    signal.display_code,
                    blocked,
                )
            opening_barrier.wait_until_released()
        return engine.execute(signal)
    except Exception as exc:
        logger.exception(
            "%s | %s | 未处理异常 | %s",
            signal.console_event("失败"), signal.display_code, exc,
        )
        # 返回一个失败结果让上层能 ACK
        return ExecutionResult(
            signal_id=signal.signal_id,
            status=ExecutionStatus.FAILED_BROKER,
            requested_qty=signal.amount,
            filled_qty=0,
            attempts=0,
            message="unhandled exception in worker thread",
        )
    finally:
        if signal.action == Action.SELL:
            opening_barrier.release(signal.signal_id)


def _submit_trade(
    message: StreamMessage,
    pending: dict[Future[ExecutionResult], StreamMessage],
    pools: TradePools,
    engine: OrderExecutionEngine,
    opening_barrier: OpeningSellBarrier,
    stream: RedisStreamClient | None = None,
) -> None:
    """登记盘前卖单后，把交易信号提交给对应方向的并发工作池。

    传入 stream 时挂一个完成回调, 让 ACK 和终态日志在执行结束的那一刻发生,
    而不是等消费循环的下一次轮询 —— 这样 block_ms 就不再是 ACK 时效的瓶颈。
    """
    signal = message.signal
    opening_barrier.register(signal)
    pool = pools[signal.action]
    future = pool.submit(_execute_safe, engine, signal, opening_barrier)
    pending[future] = message
    if stream is not None:
        future.add_done_callback(
            lambda f: _reap_one_safe(f, pending, stream)
        )


def _reap_one_safe(
    future: Future[ExecutionResult],
    pending: dict[Future[ExecutionResult], StreamMessage],
    stream: RedisStreamClient,
) -> None:
    """完成回调入口: 在工作线程里跑, 绝不能抛 —— 抛了只会进 futures 的日志。"""
    try:
        _reap_one(future, pending, stream)
    except Exception as exc:
        logger.exception("【系统】❌ 完成回调异常 | %s", exc)


def _reap_completed(
    pending: dict[Future[ExecutionResult], StreamMessage],
    stream: RedisStreamClient,
) -> None:
    """清理所有已完成的任务，ACK 对应的 Redis 消息。

    必须先 list() 快照再遍历: 完成回调在工作线程里 pop 同一个 pending,
    直接遍历 dict 会撞上 "dictionary changed size during iteration" ——
    而这个异常会一路穿出没有 except 的消费循环, 让当天的跟单直接收工。
    """
    for future in list(pending):
        if future.done():
            _reap_one(future, pending, stream)


def _reap_one(
    future: Future[ExecutionResult],
    pending: dict[Future[ExecutionResult], StreamMessage],
    stream: RedisStreamClient,
) -> None:
    """处理单个已完成任务：ACK + 日志。

    完成回调、主循环兜底收割、退出等待三条路径会对同一条记录竞争, 由 pop 的
    原子性定胜负: 谁先取到 message 谁负责 ACK, 后到的直接返回。
    """
    message = pending.pop(future, None)
    if message is None:
        return
    if message.plan is not None:
        try:
            results = future.result()
            logger.info(
                "🏁 日计划执行完成 | %s | 派生 %s 条 | %s",
                message.plan.signal_id,
                len(results),
                ",".join(f"{r.signal_id}={r.status.value}" for r in results),
            )
        except Exception as exc:
            logger.exception(
                "【计划】❌ 日计划结果处理异常 | %s | %s",
                message.plan.signal_id, exc,
            )
        stream.ack(message.message_id)
        return
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
            message.signal.console_event("失败"), message.signal.display_code, exc,
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

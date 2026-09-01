"""QMT 跟单助手 —— 应用组装入口。

负责 DI 依赖注入: 配置 → Redis → SQLite → 行情/交易适配器 → 执行引擎 → 主循环。
Redis 收信和行情预订阅保持响应; 交易信号按买卖方向进独立线程池并发执行:
- 盘前卖出窗口只预挂卖单; 连续竞价开始时买单等待普通盘前卖单完成或明确终止;
- 池内并发 (--workers, 默认 8): 多只盘前卖单必须并行提交才能及时排队，
  买单过闸后也并发抢单。涨跌停排队单(买卖两侧)挂单落库后移交 qmt-queue
  专用线程池慢轮询, 不占用买卖 worker; 排队消息在途期间由 _MessageIdRegistry
  登记, Redis pending 扫描不会重投。
  同一 signal_id 由 SQLite 幂等去重兜底, 跨代码乱序无业务影响。
"""

from __future__ import annotations

import argparse
import atexit
import datetime as dt
import logging
import signal as _signal
import threading
import time
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from dataclasses import replace
from typing import Any, Sequence

from miniqmt_follower.adapters.qmt import QmtBrokerAdapter, QmtMarketDataAdapter
from miniqmt_follower.config import RuntimeConfig
from miniqmt_follower.executor import OrderExecutionEngine
from miniqmt_follower.logging_config import setup_logging
from miniqmt_follower.models import (
    Action,
    ExecutionResult,
    ExecutionStatus,
    TradeSignal,
    format_stock_label,
    is_terminal_execution_status,
)
from miniqmt_follower.opening import OpeningSellBarrier, seconds_until_market_open
from miniqmt_follower.plan_executor import PlanExecutor, submit_plan_tasks
from miniqmt_follower.process_lock import SingleInstanceLock
from miniqmt_follower.redis_stream import RedisStreamClient, StreamMessage
from miniqmt_follower.strategy_config import load_runtime_with_strategy
from miniqmt_follower.strategy_engine import LocalStrategyEngine
from miniqmt_follower.strategy_models import CandidatePlanStatus
from miniqmt_follower.store import (
    CandidatePlanConflict,
    PlanPayloadConflict,
    SQLiteExecutionStore,
)

logger = logging.getLogger(__name__)

TRADE_EXECUTION_WORKERS = 8
TradePools = dict[Action, ThreadPoolExecutor]

# 优雅退出事件: 信号处理器 set, read_forever 内部循环检查并退出。
_shutdown_event = threading.Event()


def _local_today() -> dt.date:
    """交易机本地日期; 独立成函数便于测试替换。"""
    return dt.date.today()


def _message_trade_date(value: Any) -> dt.date | None:
    """从信号/计划对象推导交易日: sent_at_ms(本地时区)优先, 其次 created_at 前缀。

    隔天开机的机器会把消费组里昨天未读的消息当新消息重放 —— 日期早于今天的
    消息必须干净忽略(ACK 不执行), 而不是落库污染或靠信号级 600s 过期兜底
    (旧协议消息没有 sent_at_ms 时会真的执行)。推导不出返回 None: 门控跳过,
    交给既有过期/拒收路径, 不误杀任何旧协议消息。
    """
    sent_at_ms = getattr(value, "sent_at_ms", None)
    if sent_at_ms:
        try:
            return dt.datetime.fromtimestamp(sent_at_ms / 1000.0).date()
        except (OSError, OverflowError, ValueError):
            pass
    created = str(getattr(value, "created_at", "") or "")
    if len(created) >= 10 and created[4] == "-" and created[7] == "-":
        try:
            return dt.date.fromisoformat(created[:10])
        except ValueError:
            pass
    return None


class _MessageIdRegistry:
    """在途消息 id 注册表: pending 扫描必须跳过"已移交排队/后台展开中"的消息。

    排队单移交专用线程后消息不再 ACK、也不在 pending 字典里, 若不在注册表中,
    每 5 秒的 pending 扫描会用 idle_ms=0 把同一消息反复接管重投, 重投走
    recover() 会以排队截止为限阻塞卖出 worker, 数十秒内占满整个卖出池。
    snapshot() 返回副本, 避免迭代期间被并发修改抛 RuntimeError。
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._ids: set[str] = set()

    def add(self, message_id: str) -> None:
        with self._lock:
            self._ids.add(message_id)

    def discard(self, message_id: str) -> None:
        with self._lock:
            self._ids.discard(message_id)

    def snapshot(self) -> set[str]:
        with self._lock:
            return set(self._ids)


def _on_shutdown(signum, _frame):
    _shutdown_event.set()
    logger.info("【系统】🛑 收到退出信号 %s | 正在结束消费循环", signum)


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="QMT跟单助手 —— Redis Stream → miniQMT 实盘跟单")
    parser.add_argument(
        "--config",
        default=None,
        help="YAML 配置文件路径 (缺省: main.py 同级目录下的 config.yaml)",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=TRADE_EXECUTION_WORKERS,
        help="每个买卖方向的交易执行线程数 (默认 %(default)s)。盘前多票抢单与"
        "盘中多票止损需要并行, 至少要 ≥ 策略单日目标股数(例如 5), 否则"
        "第 5 只买单要等前面的 worker 空出来才提交, 错过连续竞价排位; "
        "涨跌停排队单(买卖两侧)在 qmt-queue 专用线程池慢轮询, 不再占用"
        "买卖 worker",
    )
    args = parser.parse_args(argv)
    if args.workers < 1:
        parser.error("--workers 必须 ≥ 1")
    return args


def _build_execution_components(
    *,
    config: RuntimeConfig,
    store: SQLiteExecutionStore,
    market_data: QmtMarketDataAdapter,
    broker: QmtBrokerAdapter,
    queue_executor=None,
    stop_event: threading.Event | None = None,
) -> tuple[OpeningSellBarrier, OrderExecutionEngine]:
    """用同一个交易机日程实例组装开盘屏障与执行引擎。"""
    opening_barrier = OpeningSellBarrier(config.machine_schedule)
    engine = OrderExecutionEngine(
        store=store,
        market_data=market_data,
        broker=broker,
        config=config.execution,
        machine_schedule=config.machine_schedule,
        on_limit_down_queued=opening_barrier.release_all,
        queue_executor=queue_executor,
        stop_event=stop_event,
    )
    return opening_barrier, engine


def main(default_config: str = "config.yaml") -> None:
    """后端服务入口。

    把配置、Redis、SQLite、行情适配器、券商适配器组装起来，
    然后用买入、卖出两个独立线程池 (各 --workers 线程) 并发处理交易信号。
    default_config 由 main.py 传入 (锚定在 main.py 同级目录), 使
    ``python main.py`` 从任意工作目录启动都能找到同级的 config.yaml;
    --config 显式指定时优先。
    """
    args = _parse_args()
    config_path = args.config or default_config

    config, strategy_config = load_runtime_with_strategy(
        config_path, workers=args.workers
    )

    # 初始化日志系统: 控制台 + 文件双输出
    setup_logging(
        level=config.log_level,
        log_dir=config.log_dir,
        file_level=config.log_file_level,
    )
    logger.info("【系统】🚀 QMT跟单助手启动中 | 买卖各 %s 线程并发", args.workers)
    if strategy_config is None:
        logger.info(
            "【系统】🧭 运行模式: 纯跟单 | 无本地策略引擎, 只执行白名单策略"
            "下发的普通信号 (plan/buy/sell/sell_half/sell_all/watchlist)"
        )
    else:
        logger.info(
            "【系统】🧭 运行模式: 本地策略引擎 | strategy_id=%s",
            strategy_config.strategy_id,
        )
    allowed_strategies = set(config.redis.allowed_strategy_ids)
    if allowed_strategies:
        logger.info("【系统】🛂 策略白名单已启用 | %s", ",".join(sorted(allowed_strategies)))
    continuous_start_at = (
        config.machine_schedule.market_session.continuous_trading_start_at.strftime(
            "%H:%M:%S"
        )
    )
    if config.execution.plan_execute_at != continuous_start_at:
        logger.warning(
            "【配置】⚠️ plan_execute_at=%s 仅作旧配置兼容，当前不控制延时；"
            "买单由%s连续竞价屏障控制",
            config.execution.plan_execute_at,
            continuous_start_at,
        )

    instance_lock = SingleInstanceLock(str(config.state_db) + ".lock")
    instance_lock.acquire()
    atexit.register(instance_lock.release)

    # Redis 负责收信号; SQLite 负责记录本机执行状态; Engine 负责下单状态机。
    stream = RedisStreamClient(config.redis)
    store = SQLiteExecutionStore(config.state_db)
    market_data = QmtMarketDataAdapter(pre_subscribe_codes=config.market_data.pre_subscribe_codes)
    broker = QmtBrokerAdapter(
        config.trading,
        ghost_order_detect_grace_sec=config.execution.ghost_order_detect_grace_sec,
    )

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
    # 涨跌停排队单(买卖两侧)都在 qmt-queue 专用线程池慢轮询, 不再占用买卖
    # worker; 队列池容量 = 卖出排队上限 + 买入排队上限, 由池构造自动对齐。
    logger.info(
        "【系统】🎫 排队单专用线程池 | 卖出上限 %s + 买入上限 %s",
        config.execution.max_concurrent_queue_sells,
        config.execution.max_concurrent_queue_buys,
    )

    with (
        ThreadPoolExecutor(
            max_workers=args.workers, thread_name_prefix="qmt-sell"
        ) as sell_pool,
        ThreadPoolExecutor(
            max_workers=args.workers, thread_name_prefix="qmt-buy"
        ) as buy_pool,
        ThreadPoolExecutor(
            max_workers=max(
                1,
                config.execution.max_concurrent_queue_sells
                + config.execution.max_concurrent_queue_buys,
            ),
            thread_name_prefix="qmt-queue",
        ) as queue_pool,
    ):
        opening_barrier, engine = _build_execution_components(
            config=config,
            store=store,
            market_data=market_data,
            broker=broker,
            queue_executor=queue_pool,
            stop_event=_shutdown_event,
        )
        plan_executor = PlanExecutor(store=store, broker=engine.broker)
        pools = {Action.SELL: sell_pool, Action.BUY: buy_pool}
        pending: dict[Future[Any], StreamMessage] = {}
        message_registry = _MessageIdRegistry()
        if strategy_config is None:
            # 纯跟单模式: 不建本地策略引擎、不拒绝任何普通信号, 白名单策略
            # 的 plan/buy/sell/sell_half/sell_all/watchlist 消息全部进执行链路。
            local_strategy = None
            local_strategy_id = None
        else:
            local_strategy = LocalStrategyEngine(
                config=strategy_config,
                machine_schedule=config.machine_schedule,
                store=store,
                market_data=market_data,
                broker=broker,
                executor=engine,
                pools=pools,
                opening_barrier=opening_barrier,
                # 单票仓位上限唯一来源: config.strategy.yaml 的 execution 节点,
                # 已通过策略执行覆盖注入到 runtime.execution。
                single_position_limit_pct=config.execution.max_single_position_pct,
                stop_event=_shutdown_event,
            )
            local_strategy_id = strategy_config.strategy_id
            local_strategy.start()

        try:
            for message in stream.read_forever(
                stop_event=_shutdown_event,
                active_message_ids_provider=lambda: {
                    item.message_id for item in list(pending.values())
                } | message_registry.snapshot(),
            ):
                if _shutdown_event.is_set():
                    break

                # 兜底收割: ACK 主要由任务完成回调驱动(见 _submit_trade),
                # 这里只是防止回调因异常漏掉。因为不再靠它保证 ACK 及时性,
                # block_ms 可以放心调大 —— Redis 阻塞读是服务端推送, BLOCK
                # 调大不增加收信延迟, 只减少跨网络的空轮询。
                _reap_completed(
                    pending, stream, store, engine, message_registry,
                )

                if message is None:
                    continue

                # 单条消息的轻量分发; 重活(订阅/查持仓)在其内部转后台线程。
                # 打点"收信处理耗时"供复盘: 目标 P95 < 50ms。
                t0 = time.monotonic()
                _dispatch_message(
                    message=message,
                    config=config,
                    allowed_strategies=allowed_strategies,
                    plan_executor=plan_executor,
                    pools=pools,
                    engine=engine,
                    opening_barrier=opening_barrier,
                    pending=pending,
                    stream=stream,
                    store=store,
                    local_strategy=local_strategy,
                    local_strategy_id=local_strategy_id,
                    market_data=market_data,
                    registry=message_registry,
                )
                logger.debug(
                    "⏱️ 收信处理耗时 | %.1fms | msg_id=%s",
                    (time.monotonic() - t0) * 1000, message.message_id,
                )

        finally:
            if local_strategy is not None:
                local_strategy.stop()
            if _shutdown_event.is_set():
                logger.info("【系统】⏳ 等待 %s 个进行中的任务完成", len(pending))
            else:
                logger.info("【系统】⏳ 消费循环退出 | 等待 %s 个任务完成", len(pending))
            # 等待所有进行中的任务。先快照: as_completed 会遍历传入的容器,
            # 而工作线程的完成回调正在并发 pop 同一个 pending。
            for future in as_completed(list(pending)):
                _reap_one(future, pending, stream, store, engine, message_registry)
            # Future 进入 done 后完成回调仍可能在工作线程里收尾。
            # 先确认两个线程池都退出，再关 SQLite，避免回调读账本时
            # 连接被主线程提前关掉。with 退出时再 shutdown 一次是无害的。
            sell_pool.shutdown(wait=True)
            buy_pool.shutdown(wait=True)
            try:
                store.close_all()
            finally:
                instance_lock.release()
            logger.info("【系统】👋 QMT跟单助手已退出")


def _strategy_allowed(strategy_id: str, allowed: set[str]) -> bool:
    """strategy_id 白名单判定; 白名单为空表示不过滤 (兼容旧配置)。"""
    return not allowed or strategy_id in allowed


def _ack_if_candidate_not_allowed(
    message: StreamMessage,
    allowed: set[str],
    stream: RedisStreamClient,
) -> bool:
    """候选计划白名单校验: 不在白名单则记日志并 ACK, 绝不进入落库路径。"""
    plan = message.candidate_plan
    if plan is None or _strategy_allowed(plan.strategy_id, allowed):
        return False
    logger.info(
        "【策略】🛂 候选计划忽略并ACK | 策略 %s 不在白名单 | %s",
        plan.strategy_id,
        plan.plan_id,
    )
    stream.ack(message.message_id)
    return True


def _ack_if_not_live(message: StreamMessage, stream: RedisStreamClient) -> bool:
    """非 live 消息只留审计日志并确认，绝不进入行情/资金/券商路径。"""
    if message.signal is not None:
        mode = message.signal.mode
        label = message.signal.signal_id
    elif message.plan is not None:
        mode = message.plan.mode
        label = message.plan.signal_id
    elif message.watchlist is not None:
        mode = message.watchlist.mode
        label = "watchlist"
    elif message.candidate_plan is not None:
        mode = message.candidate_plan.mode
        label = message.candidate_plan.plan_id
    else:
        return False
    if str(mode).strip().lower() == "live":
        return False
    logger.warning(
        "【Redis】⛔ 非实盘消息已忽略 | mode=%s | %s", mode, label,
    )
    stream.ack(message.message_id)
    return True


def _handle_candidate_plan(
    message: StreamMessage,
    local_strategy: LocalStrategyEngine,
    stream: RedisStreamClient,
) -> None:
    """候选计划必须先落 SQLite 再 ACK；落库失败则保留 Redis 待办。"""
    plan = message.candidate_plan
    try:
        status = local_strategy.accept_candidate_plan(plan)
    except CandidatePlanConflict as exc:
        logger.critical(
            "【策略】🛑 候选计划冲突 | %s | 当日禁止买入 | %s",
            plan.plan_id,
            exc,
        )
        stream.ack(message.message_id)
        return
    except Exception as exc:
        logger.exception(
            "【策略】❌ 候选计划落库失败 | %s | 保留Redis待办 | %s",
            plan.plan_id,
            exc,
        )
        return
    if status in (CandidatePlanStatus.LATE, CandidatePlanStatus.REJECTED):
        # 隔天开机重放的昨日候选计划: 仅审计落库, 不 READY、不订阅、不触发
        # 盘前挂单 —— 日志用警告级文案避免被误读为"正常采纳"。
        logger.warning(
            "【策略】⏳ 候选计划未采纳 | %s | %s | %s只 | 仅审计落库, 不执行不订阅",
            plan.plan_id,
            status.value,
            len(plan.candidates),
        )
    else:
        logger.info(
            "【策略】📋 候选计划已落库 | %s | %s | %s只",
            plan.plan_id,
            status.value,
            len(plan.candidates),
        )
    stream.ack(message.message_id)


def _reject_local_strategy_ordinary_message(
    message: StreamMessage,
    local_strategy_id: str,
    stream: RedisStreamClient,
    *,
    allowed_sell_purposes: Sequence[str],
) -> bool:
    """拒绝本地策略同 ID 的旧订单/旧 plan，其他策略仍走原白名单路径。

    例外: 聚宽侧下发的卖出信号 (quantity_mode=sell_all 且 purpose 在
    config.strategy.yaml 的 external_signals.allowed_sell_purposes 白名单内)
    放行 —— 那些退出决策以聚宽模拟盘为准, 交易端按真实持仓意图型全清执行;
    白名单内容属于策略契约, 代码不内置任何默认值。
    """
    if message.signal is not None:
        strategy_id = message.signal.strategy_id
        label = message.signal.signal_id
    elif message.plan is not None:
        strategy_id = message.plan.strategy_id
        label = message.plan.signal_id
    else:
        return False
    if strategy_id != local_strategy_id:
        return False
    if (
        message.signal is not None
        and message.signal.quantity_mode == "sell_all"
        and message.signal.purpose in allowed_sell_purposes
    ):
        return False
    logger.warning(
        "【策略】⛔ 本地策略普通交易消息已拒绝 | %s | 仅接受 candidate_plan 与白名单 sell_all 信号(%s)",
        label,
        "、".join(allowed_sell_purposes),
    )
    stream.ack(message.message_id)
    return True


def _handle_plan_message(
    *,
    message: StreamMessage,
    config,
    allowed_strategies: set[str],
    plan_executor: PlanExecutor,
    pools: TradePools,
    engine: OrderExecutionEngine,
    opening_barrier: OpeningSellBarrier,
    pending: dict[Future[Any], StreamMessage],
    stream: RedisStreamClient,
    store: SQLiteExecutionStore,
) -> None:
    """单条日计划的故障边界；失败只保留Redis待办，不能打退出监听循环。"""
    plan = message.plan
    if not config.execution.plan_enabled:
        logger.warning(
            "【计划】🚫 日计划执行已关闭 | plan_enabled=false | %s",
            plan.signal_id,
        )
        stream.ack(message.message_id)
        return
    if not _strategy_allowed(plan.strategy_id, allowed_strategies):
        logger.info(
            "【计划】🛂 日计划忽略 | 策略 %s 不在白名单",
            plan.strategy_id,
        )
        stream.ack(message.message_id)
        return
    try:
        if not plan_executor.record(plan):
            logger.info("【计划】⏭️ 重复日计划 | %s", plan.signal_id)
        execute = _execute_recovered_safe if message.recovered else _execute_safe
        combined = submit_plan_tasks(
            plan,
            plan_executor,
            pools,
            engine,
            execute,
            opening_barrier,
        )
    except PlanPayloadConflict as exc:
        logger.critical(
            "【计划】🛑 日计划编号冲突 | %s | 已拒绝且不执行 | %s",
            plan.signal_id, exc,
        )
        stream.ack(message.message_id)
        return
    except Exception as exc:
        logger.exception(
            "【计划】❌ 日计划展开失败 | %s | 保留Redis待办 | %s",
            plan.signal_id, exc,
        )
        return
    if combined is None:
        logger.info("【计划】📋 日计划无可执行派生信号 | %s", plan.signal_id)
        stream.ack(message.message_id)
        return
    pending[combined] = message
    combined.add_done_callback(
        lambda future: _reap_one_safe(future, pending, stream, store, engine)
    )


def _execute_safe(
    engine: OrderExecutionEngine,
    signal: TradeSignal,
    opening_barrier: OpeningSellBarrier,
) -> ExecutionResult:
    """在线程中安全执行信号，捕获异常防止线程崩溃。"""
    return _execute_with_barrier(
        engine, signal, opening_barrier, recover_existing=False,
    )


def _execute_recovered_safe(
    engine: OrderExecutionEngine,
    signal: TradeSignal,
    opening_barrier: OpeningSellBarrier,
) -> ExecutionResult:
    """恢复重投信号的安全执行入口: 走 engine.recover 而非重新下单。"""
    return _execute_with_barrier(
        engine, signal, opening_barrier, recover_existing=True,
    )


def _execute_with_barrier(
    engine: OrderExecutionEngine,
    signal: TradeSignal,
    opening_barrier: OpeningSellBarrier,
    *,
    recover_existing: bool,
) -> ExecutionResult:
    """在开盘屏障约束下执行单条信号: BUY 先等连续竞价与盘前卖单释放, SELL 落终态后释放屏障。

    盘前挂单标记(preopen_submit)的买单豁免等待: 引擎已用盘前资金核定预算,
    委托要在 9:25-9:30 排队、9:30:00 开盘价撮合, 睡到开盘/等卖单反而错过撮合。
    """
    result: ExecutionResult | None = None
    try:
        if signal.action == Action.BUY and not signal.preopen_submit:
            preopen_wait = seconds_until_market_open(engine.machine_schedule)
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
        result = engine.recover(signal) if recover_existing else engine.execute(signal)
        return result
    except Exception as exc:
        logger.exception(
            "%s | %s | 未处理异常 | %s",
            signal.console_event("失败"), signal.display_code, exc,
        )
        # 不能把未落库的异常伪装成终态；让 Future 保留异常，上层据此不 ACK。
        raise
    finally:
        # 排队单转交专用线程后返回占位结果, 屏障释放由排队线程挂单落库后
        # 经 release_all 完成 —— 这里不能提前释放(否则破坏"卖单完成才放买")。
        if signal.action == Action.SELL and (
            result is None
            or result.status
            not in {
                ExecutionStatus.QUEUED_LIMIT_DOWN,
                ExecutionStatus.QUEUED_LIMIT_UP,
            }
        ):
            opening_barrier.release(signal.signal_id)


def _dispatch_message(
    *,
    message: StreamMessage,
    config: RuntimeConfig,
    allowed_strategies: set[str],
    plan_executor: PlanExecutor,
    pools: TradePools,
    engine: OrderExecutionEngine,
    opening_barrier: OpeningSellBarrier,
    pending: dict[Future[Any], StreamMessage],
    stream: RedisStreamClient,
    store: SQLiteExecutionStore,
    local_strategy: LocalStrategyEngine | None,
    local_strategy_id: str | None,
    market_data: QmtMarketDataAdapter,
    registry: _MessageIdRegistry | None = None,
) -> None:
    """主循环的单条消息分发: 只做轻量判断与派发, 重活一律进后台线程。

    从 main() 的收信循环里整体搬出以便直接单测, 语义与原循环逐行一致,
    仅三处变化: watchlist 订阅后台化、收单取名走只读缓存、plan 展开后台化。
    registry 非空时, 后台处理中的消息(id)登记在途, pending 扫描不会重投。
    local_strategy 为 None 时运行在纯跟单模式: 不做本地决策、不拒绝普通信号。
    """
    # 无法解析的消息: 记日志后直接 ACK, 绝不让它卡住消费组, 也绝不
    # 让它把消费循环打崩。Stream 是多策略共享的, 别的策略换个 schema
    # 就可能产生本执行端不认识的消息。
    if message.rejected is not None:
        logger.warning(
            "【Redis】⛔ 消息已丢弃 | msg_id=%s | %s",
            message.message_id, message.rejected,
        )
        stream.ack(message.message_id)
        return

    # 回测/测试消息必须在查询行情、账户和券商之前截住。
    if _ack_if_not_live(message, stream):
        return

    if message.candidate_plan is not None:
        if _ack_if_candidate_not_allowed(
            message, allowed_strategies, stream
        ):
            return
        if local_strategy is None:
            # 纯跟单模式没有本地策略引擎, candidate_plan 无处消费:
            # 记日志直接 ACK, 不落库不订阅。
            logger.warning(
                "【策略】⛔ 候选计划已忽略 | 纯跟单模式不消费 candidate_plan | %s",
                message.candidate_plan.plan_id,
            )
            stream.ack(message.message_id)
            return
        if registry is not None:
            registry.add(message.message_id)
        try:
            _handle_candidate_plan(message, local_strategy, stream)
        finally:
            if registry is not None:
                registry.discard(message.message_id)
        return

    # 本地引擎模式: 引擎对专用账户拥有唯一决策权；同一策略残留的旧普通
    # 交易/plan 必须 ACK 丢弃，避免聚宽与 QMT 同时管理真实仓位。白名单外的
    # sell_all 卖出信号属于例外(见 _reject_local_strategy_ordinary_message)。
    # 纯跟单模式(local_strategy=None)不做该拒绝, 白名单策略的普通信号照常执行。
    if local_strategy is not None and _reject_local_strategy_ordinary_message(
        message,
        local_strategy_id,
        stream,
        allowed_sell_purposes=(
            local_strategy.config.external_signals.allowed_sell_purposes
        ),
    ):
        return

    # 日计划: 展开要逐票全量查持仓(QMT同步调用), 转后台线程展开, 不堵收信。
    if message.plan is not None:
        plan_date = _message_trade_date(message.plan)
        if plan_date is not None and plan_date < _local_today():
            # 隔天开机重放昨天的 plan: 不落库、不展开、直接 ACK。派生信号继承
            # 发送时刻, 只靠 600s 过期兜底会落库污染; sent_at_ms 缺失时更会
            # 把昨天的清仓/建仓当真执行。
            logger.warning(
                "【计划】⏳ 昨日日计划已忽略 | %s | 计划日期=%s | 不落库不执行",
                message.plan.signal_id, plan_date,
            )
            stream.ack(message.message_id)
            return
        _handle_plan_message_in_background(
            message=message,
            config=config,
            allowed_strategies=allowed_strategies,
            plan_executor=plan_executor,
            pools=pools,
            engine=engine,
            opening_barrier=opening_barrier,
            pending=pending,
            stream=stream,
            store=store,
            registry=registry,
        )
        return

    # 预订阅指令: 只订阅行情, 不进执行引擎。首次订阅可能拉取 tick 历史,
    # 先 ACK 再丢后台线程, 绝不堵收信。
    if message.watchlist is not None:
        if not _strategy_allowed(message.watchlist.strategy_id, allowed_strategies):
            logger.info(
                "【行情】🛂 预订阅忽略 | 策略 %s 不在白名单",
                message.watchlist.strategy_id or "<空>",
            )
            stream.ack(message.message_id)
            return
        stream.ack(message.message_id)
        _subscribe_watchlist_in_background(message.watchlist, market_data)
        return

    sig = message.signal
    # 日志展示用中文名: 收单只读缓存(未命中不查QMT), 工作线程里兜底补名。
    if not sig.stock_name:
        sig = sig.with_stock_name(market_data.cached_instrument_name(sig.code))
        message = replace(message, signal=sig)
    if not _strategy_allowed(sig.strategy_id, allowed_strategies):
        logger.warning(
            "%s | %s | 策略 %s 不在白名单 | 忽略并ACK",
            sig.console_event("跳过"), sig.display_code, sig.strategy_id,
        )
        stream.ack(message.message_id)
        return
    sig_date = _message_trade_date(sig)
    if sig_date is not None and sig_date < _local_today():
        # 隔天开机重放昨天的交易信号: 直接忽略并 ACK, 不进执行引擎。引擎里的
        # 600s 过期是第二道闸, 但旧协议消息没有 sent_at_ms 时不会触发 ——
        # 这道收信侧门控把"昨天消息今天执行"的可能性彻底关死。
        logger.warning(
            "%s | %s | ⏳ 陈旧信号已忽略 | 信号日期=%s | ACK不下单",
            sig.console_event("跳过"), sig.display_code, sig_date,
        )
        stream.ack(message.message_id)
        return
    logger.debug("📨 收到信号原始ID | signal_id=%s", sig.signal_id)
    logger.info(
        "%s | %s | %s股 | 参考 %.3f | 策略=%s | 前方 %s单%s",
        sig.console_prefix, sig.display_code, sig.amount,
        sig.reference_price, sig.strategy_id, len(pending),
        _transport_latency_label(sig.sent_at_ms),
    )

    _submit_trade(
        message, pending, pools, engine, opening_barrier, stream, store,
        registry,
    )


def _subscribe_watchlist_in_background(watchlist, market_data) -> threading.Thread:
    """盘前预订阅放后台线程: 首次订阅可能拉取 tick 历史, 不能堵收信循环。"""

    def _run() -> None:
        try:
            market_data.subscribe(watchlist.codes)
            code_labels = [
                format_stock_label(code, market_data.instrument_name(code))
                for code in watchlist.codes
            ]
            logger.info(
                "【行情】📡 预订阅 | 策略=%s | %s只 | %s",
                watchlist.strategy_id,
                len(watchlist.codes),
                ",".join(code_labels),
            )
        except Exception as exc:
            logger.exception(
                "【行情】❌ 预订阅失败 | 策略=%s | %s", watchlist.strategy_id, exc,
            )

    thread = threading.Thread(
        target=_run, name="qmt-watchlist-subscribe", daemon=True,
    )
    thread.start()
    return thread


def _handle_plan_message_in_background(*, registry=None, **kwargs) -> threading.Thread:
    """plan 展开要逐票全量查持仓(QMT同步调用), 放后台线程, 不堵收信循环。

    展开期间消息加入在途注册表, pending 扫描不会重投; 展开结束(派单进
    pending 或直接 ACK/丢弃)后注销。
    """
    message = kwargs["message"]
    if registry is not None:
        registry.add(message.message_id)

    def _run() -> None:
        try:
            _handle_plan_message(**kwargs)
        finally:
            if registry is not None:
                registry.discard(message.message_id)

    thread = threading.Thread(
        target=_run, name="qmt-plan-expand", daemon=True,
    )
    thread.start()
    return thread


def _submit_trade(
    message: StreamMessage,
    pending: dict[Future[ExecutionResult], StreamMessage],
    pools: TradePools,
    engine: OrderExecutionEngine,
    opening_barrier: OpeningSellBarrier,
    stream: RedisStreamClient | None = None,
    store: SQLiteExecutionStore | None = None,
    registry: _MessageIdRegistry | None = None,
) -> None:
    """登记盘前卖单后，把交易信号提交给对应方向的并发工作池。

    传入 stream 时挂一个完成回调, 让 ACK 和终态日志在执行结束的那一刻发生,
    而不是等消费循环的下一次轮询 —— 这样 block_ms 就不再是 ACK 时效的瓶颈。
    """
    signal = message.signal
    opening_barrier.register(signal)
    pool = pools[signal.action]
    execute = _execute_recovered_safe if message.recovered else _execute_safe
    future = pool.submit(execute, engine, signal, opening_barrier)
    pending[future] = message
    if stream is not None:
        future.add_done_callback(
            lambda f: _reap_one_safe(
                f, pending, stream, store, engine, registry,
            )
        )


def _reap_one_safe(
    future: Future[ExecutionResult],
    pending: dict[Future[ExecutionResult], StreamMessage],
    stream: RedisStreamClient,
    store: SQLiteExecutionStore | None = None,
    engine: OrderExecutionEngine | None = None,
    registry: _MessageIdRegistry | None = None,
) -> None:
    """完成回调入口: 在工作线程里跑, 绝不能抛 —— 抛了只会进 futures 的日志。"""
    try:
        _reap_one(future, pending, stream, store, engine, registry)
    except Exception as exc:
        logger.exception("【系统】❌ 完成回调异常 | %s", exc)


def _reap_completed(
    pending: dict[Future[ExecutionResult], StreamMessage],
    stream: RedisStreamClient,
    store: SQLiteExecutionStore | None = None,
    engine: OrderExecutionEngine | None = None,
    registry: _MessageIdRegistry | None = None,
) -> None:
    """清理所有已完成的任务，ACK 对应的 Redis 消息。

    必须先 list() 快照再遍历: 完成回调在工作线程里 pop 同一个 pending,
    直接遍历 dict 会撞上 "dictionary changed size during iteration" ——
    而这个异常会一路穿出没有 except 的消费循环, 让当天的跟单直接收工。
    """
    for future in list(pending):
        if future.done():
            _reap_one(future, pending, stream, store, engine, registry)


def _reap_one(
    future: Future[ExecutionResult],
    pending: dict[Future[ExecutionResult], StreamMessage],
    stream: RedisStreamClient,
    store: SQLiteExecutionStore | None = None,
    engine: OrderExecutionEngine | None = None,
    registry: _MessageIdRegistry | None = None,
) -> None:
    """处理单个已完成任务：ACK + 日志。

    完成回调、主循环兜底收割、退出等待三条路径会对同一条记录竞争, 由 pop 的
    原子性定胜负: 谁先取到 message 谁负责 ACK, 后到的直接返回。
    排队单的占位结果(QUEUED_*)会在这里挂接专用线程 future, 终态后补 ACK;
    移交期间消息 id 登记在途注册表, pending 扫描不会重投。
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
            return
        if not all(_result_is_durably_terminal(result, store) for result in results):
            if engine is not None:
                for result in results:
                    if result.status in {
                        ExecutionStatus.QUEUED_LIMIT_DOWN,
                        ExecutionStatus.QUEUED_LIMIT_UP,
                    }:
                        queued = engine.queue_future_for(result.signal_id)
                        if queued is not None and not queued.done():
                            if registry is not None:
                                registry.add(message.message_id)
                            queued.add_done_callback(
                                lambda f, m=message, rs=results: _reap_plan_queued(
                                    f, m, rs, stream, store, registry,
                                )
                            )
            logger.error(
                "【计划】🛑 日计划存在未落库终态 | %s | 保留Redis待办",
                message.plan.signal_id,
            )
            return
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
        return

    if not _result_is_durably_terminal(result, store):
        if (
            engine is not None
            and result.status
            in {
                ExecutionStatus.QUEUED_LIMIT_DOWN,
                ExecutionStatus.QUEUED_LIMIT_UP,
            }
        ):
            queued = engine.queue_future_for(result.signal_id)
            if queued is not None and not queued.done():
                if registry is not None:
                    registry.add(message.message_id)
                queued.add_done_callback(
                    lambda f, m=message: _reap_queued_terminal(
                        f, m, stream, store, registry,
                    )
                )
                return
        logger.error(
            "%s | %s | 本机账本尚无明确终态 | 保留Redis待办",
            message.signal.console_event("停止"), message.signal.display_code,
        )
        return
    stream.ack(message.message_id)


def _reap_queued_terminal(
    future: Future[ExecutionResult],
    message: StreamMessage,
    stream: RedisStreamClient,
    store: SQLiteExecutionStore | None = None,
    registry: _MessageIdRegistry | None = None,
) -> None:
    """排队单在专用线程到达终态后补发 ACK; 未落库终态则保留 Redis 待办。"""
    try:
        result = future.result()
        if not _result_is_durably_terminal(result, store):
            logger.error(
                "%s | 排队单未落库终态 | 保留Redis待办", result.signal_id,
            )
            return
        # 先注销再 ACK: ACK 失败时消息重新可被 pending 扫描接管,
        # 重投 → 幂等/恢复 → 终态 → 重试 ACK, 闭环成立。
        if registry is not None:
            registry.discard(message.message_id)
        logger.debug(
            "🏁 排队单执行完成 | %s 状态=%s",
            result.signal_id, result.status.value,
        )
        stream.ack(message.message_id)
    except Exception:
        logger.exception("【系统】❌ 排队单终态回调异常 | %s", message.message_id)


def _reap_plan_queued(
    future: Future[ExecutionResult],
    message: StreamMessage,
    results: list[ExecutionResult],
    stream: RedisStreamClient,
    store: SQLiteExecutionStore | None = None,
    registry: _MessageIdRegistry | None = None,
) -> None:
    """日计划里有排队派生信号时, 最后一个排队单终态后复查全部结果再 ACK。"""
    try:
        if not all(_result_is_durably_terminal(r, store) for r in results):
            return
        if registry is not None:
            registry.discard(message.message_id)
        stream.ack(message.message_id)
    except Exception:
        logger.exception("【计划】❌ 排队派生信号终态回调异常 | %s", message.message_id)


def _result_is_durably_terminal(
    result: ExecutionResult,
    store: SQLiteExecutionStore | None,
) -> bool:
    """生产环境以 SQLite 状态为准；无 store 仅供现有纯单元测试兼容。"""
    if store is None:
        return is_terminal_execution_status(result.status)
    try:
        stored = store.get_signal(result.signal_id)
    except Exception as exc:
        logger.exception(
            "【账本】❌ 读取信号终态失败 | signal_id=%s | %s",
            result.signal_id, exc,
        )
        return False
    return is_terminal_execution_status(stored.status)


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

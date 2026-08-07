"""订单执行状态机。

核心职责: 接收 TradeSignal → 幂等去重 → 行情定价 → 下单 → 轮询 → 成交/撤单/重挂 → 终态记录。

对 BrokerAdapter / MarketDataAdapter 只依赖 Protocol, 不耦合具体实现。
"""

from __future__ import annotations

import datetime as dt
import logging
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass, replace
from typing import Protocol

from miniqmt_follower.models import (
    Action,
    BrokerOrderRejected,
    BrokerOrderStatus,
    BrokerRejectionKind,
    BrokerSubmissionUncertain,
    ExecutionConfig,
    ExecutionResult,
    ExecutionStatus,
    OrderSnapshot,
    Quote,
    TradeSignal,
)
from miniqmt_follower.opening import is_preopen_sell
from miniqmt_follower.pricing import calculate_order_price, tick_size_for
from miniqmt_follower.sizing import resolve_auto_buy, resolve_sell_all, resolve_sell_half
from miniqmt_follower.store import SQLiteExecutionStore

logger = logging.getLogger(__name__)


class MarketDataAdapter(Protocol):
    """行情适配器协议。

    真实环境由 xtquant 提供行情快照, 测试环境可用假行情源替代。
    """

    def latest_quote(self, code: str) -> Quote:
        pass

    def instrument_name(self, code: str) -> str | None:
        """返回证券中文名; 合约信息不可得时返回 None, 日志回退为只显示代码。"""
        pass


class BrokerAdapter(Protocol):
    """券商交易适配器协议。

    执行引擎只依赖这五个动作: 查资金、查持仓、下单、查单、撤单。
    这样 miniQMT 的 API 细节不会污染核心状态机。
    """

    def query_available_cash(self) -> float:
        """查询账户当前可用资金。"""
        pass

    def query_available_position(self, code: str) -> int:
        """查询某只股票的可用持仓（可卖数量）。"""
        pass

    def query_total_assets(self) -> float:
        """查询账户当前总资产（现金 + 股票市值）。"""
        pass

    def query_position(self, code: str) -> int:
        """查询某只股票的总持仓（含当日不可卖部分），用于"已有持仓不补仓"。"""
        pass

    def submit_order(self, signal: TradeSignal, quantity: int, price: float) -> str:
        pass

    def get_order_snapshot(self, order_id: str) -> OrderSnapshot:
        pass

    def cancel_order(self, order_id: str) -> None:
        pass


class _TradingHalted(RuntimeError):
    """交易通道已熔断，禁止新的委托触达券商。"""


# ---------------------------------------------------------------------------
# 状态名称中英文映射 —— 让日志中的状态值更易读
# ---------------------------------------------------------------------------

_STATUS_LABELS: dict[ExecutionStatus, str] = {
    ExecutionStatus.RECEIVED: "已接收",
    ExecutionStatus.ACCEPTED: "已登记",
    ExecutionStatus.DUPLICATE_IGNORED: "重复忽略",
    ExecutionStatus.ORDER_SUBMITTED: "已下单",
    ExecutionStatus.FILLED: "完全成交",
    ExecutionStatus.EXPIRED: "信号过期",
    ExecutionStatus.FAILED_TIMEOUT: "超时失败",
    ExecutionStatus.PARTIALLY_FILLED_TIMEOUT: "部分成交超时",
    ExecutionStatus.FAILED_RISK: "风控拒绝",
    ExecutionStatus.FAILED_BROKER: "券商失败",
    ExecutionStatus.SKIPPED_NO_POSITION: "实盘无持仓跳过",
    ExecutionStatus.SKIPPED_SMALL_POSITION: "半仓不足一手跳过",
    ExecutionStatus.SKIPPED_LIMIT_DOWN: "跌停跳过",
    ExecutionStatus.SKIPPED_LIMIT_UP: "涨停跳过",
    ExecutionStatus.QUEUED_LIMIT_DOWN: "跌停排队中",
    ExecutionStatus.LIMIT_DOWN_QUEUE_EXPIRED: "跌停排队未成",
    ExecutionStatus.QUEUED_LIMIT_UP: "涨停排队中",
    ExecutionStatus.LIMIT_UP_QUEUE_EXPIRED: "涨停排队未成",
}

_BROKER_STATUS_LABELS: dict[BrokerOrderStatus, str] = {
    BrokerOrderStatus.OPEN: "等待成交",
    BrokerOrderStatus.PARTIALLY_FILLED: "部分成交",
    BrokerOrderStatus.FILLED: "完全成交",
    BrokerOrderStatus.CANCELED: "已撤销",
    BrokerOrderStatus.REJECTED: "已拒绝",
}

_TERMINAL_ORDER_STATUSES = {
    BrokerOrderStatus.FILLED,
    BrokerOrderStatus.CANCELED,
    BrokerOrderStatus.REJECTED,
}

_QUEUE_BUY_POLL_INTERVAL_SEC = 3.0
_OPENING_SELL_RECONCILE_GRACE_SEC = 0.5


@dataclass(frozen=True)
class _QueueBuyRetry:
    """涨停排队废单已确认终态，携带账务结果回普通主循环刷新。"""

    target_qty: int
    total_filled: int
    attempts: int


def _status_label(status: ExecutionStatus) -> str:
    return _STATUS_LABELS.get(status, status.value)


def _broker_label(status: BrokerOrderStatus) -> str:
    return _BROKER_STATUS_LABELS.get(status, status.value)


# ---------------------------------------------------------------------------
# 竞价时段保护
# ---------------------------------------------------------------------------

_AUCTION_START = dt.time(9, 15)
_MARKET_OPEN = dt.time(9, 30)


def _seconds_until_market_open() -> float:
    """竞价时段(9:15~9:30)返回距开盘的秒数, 其他时间返回 0。

    策略常在 9:25~9:30 之间发信号(选股在竞价结束时完成), 此时提交的委托会在
    券商排队、开盘才撮合。若仍按 order_timeout_sec 判超时, 会把排队中的开盘单
    撤掉。因此竞价时段把单次轮询和总时长限制都顺延到开盘之后。
    测试可通过替换本模块级函数关闭该行为。
    """
    now = dt.datetime.now()
    if _AUCTION_START <= now.time() < _MARKET_OPEN:
        open_dt = now.replace(hour=_MARKET_OPEN.hour, minute=_MARKET_OPEN.minute, second=0, microsecond=0)
        return (open_dt - now).total_seconds()
    return 0.0


def _seconds_until_queue_sell_deadline(deadline_hhmmss: str) -> float:
    """返回距跌停排队截止时刻的秒数; 已过截止或格式非法返回 0。

    截止默认 14:56:30, 避开深市 14:57 尾盘集合竞价; 到点主动撤单,
    让本地台账在收盘前落终态 (券商收盘也会自动废单, 主动撤是为了记账确定性)。
    测试可通过替换本模块级函数控制排队时长。
    """
    try:
        parts = [int(p) for p in str(deadline_hhmmss).split(":")]
        deadline_time = dt.time(*parts)
    except (ValueError, TypeError):
        logger.error("❌ queue_sell_deadline 格式非法: %r | 跌停排队降级为跳过", deadline_hhmmss)
        return 0.0
    now = dt.datetime.now()
    deadline_dt = now.replace(
        hour=deadline_time.hour,
        minute=deadline_time.minute,
        second=deadline_time.second,
        microsecond=0,
    )
    return max(0.0, (deadline_dt - now).total_seconds())


def _seconds_until_queue_buy_deadline(deadline_hhmmss: str) -> float:
    """返回距涨停买单排队截止时刻的秒数；已过截止或格式非法返回 0。"""
    try:
        parts = [int(p) for p in str(deadline_hhmmss).split(":")]
        deadline_time = dt.time(*parts)
    except (ValueError, TypeError):
        logger.error("❌ queue_buy_deadline 格式非法: %r | 涨停排队降级为跳过", deadline_hhmmss)
        return 0.0
    now = dt.datetime.now()
    deadline_dt = now.replace(
        hour=deadline_time.hour,
        minute=deadline_time.minute,
        second=deadline_time.second,
        microsecond=0,
    )
    return max(0.0, (deadline_dt - now).total_seconds())


# ---------------------------------------------------------------------------
# 执行引擎
# ---------------------------------------------------------------------------


class OrderExecutionEngine:
    """订单执行状态机。

    核心规则:
    1. 先用 signal_id 做严格幂等, 防止重复下单。
    2. 每次下单前取最新行情并重新按滑点定价。
    3. 超时未完全成交就撤单, 只对剩余数量继续重挂。
    4. 达到最大尝试次数或总耗时限制后进入终态。
    """

    def __init__(
        self,
        store: SQLiteExecutionStore,
        market_data: MarketDataAdapter,
        broker: BrokerAdapter,
        config: ExecutionConfig,
        *,
        on_limit_down_queued: Callable[[str], None] | None = None,
    ):
        self.store = store
        self.market_data = market_data
        self.broker = broker
        self.config = config
        self._on_limit_down_queued = on_limit_down_queued or (lambda _signal_id: None)
        self._trading_halt_reason: str | None = None
        self._broker_submission_lock = threading.Lock()
        # 跌停排队卖出的并发闸: 排队单占用 worker 直到成交或截止,
        # 超过上限的新排队请求降级为 skip, 保证有 worker 留给正常信号。
        self._queue_sell_lock = threading.Lock()
        self._active_queue_sells = 0
        self._queue_buy_lock = threading.Lock()
        self._active_queue_buys = 0
        self._buy_cash_submit_lock = threading.Lock()

    def execute(self, signal: TradeSignal) -> ExecutionResult:
        # 延迟打点起点: 从工作线程真正开始处理这条信号算起。
        exec_started = time.monotonic()
        # 日志展示用中文名: 每条信号解析一次, 整条时间线统一显示 名称(代码)。
        if not signal.stock_name:
            signal = signal.with_stock_name(self.market_data.instrument_name(signal.code))
        code_label = signal.display_code
        logger.debug("⚡ 开始执行信号 | signal_id=%s", signal.signal_id)

        # ---- 幂等去重 ----
        if not self.store.try_accept_signal(signal):
            existing = self.store.get_signal(signal.signal_id)
            logger.info(
                "%s | %s | 已有状态 %s | 成交 %s股",
                signal.console_event("重复"),
                code_label,
                _status_label(existing.status),
                existing.filled_qty,
            )
            return ExecutionResult(
                signal_id=signal.signal_id,
                status=ExecutionStatus.DUPLICATE_IGNORED,
                requested_qty=signal.amount,
                filled_qty=existing.filled_qty,
                attempts=0,
                message="signal_id already accepted",
            )

        logger.debug(
            "🔖 信号已登记 | %s @%.2f 策略=%s",
            signal.label,
            signal.reference_price,
            signal.strategy_id,
        )

        if signal.quantity_mode != "exact":
            try:
                resolved = self._resolve_intent_amount(signal)
            except Exception as exc:
                logger.error(
                    "%s | %s | 意图数量解析失败 | %s",
                    signal.console_event("失败"), code_label, exc,
                )
                return self._finish(
                    signal, ExecutionStatus.FAILED_BROKER, 0, 0,
                    f"intent resolution failed: {exc}",
                )
            if resolved <= 0:
                if signal.action == Action.SELL:
                    if (
                        signal.quantity_mode == "sell_half"
                        and self.broker.query_available_position(signal.code) > 0
                    ):
                        logger.warning(
                            "%s | %s | 半仓不足一手 | 配置为跳过",
                            signal.console_event("跳过"), code_label,
                        )
                        return self._finish(
                            signal, ExecutionStatus.SKIPPED_SMALL_POSITION, 0, 0,
                            "skipped: sell-half below one lot",
                        )
                    logger.warning(
                        "%s | %s | 实盘无可卖持仓 | 跳过",
                        signal.console_event("跳过"), code_label,
                    )
                    return self._finish(
                        signal, ExecutionStatus.SKIPPED_NO_POSITION, 0, 0,
                        "skipped: no live position",
                    )
                logger.warning(
                    "%s | %s | 自动买入预算不足(0股)",
                    signal.console_event("风控"), code_label,
                )
                return self._finish(
                    signal, ExecutionStatus.FAILED_RISK, 0, 0,
                    "insufficient funds for auto buy",
                )
            logger.info(
                "%s | %s | 实盘计算数量 | %s=%s股",
                signal.console_prefix, code_label, signal.quantity_mode, resolved,
            )
            signal = replace(
                signal, amount=resolved, quantity_mode="exact", budget_group_size=None,
            )

        if signal.expire_at:
            try:
                expire_at = dt.datetime.strptime(signal.expire_at, "%Y-%m-%d %H:%M:%S")
            except ValueError:
                logger.error(
                    "%s | %s | 过期时间非法 %r | 未下单",
                    signal.console_event("失败"), code_label, signal.expire_at,
                )
                return self._finish(
                    signal,
                    ExecutionStatus.FAILED_RISK,
                    0,
                    0,
                    "invalid expire_at: %s" % signal.expire_at,
                )
            now = dt.datetime.now()
            if now > expire_at:
                overdue_sec = (now - expire_at).total_seconds()
                logger.warning(
                    "%s | %s | 截止 %s | 已过期 %.1fs | 未下单",
                    signal.console_event("过期"), code_label, signal.expire_at, overdue_sec,
                )
                return self._finish(
                    signal,
                    ExecutionStatus.EXPIRED,
                    0,
                    0,
                    "signal expired at %s" % signal.expire_at,
                )

        # 上一笔订单撤单终态不明确时，禁止任何后续信号继续触达券商。
        halt_reason = self._current_trading_halt_reason()
        if halt_reason is not None:
            logger.critical(
                "%s | %s | 交易通道已停止 | %s",
                signal.console_event("停止"), code_label, halt_reason,
            )
            return self._finish(
                signal,
                ExecutionStatus.FAILED_BROKER,
                0,
                0,
                halt_reason,
            )

        # ---- 参数校验 ----
        if signal.amount <= 0:
            logger.warning("%s | %s | 数量非法 %s股", signal.console_event("风控"), code_label, signal.amount)
            return self._finish(signal, ExecutionStatus.FAILED_RISK, 0, 0, "amount must be positive")

        # ---- 主循环: 下单 → 轮询 → 成交/撤单/重挂 ----
        started = time.monotonic()
        target_qty: int | None = None
        remaining_qty = signal.amount
        total_filled = 0
        attempts = 0

        # 竞价时段收到的信号: 委托在券商排队至开盘撮合, 总时长限制顺延到开盘之后。
        auction_extra = _seconds_until_market_open()
        if auction_extra > 0:
            logger.info(
                "%s | %s | 距开盘 %.0f秒 | 委托排队等待撮合",
                signal.console_event("竞价"), code_label, auction_extra,
            )
        while remaining_qty > 0 and attempts < self.config.max_attempts:
            # 总时长限制
            elapsed = time.monotonic() - started
            if elapsed > self.config.max_total_duration_sec + auction_extra:
                logger.warning(
                    "%s | %s | 总耗时 %.1fs | 超过上限 %ss",
                    signal.console_event("超时"), code_label, elapsed,
                    self.config.max_total_duration_sec,
                )
                break

            attempt_no = attempts + 1
            logger.debug(
                "🔄 第%d次尝试开始 | %s 剩余=%s股 已成交=%s股",
                attempt_no, signal.label, remaining_qty, total_filled,
            )

            # ---- 每次委托都重新获取行情和账户可用资源 ----
            try:
                quote = self.market_data.latest_quote(signal.code)

                # ---- 跌停锁盘卖单: 排队模式 ----
                # 开板窗口往往只有几秒, 放弃排队 = 放弃唯一逃生口。挂跌停价
                # 排队至截止时间, 期间不撤不重挂 (重挂丢队列位置); 无法排队
                # 的各种回退 (取不到跌停价/过截止/并发满) 在方法内降级为 skip。
                if (
                    signal.action == Action.SELL
                    and quote.bid1 is None
                    and self.config.effective_limit_down_sell_mode() == "queue"
                ):
                    return self._queue_sell_at_limit_down(
                        signal, quote, remaining_qty, total_filled, attempts,
                    )

                # ---- 涨停锁盘买单: 挂涨停价全天保留队列位置 ----
                if (
                    self.config.effective_limit_up_buy_mode() == "queue"
                    and self._is_confirmed_limit_up(signal, quote)
                ):
                    queue_result = self._queue_buy_at_limit_up(
                        signal,
                        quote,
                        remaining_qty,
                        total_filled,
                        attempts,
                        target_qty,
                    )
                    if isinstance(queue_result, ExecutionResult):
                        return queue_result
                    target_qty = queue_result.target_qty
                    total_filled = queue_result.total_filled
                    attempts = queue_result.attempts
                    remaining_qty = target_qty - total_filled
                    retry_delay = min(self.config.poll_interval_sec, 0.05)
                    if retry_delay > 0:
                        time.sleep(retry_delay)
                    continue

                # ---- 涨跌停无对手盘快速跳过 ----
                # 跌停无买盘时卖单只会排进跌停价的巨量队列, 几乎不可能成交,
                # 却占满 order_timeout/max_total_duration 和同方向 worker
                # (竞价时段还会被开盘顺延放大)。重挂前同样检查, 盘中砸到
                # 跌停的重试也能及时止损退出。买入对涨停缺卖盘同理。
                locked_label = self._locked_book_label(signal, quote)
                if locked_label is not None:
                    logger.warning(
                        "%s | %s | %s | 不下单直接终态",
                        signal.console_event("跳过"), code_label, locked_label,
                    )
                    if total_filled:
                        status = ExecutionStatus.PARTIALLY_FILLED_TIMEOUT
                    elif signal.action == Action.SELL:
                        status = ExecutionStatus.SKIPPED_LIMIT_DOWN
                    else:
                        status = ExecutionStatus.SKIPPED_LIMIT_UP
                    return self._finish(
                        signal, status, total_filled, attempts,
                        "skipped: locked limit book, no counterparty side",
                    )

                order_price = calculate_order_price(signal, quote, self.config)
                if signal.action == Action.BUY:
                    order_id, attempt_qty = self._submit_buy_with_cash_lock(
                        signal, remaining_qty, order_price,
                    )
                else:
                    attempt_qty = self._cap_attempt_to_available_resources(
                        signal, remaining_qty, order_price,
                    )
                    order_id = None
                if attempt_qty <= 0:
                    if signal.action == Action.SELL and total_filled == 0:
                        logger.warning(
                            "%s | %s | 实盘无可卖持仓 | 跳过",
                            signal.console_event("跳过"), code_label,
                        )
                        return self._finish(
                            signal,
                            ExecutionStatus.SKIPPED_NO_POSITION,
                            total_filled,
                            attempts,
                            "skipped: no live position",
                        )
                    logger.warning("%s | %s | 可用资源不足", signal.console_event("风控"), code_label)
                    status = (
                        ExecutionStatus.PARTIALLY_FILLED_TIMEOUT
                        if total_filled
                        else ExecutionStatus.FAILED_RISK
                    )
                    return self._finish(
                        signal, status, total_filled, attempts,
                        "insufficient funds or position",
                    )
                if target_qty is None:
                    # 首次实时查询决定该信号的实际执行目标，兼容原有资源上限语义。
                    target_qty = attempt_qty
                    remaining_qty = target_qty
                logger.debug(
                    "💰 第%d次定价 | %s 最新价=%.3f 卖一=%s 买一=%s 委托价=%.3f 数量=%s",
                    attempt_no, signal.label, quote.last_price, quote.ask1, quote.bid1,
                    order_price, attempt_qty,
                )
                if signal.action == Action.SELL:
                    order_id = self._submit_order_with_halt_check(
                        signal, attempt_qty, order_price,
                    )
                assert order_id is not None
                attempts = attempt_no
                if attempts == 1:
                    # 延迟打点: 开始处理 → 首笔委托到达柜台。抢单优化就看这个数。
                    logger.debug(
                        "⏱️ 首笔委托耗时 | %s 处理→委托=%.0fms",
                        signal.label, (time.monotonic() - exec_started) * 1000,
                    )
            except BrokerOrderRejected as exc:
                # 同步 order_stock 明确返回失败，没有有效订单号，不写伪造 attempt。
                attempts = attempt_no
                if exc.kind == BrokerRejectionKind.HARD_STOP:
                    logger.error(
                        "%s | %s | 第%02d次 | 硬拒单终止 | 分类=%s | 原因=%s",
                        signal.console_event("失败"), code_label, attempts,
                        exc.kind.value, exc.reason,
                    )
                    status = (
                        ExecutionStatus.PARTIALLY_FILLED_TIMEOUT
                        if total_filled
                        else ExecutionStatus.FAILED_BROKER
                    )
                    return self._finish(signal, status, total_filled, attempts, exc.reason)
                logger.warning(
                    "%s | %s | 第%02d次 | 下单未受理 | 分类=%s | 原因=%s | 刷新重试",
                    signal.console_event("重试"), code_label, attempts,
                    exc.kind.value, exc.reason,
                )
                retry_delay = min(self.config.poll_interval_sec, 0.05)
                if retry_delay > 0:
                    time.sleep(retry_delay)
                continue
            except BrokerSubmissionUncertain as exc:
                attempts = attempt_no
                halt_reason = self._set_trading_halt(
                    f"order submission state is uncertain: {exc}; "
                    "restart only after manual MiniQMT reconciliation"
                )
                logger.critical(
                    "%s | %s | 第%02d次 | 下单受理状态不明，停止后续交易 | %s",
                    signal.console_event("停止"), code_label, attempts, exc,
                )
                status = (
                    ExecutionStatus.PARTIALLY_FILLED_TIMEOUT
                    if total_filled
                    else ExecutionStatus.FAILED_BROKER
                )
                return self._finish(
                    signal, status, total_filled, attempts, halt_reason,
                )
            except Exception as exc:
                logger.error(
                    "%s | %s | 券商下单失败 | %s",
                    signal.console_event("失败"), code_label, exc,
                )
                return self._finish(signal, ExecutionStatus.FAILED_BROKER, total_filled, attempts, str(exc))

            # ---- 记录委托 ----
            self.store.record_attempt(
                signal.signal_id,
                attempts,
                order_id,
                attempt_qty,
                order_price,
                ExecutionStatus.ORDER_SUBMITTED.value,
            )

            # ---- 轮询等终态 ----
            try:
                snapshot = self._wait_for_terminal_or_timeout(
                    order_id,
                    opening_sell_first_attempt=(
                        attempts == 1 and is_preopen_sell(signal)
                    ),
                )
            except Exception as exc:
                halt_reason = self._set_trading_halt(
                    f"order {order_id} state is uncertain: {exc}; "
                    "restart only after manual MiniQMT reconciliation"
                )
                logger.critical(
                    "%s | %s | 查单失败，订单终态不明，停止后续交易 | QMT单号=%s | %s",
                    signal.console_event("停止"), code_label, order_id, exc,
                )
                status = (
                    ExecutionStatus.PARTIALLY_FILLED_TIMEOUT
                    if total_filled
                    else ExecutionStatus.FAILED_BROKER
                )
                return self._finish(
                    signal, status, total_filled, attempts, halt_reason,
                )
            if snapshot.status not in _TERMINAL_ORDER_STATUSES:
                try:
                    # 撤单确认用独立宽限, 不从信号总预算里扣: 最后一次尝试必然贴着
                    # 总预算边界, 借用总预算等于一进门就超时, 会把普通的未成交撤单
                    # 误判成终态不明并停掉整条交易通道。
                    snapshot = self._cancel_and_wait_for_terminal(
                        order_id, time.monotonic() + self.config.cancel_confirm_timeout_sec,
                    )
                except Exception as exc:
                    known_filled = max(0, min(snapshot.filled_qty, attempt_qty))
                    total_filled += known_filled
                    self.store.update_attempt(order_id, snapshot.status.value, snapshot.filled_qty)
                    halt_reason = self._set_trading_halt(
                        f"order {order_id} cancel state is uncertain: {exc}; "
                        "restart only after manual MiniQMT reconciliation"
                    )
                    logger.critical(
                        "%s | %s | 撤单终态未确认，停止后续交易 | QMT单号=%s | %s",
                        signal.console_event("停止"), code_label, order_id, exc,
                    )
                    status = (
                        ExecutionStatus.PARTIALLY_FILLED_TIMEOUT
                        if total_filled
                        else ExecutionStatus.FAILED_BROKER
                    )
                    return self._finish(
                        signal, status, total_filled, attempts, halt_reason,
                    )
            self.store.update_attempt(order_id, snapshot.status.value, snapshot.filled_qty)

            # filled_qty 是单笔订单累计成交量；只按撤单确认后的最终快照入账一次。
            filled_this_attempt = max(0, min(snapshot.filled_qty, attempt_qty))
            total_filled += filled_this_attempt
            assert target_qty is not None
            remaining_qty = target_qty - total_filled

            logger.debug(
                "📊 第%d次尝试结果 | %s broker单号=%s 状态=%s 本次成交=%s 累计=%s/%s",
                attempts,
                signal.label,
                order_id,
                _broker_label(snapshot.status),
                filled_this_attempt,
                total_filled,
                target_qty,
            )

            # ---- 终态判断 ----
            if remaining_qty <= 0:
                logger.info(
                    "%s | %s | 第%02d次 | 挂 %.3f×%s | 全成 %s/%s | 总耗时 %.1fs",
                    signal.console_event("成交"), code_label, attempts, order_price,
                    attempt_qty, total_filled, target_qty, time.monotonic() - started,
                )
                return self._finish(signal, ExecutionStatus.FILLED, total_filled, attempts, "filled")

            if snapshot.status == BrokerOrderStatus.REJECTED:
                rejection_kind = snapshot.rejection_kind or BrokerRejectionKind.UNKNOWN
                rejection_reason = snapshot.rejection_reason or "券商未返回拒单原因"
                if rejection_kind == BrokerRejectionKind.HARD_STOP:
                    logger.error(
                        "%s | %s | 第%02d次 | 硬拒单终止 | 分类=%s | 原因=%s | 累计 %s/%s",
                        signal.console_event("失败"), code_label, attempts,
                        rejection_kind.value, rejection_reason, total_filled, target_qty,
                    )
                    status = (
                        ExecutionStatus.PARTIALLY_FILLED_TIMEOUT
                        if total_filled
                        else ExecutionStatus.FAILED_BROKER
                    )
                    return self._finish(
                        signal, status, total_filled, attempts, rejection_reason,
                    )
                logger.warning(
                    "%s | %s | 第%02d次 | 券商拒单 | 分类=%s | 原因=%s | "
                    "累计 %s/%s | 剩余 %s | 刷新重试",
                    signal.console_event("重试"), code_label, attempts,
                    rejection_kind.value, rejection_reason, total_filled, target_qty,
                    remaining_qty,
                )
                retry_delay = min(self.config.poll_interval_sec, 0.05)
                if retry_delay > 0:
                    time.sleep(retry_delay)
                continue

            # ---- 本笔订单已终态但信号仍有剩余，下一轮重新查资源和行情 ----
            bid_label = "-" if quote.bid1 is None else f"{quote.bid1:.3f}"
            ask_label = "-" if quote.ask1 is None else f"{quote.ask1:.3f}"
            if snapshot.status == BrokerOrderStatus.CANCELED:
                outcome = "部分成交→已撤" if filled_this_attempt else "未成→已撤"
            else:
                outcome = "本笔全成→继续"
            logger.info(
                "%s | %s | 第%02d次 | 行情 %.3f | 买/卖 %s/%s | 挂 %.3f×%s | "
                "%s | 本次 %s | 累计 %s/%s | 剩余 %s | %.1fs",
                signal.console_event("重试"), code_label, attempts, quote.last_price,
                bid_label, ask_label, order_price, attempt_qty, outcome,
                filled_this_attempt, total_filled, target_qty, remaining_qty,
                time.monotonic() - started,
            )

        # ---- 次数或时间用尽 ----
        effective_target = target_qty if target_qty is not None else signal.amount
        if total_filled:
            logger.warning(
                "%s | %s | 部分成交 %s/%s | 尝试 %s次 | %.1fs",
                signal.console_event("超时"), code_label, total_filled, effective_target,
                attempts, time.monotonic() - started,
            )
        else:
            logger.warning(
                "%s | %s | 未成交 | 尝试 %s次 | %.1fs",
                signal.console_event("超时"), code_label, attempts, time.monotonic() - started,
            )
        status = ExecutionStatus.PARTIALLY_FILLED_TIMEOUT if total_filled else ExecutionStatus.FAILED_TIMEOUT
        return self._finish(signal, status, total_filled, attempts, "attempt or duration limit reached")

    # -------------------------------------------------------------------
    # 内部方法
    # -------------------------------------------------------------------

    def _resolve_intent_amount(self, signal: TradeSignal) -> int:
        """意图型信号 → 具体股数（sell_all/sell_half 查持仓, auto_buy 查资金+行情）。"""
        if signal.quantity_mode == "sell_all":
            return resolve_sell_all(
                self.broker.query_available_position(signal.code)
            )
        if signal.quantity_mode == "sell_half":
            return resolve_sell_half(
                self.broker.query_available_position(signal.code),
                self.config.sell_half_insufficient_lot_mode,
            )
        if signal.quantity_mode == "auto_buy":
            quote = self.market_data.latest_quote(signal.code)
            return resolve_auto_buy(
                available_cash=self.broker.query_available_cash(),
                total_assets=self.broker.query_total_assets(),
                buy_count=signal.budget_group_size or 1,
                price=quote.last_price,
                max_single_position_pct=self.config.max_single_position_pct,
            )
        raise ValueError(f"invalid quantity_mode: {signal.quantity_mode}")

    def _wait_for_terminal_or_timeout(
        self,
        order_id: str,
        *,
        opening_sell_first_attempt: bool = False,
    ) -> OrderSnapshot:
        """轮询订单直到终态或单次委托超时。

        使用自适应轮询策略: 前 1 秒用快速间隔, 之后降速。
        这样在流动性好的快速成交场景下能更快确认成交。
        竞价时段(9:15~9:30)提交的委托要排队到开盘才可能成交, deadline 顺延。
        """
        if opening_sell_first_attempt:
            timeout = (
                _seconds_until_market_open() + _OPENING_SELL_RECONCILE_GRACE_SEC
            )
        else:
            timeout = self.config.order_timeout_sec + _seconds_until_market_open()
        deadline = time.monotonic() + timeout
        fast_deadline = time.monotonic() + 1.0  # 前 1 秒快速轮询
        last_snapshot = self.broker.get_order_snapshot(order_id)

        while time.monotonic() < deadline:
            if last_snapshot.status in {
                BrokerOrderStatus.FILLED,
                BrokerOrderStatus.CANCELED,
                BrokerOrderStatus.REJECTED,
            }:
                logger.debug(
                    "🔍 订单终态 | broker单号=%s 状态=%s 成交=%s",
                    order_id, _broker_label(last_snapshot.status), last_snapshot.filled_qty,
                )
                return last_snapshot

            # 自适应间隔: 前 1 秒快速轮询, 之后使用配置间隔
            if time.monotonic() < fast_deadline:
                interval = min(self.config.poll_interval_sec, 0.05)
            else:
                interval = self.config.poll_interval_sec

            if interval > 0:
                time.sleep(interval)
            last_snapshot = self.broker.get_order_snapshot(order_id)

        # 超时时返回最后一次快照, 上层根据成交量决定撤单和是否重挂。
        logger.debug(
            "⏱️ 订单轮询超时 | broker单号=%s 最后状态=%s 成交=%s",
            order_id, _broker_label(last_snapshot.status), last_snapshot.filled_qty,
        )
        return last_snapshot

    def _cancel_and_wait_for_terminal(self, order_id: str, deadline: float) -> OrderSnapshot:
        """提交撤单请求并等待 MiniQMT 返回真实终态。"""
        self.broker.cancel_order(order_id)
        logger.debug("🔙 等待撤单终态 | broker单号=%s", order_id)
        last_snapshot = self.broker.get_order_snapshot(order_id)
        while last_snapshot.status not in _TERMINAL_ORDER_STATUSES:
            if time.monotonic() >= deadline:
                raise TimeoutError(f"cancel not confirmed for order {order_id}")
            interval = min(self.config.poll_interval_sec, 0.05)
            if interval > 0:
                time.sleep(interval)
            last_snapshot = self.broker.get_order_snapshot(order_id)
        logger.debug(
            "🔙 撤单终态已确认 | broker单号=%s 状态=%s 成交=%s",
            order_id, _broker_label(last_snapshot.status), last_snapshot.filled_qty,
        )
        return last_snapshot

    def _locked_book_label(self, signal: TradeSignal, quote: Quote) -> str | None:
        """无对手盘(涨跌停封死)时返回日志标签, 正常返回 None。

        Quote 的 ask1/bid1 为 None 表示该侧盘口无档 —— QMT tick 里涨停时
        卖档为空、跌停时买档为空。需行情源提供五档数据, 否则会误跳过,
        因此该保护默认关闭, 由配置显式开启。
        """
        if signal.action == Action.SELL:
            if self.config.effective_limit_down_sell_mode() == "skip" and quote.bid1 is None:
                return "跌停无买盘"
            return None
        if (
            self.config.effective_limit_up_buy_mode() == "skip"
            and quote.ask1 is None
        ):
            return "涨停无卖盘"
        return None

    def _is_confirmed_limit_up(self, signal: TradeSignal, quote: Quote) -> bool:
        """仅在涨停价可用且最新价/买一已触及涨停时确认锁盘。"""
        if (
            signal.action != Action.BUY
            or quote.ask1 is not None
            or quote.high_limit is None
            or quote.high_limit <= 0
        ):
            return False
        tolerance = tick_size_for(signal.code) + 1e-9
        return any(
            price is not None and abs(price - quote.high_limit) <= tolerance
            for price in (quote.last_price, quote.bid1)
        )

    def _queue_buy_at_limit_up(
        self,
        signal: TradeSignal,
        quote: Quote,
        remaining_qty: int,
        total_filled: int,
        attempts: int,
        target_qty: int | None,
    ) -> ExecutionResult | _QueueBuyRetry:
        """涨停买单只挂一笔涨停价委托，成交或截止前不撤不重挂。"""
        code_label = signal.display_code

        def _fallback_skip(message: str) -> ExecutionResult:
            status = (
                ExecutionStatus.PARTIALLY_FILLED_TIMEOUT
                if total_filled
                else ExecutionStatus.SKIPPED_LIMIT_UP
            )
            return self._finish(signal, status, total_filled, attempts, message)

        high_limit = quote.high_limit
        if high_limit is None or high_limit <= 0:
            logger.warning(
                "%s | %s | 涨停排队降级跳过 | 行情源未提供涨停价",
                signal.console_event("跳过"), code_label,
            )
            return _fallback_skip("limit-up queue fallback: high_limit unavailable")

        wait_sec = _seconds_until_queue_buy_deadline(self.config.queue_buy_deadline)
        if wait_sec <= 0:
            logger.warning(
                "%s | %s | 涨停排队降级跳过 | 已过排队截止 %s",
                signal.console_event("跳过"), code_label, self.config.queue_buy_deadline,
            )
            return _fallback_skip("limit-up queue fallback: past queue deadline")

        with self._queue_buy_lock:
            if self._active_queue_buys >= self.config.max_concurrent_queue_buys:
                over_capacity = True
            else:
                self._active_queue_buys += 1
                over_capacity = False
        if over_capacity:
            logger.warning(
                "%s | %s | 涨停排队降级跳过 | 排队并发已满 %s",
                signal.console_event("跳过"), code_label,
                self.config.max_concurrent_queue_buys,
            )
            return _fallback_skip("limit-up queue fallback: queue capacity reached")

        try:
            attempt_no = attempts + 1
            effective_target = (
                target_qty if target_qty is not None else total_filled + remaining_qty
            )
            try:
                order_id, attempt_qty = self._submit_buy_with_cash_lock(
                    signal, remaining_qty, high_limit,
                )
            except BrokerOrderRejected as exc:
                if exc.kind == BrokerRejectionKind.HARD_STOP:
                    logger.error(
                        "%s | %s | 第%02d次 | 涨停排队硬拒单终止 | 分类=%s | 原因=%s",
                        signal.console_event("失败"), code_label, attempt_no,
                        exc.kind.value, exc.reason,
                    )
                    status = (
                        ExecutionStatus.PARTIALLY_FILLED_TIMEOUT
                        if total_filled
                        else ExecutionStatus.FAILED_BROKER
                    )
                    return self._finish(
                        signal, status, total_filled, attempt_no, exc.reason,
                    )
                logger.warning(
                    "%s | %s | 第%02d次 | 涨停排队未受理 | 分类=%s | 原因=%s | 刷新重试",
                    signal.console_event("重试"), code_label, attempt_no,
                    exc.kind.value, exc.reason,
                )
                return _QueueBuyRetry(effective_target, total_filled, attempt_no)
            except BrokerSubmissionUncertain as exc:
                halt_reason = self._set_trading_halt(
                    f"limit-up queue submission state is uncertain: {exc}; "
                    "restart only after manual MiniQMT reconciliation"
                )
                logger.critical(
                    "%s | %s | 涨停排队受理状态不明，停止后续交易 | %s",
                    signal.console_event("停止"), code_label, exc,
                )
                status = (
                    ExecutionStatus.PARTIALLY_FILLED_TIMEOUT
                    if total_filled
                    else ExecutionStatus.FAILED_BROKER
                )
                return self._finish(
                    signal, status, total_filled, attempt_no, halt_reason,
                )
            except Exception as exc:
                logger.error(
                    "%s | %s | 涨停排队下单失败 | %s",
                    signal.console_event("失败"), code_label, exc,
                )
                status = (
                    ExecutionStatus.PARTIALLY_FILLED_TIMEOUT
                    if total_filled
                    else ExecutionStatus.FAILED_BROKER
                )
                return self._finish(signal, status, total_filled, attempts, str(exc))

            if attempt_qty <= 0:
                logger.warning("%s | %s | 可用资金不足", signal.console_event("风控"), code_label)
                status = (
                    ExecutionStatus.PARTIALLY_FILLED_TIMEOUT
                    if total_filled
                    else ExecutionStatus.FAILED_RISK
                )
                return self._finish(
                    signal, status, total_filled, attempts,
                    "insufficient funds for limit-up queue buy",
                )
            assert order_id is not None
            if target_qty is None:
                effective_target = total_filled + attempt_qty

            attempts = attempt_no
            self.store.record_attempt(
                signal.signal_id,
                attempts,
                order_id,
                attempt_qty,
                high_limit,
                ExecutionStatus.QUEUED_LIMIT_UP.value,
            )
            self.store.update_signal_status(
                signal.signal_id, ExecutionStatus.QUEUED_LIMIT_UP, filled_qty=total_filled,
            )
            logger.info(
                "%s | %s | 涨停排队已挂 | %.3f×%s | 截止 %s | 保留队列位置",
                signal.console_event("竞价"), code_label, high_limit, attempt_qty,
                self.config.queue_buy_deadline,
            )

            mono_deadline = time.monotonic() + wait_sec
            snapshot: OrderSnapshot | None = None
            try:
                snapshot = self.broker.get_order_snapshot(order_id)
                while (
                    snapshot.status not in _TERMINAL_ORDER_STATUSES
                    and time.monotonic() < mono_deadline
                ):
                    interval = _QUEUE_BUY_POLL_INTERVAL_SEC
                    if interval > 0:
                        time.sleep(min(interval, max(0.0, mono_deadline - time.monotonic())))
                    snapshot = self.broker.get_order_snapshot(order_id)
            except Exception as exc:
                known_filled = (
                    max(0, min(snapshot.filled_qty, attempt_qty))
                    if snapshot is not None
                    else 0
                )
                reconciled_total = total_filled + known_filled
                if snapshot is not None:
                    self.store.update_attempt(
                        order_id, snapshot.status.value, snapshot.filled_qty,
                    )
                halt_reason = self._set_trading_halt(
                    f"order {order_id} state is uncertain: {exc}; "
                    "restart only after manual MiniQMT reconciliation"
                )
                logger.critical(
                    "%s | %s | 涨停排队查单失败，订单终态不明，停止后续交易 | "
                    "QMT单号=%s | %s",
                    signal.console_event("停止"), code_label, order_id, exc,
                )
                status = (
                    ExecutionStatus.PARTIALLY_FILLED_TIMEOUT
                    if reconciled_total
                    else ExecutionStatus.FAILED_BROKER
                )
                return self._finish(
                    signal, status, reconciled_total, attempts, halt_reason,
                )

            assert snapshot is not None

            if snapshot.status not in _TERMINAL_ORDER_STATUSES:
                try:
                    snapshot = self._cancel_and_wait_for_terminal(
                        order_id, time.monotonic() + self.config.cancel_confirm_timeout_sec,
                    )
                except Exception as exc:
                    known_filled = max(0, min(snapshot.filled_qty, attempt_qty))
                    reconciled_total = total_filled + known_filled
                    self.store.update_attempt(order_id, snapshot.status.value, snapshot.filled_qty)
                    halt_reason = self._set_trading_halt(
                        f"order {order_id} cancel state is uncertain: {exc}; "
                        "restart only after manual MiniQMT reconciliation"
                    )
                    logger.critical(
                        "%s | %s | 涨停排队撤单终态未确认，停止后续交易 | QMT单号=%s | %s",
                        signal.console_event("停止"), code_label, order_id, exc,
                    )
                    status = (
                        ExecutionStatus.PARTIALLY_FILLED_TIMEOUT
                        if reconciled_total
                        else ExecutionStatus.FAILED_BROKER
                    )
                    return self._finish(
                        signal, status, reconciled_total, attempts, halt_reason,
                    )

            self.store.update_attempt(order_id, snapshot.status.value, snapshot.filled_qty)
            filled_this_attempt = max(0, min(snapshot.filled_qty, attempt_qty))
            reconciled_total = total_filled + filled_this_attempt

            if snapshot.status == BrokerOrderStatus.FILLED or filled_this_attempt >= attempt_qty:
                if reconciled_total >= effective_target:
                    logger.info(
                        "%s | %s | 涨停排队成交 | %.3f×%s | 全成 %s/%s",
                        signal.console_event("成交"), code_label, high_limit,
                        attempt_qty, reconciled_total, effective_target,
                    )
                    return self._finish(
                        signal, ExecutionStatus.FILLED, reconciled_total, attempts,
                        "filled from limit-up queue",
                    )
                return _QueueBuyRetry(effective_target, reconciled_total, attempts)

            if snapshot.status == BrokerOrderStatus.REJECTED:
                rejection_kind = snapshot.rejection_kind or BrokerRejectionKind.UNKNOWN
                rejection_reason = snapshot.rejection_reason or "券商未返回拒单原因"
                if rejection_kind == BrokerRejectionKind.HARD_STOP:
                    logger.error(
                        "%s | %s | 涨停排队硬拒单终止 | 分类=%s | 原因=%s | 累计 %s/%s",
                        signal.console_event("失败"), code_label,
                        rejection_kind.value, rejection_reason,
                        reconciled_total, effective_target,
                    )
                    status = (
                        ExecutionStatus.PARTIALLY_FILLED_TIMEOUT
                        if reconciled_total
                        else ExecutionStatus.FAILED_BROKER
                    )
                    return self._finish(
                        signal, status, reconciled_total, attempts, rejection_reason,
                    )
                logger.warning(
                    "%s | %s | 涨停排队被拒 | 分类=%s | 原因=%s | 刷新重试",
                    signal.console_event("重试"), code_label,
                    rejection_kind.value, rejection_reason,
                )
                return _QueueBuyRetry(effective_target, reconciled_total, attempts)

            logger.warning(
                "%s | %s | 涨停排队到期 | 成交 %s/%s | 截止 %s",
                signal.console_event("超时"), code_label,
                reconciled_total, effective_target, self.config.queue_buy_deadline,
            )
            status = (
                ExecutionStatus.PARTIALLY_FILLED_TIMEOUT
                if reconciled_total
                else ExecutionStatus.LIMIT_UP_QUEUE_EXPIRED
            )
            return self._finish(
                signal, status, reconciled_total, attempts,
                "limit-up queue expired without full fill",
            )
        finally:
            with self._queue_buy_lock:
                self._active_queue_buys -= 1

    def _queue_sell_at_limit_down(
        self,
        signal: TradeSignal,
        quote: Quote,
        remaining_qty: int,
        total_filled: int,
        attempts: int,
    ) -> ExecutionResult:
        """跌停锁盘卖单的排队执行: 挂跌停价等待开板, 截止时间撤单收尾。

        与主循环的关键差异:
        - 委托价固定为跌停价 (已是最低卖价, 开板后按价格优先必然轮到, 无需重定价);
        - 不做 cancel+重挂 (重挂丢队列位置), 只在截止时刻撤一次单收尾;
        - 豁免 order_timeout_sec / max_attempts / max_total_duration_sec 与偏离度守卫
          (止损单被"偏离参考价太远"拦下是本末倒置; 竞价保护已有同类豁免先例);
        - 慢轮询 queue_sell_poll_interval_sec, 降低全天占用的开销。

        回退为 skip 的情形: 取不到跌停价 / 已过排队截止 / 排队并发已满。
        """
        code_label = signal.display_code

        def _fallback_skip(message: str) -> ExecutionResult:
            status = (
                ExecutionStatus.PARTIALLY_FILLED_TIMEOUT
                if total_filled
                else ExecutionStatus.SKIPPED_LIMIT_DOWN
            )
            return self._finish(signal, status, total_filled, attempts, message)

        low_limit = quote.low_limit
        if low_limit is None or low_limit <= 0:
            # 宁可跳过也不能用错误价格挂单。
            logger.warning(
                "%s | %s | 跌停排队降级跳过 | 行情源未提供跌停价",
                signal.console_event("跳过"), code_label,
            )
            return _fallback_skip("limit-down queue fallback: low_limit unavailable")

        wait_sec = _seconds_until_queue_sell_deadline(self.config.queue_sell_deadline)
        if wait_sec <= 0:
            logger.warning(
                "%s | %s | 跌停排队降级跳过 | 已过排队截止 %s",
                signal.console_event("跳过"), code_label, self.config.queue_sell_deadline,
            )
            return _fallback_skip("limit-down queue fallback: past queue deadline")

        with self._queue_sell_lock:
            if self._active_queue_sells >= self.config.max_concurrent_queue_sells:
                over_capacity = True
            else:
                self._active_queue_sells += 1
                over_capacity = False
        if over_capacity:
            logger.warning(
                "%s | %s | 跌停排队降级跳过 | 排队并发已满 %s",
                signal.console_event("跳过"), code_label,
                self.config.max_concurrent_queue_sells,
            )
            return _fallback_skip("limit-down queue fallback: queue capacity reached")

        try:
            attempt_qty = self._cap_attempt_to_available_resources(
                signal, remaining_qty, low_limit,
            )
            if attempt_qty <= 0:
                if total_filled == 0:
                    logger.warning(
                        "%s | %s | 实盘无可卖持仓 | 跳过",
                        signal.console_event("跳过"), code_label,
                    )
                    return self._finish(
                        signal,
                        ExecutionStatus.SKIPPED_NO_POSITION,
                        total_filled,
                        attempts,
                        "skipped: no live position",
                    )
                logger.warning("%s | %s | 可用持仓不足", signal.console_event("风控"), code_label)
                status = (
                    ExecutionStatus.PARTIALLY_FILLED_TIMEOUT
                    if total_filled
                    else ExecutionStatus.FAILED_RISK
                )
                return self._finish(
                    signal, status, total_filled, attempts,
                    "insufficient position for limit-down queue sell",
                )

            attempt_no = attempts + 1
            try:
                order_id = self._submit_order_with_halt_check(
                    signal, attempt_qty, low_limit,
                )
            except BrokerSubmissionUncertain as exc:
                halt_reason = self._set_trading_halt(
                    f"limit-down queue submission state is uncertain: {exc}; "
                    "restart only after manual MiniQMT reconciliation"
                )
                logger.critical(
                    "%s | %s | 跌停排队受理状态不明，停止后续交易 | %s",
                    signal.console_event("停止"), code_label, exc,
                )
                status = (
                    ExecutionStatus.PARTIALLY_FILLED_TIMEOUT
                    if total_filled
                    else ExecutionStatus.FAILED_BROKER
                )
                return self._finish(
                    signal,
                    status,
                    total_filled,
                    attempt_no,
                    halt_reason,
                )
            except Exception as exc:
                logger.error(
                    "%s | %s | 跌停排队下单失败 | %s",
                    signal.console_event("失败"), code_label, exc,
                )
                status = (
                    ExecutionStatus.PARTIALLY_FILLED_TIMEOUT
                    if total_filled
                    else ExecutionStatus.FAILED_BROKER
                )
                return self._finish(signal, status, total_filled, attempts, str(exc))

            attempts = attempt_no
            self.store.record_attempt(
                signal.signal_id,
                attempts,
                order_id,
                attempt_qty,
                low_limit,
                ExecutionStatus.QUEUED_LIMIT_DOWN.value,
            )
            # 中间态落库: 盘中可从 signals 表看到"排队中", 崩溃恢复时也有迹可循。
            self.store.update_signal_status(
                signal.signal_id, ExecutionStatus.QUEUED_LIMIT_DOWN, filled_qty=total_filled,
            )
            self._on_limit_down_queued(signal.signal_id)
            logger.info(
                "%s | %s | 跌停排队已挂 | %.3f×%s | 截止 %s | 排队等待开板",
                signal.console_event("竞价"), code_label, low_limit, attempt_qty,
                self.config.queue_sell_deadline,
            )

            # ---- 慢轮询直到终态或截止 ----
            mono_deadline = time.monotonic() + wait_sec
            snapshot: OrderSnapshot | None = None
            try:
                snapshot = self.broker.get_order_snapshot(order_id)
                while (
                    snapshot.status not in _TERMINAL_ORDER_STATUSES
                    and time.monotonic() < mono_deadline
                ):
                    interval = self.config.queue_sell_poll_interval_sec
                    if interval > 0:
                        time.sleep(min(interval, max(0.0, mono_deadline - time.monotonic())))
                    snapshot = self.broker.get_order_snapshot(order_id)
            except Exception as exc:
                known_filled = (
                    max(0, min(snapshot.filled_qty, attempt_qty))
                    if snapshot is not None
                    else 0
                )
                reconciled_total = total_filled + known_filled
                if snapshot is not None:
                    self.store.update_attempt(
                        order_id, snapshot.status.value, snapshot.filled_qty,
                    )
                halt_reason = self._set_trading_halt(
                    f"order {order_id} state is uncertain: {exc}; "
                    "restart only after manual MiniQMT reconciliation"
                )
                logger.critical(
                    "%s | %s | 跌停排队查单失败，订单终态不明，停止后续交易 | "
                    "QMT单号=%s | %s",
                    signal.console_event("停止"), code_label, order_id, exc,
                )
                status = (
                    ExecutionStatus.PARTIALLY_FILLED_TIMEOUT
                    if reconciled_total
                    else ExecutionStatus.FAILED_BROKER
                )
                return self._finish(
                    signal, status, reconciled_total, attempts, halt_reason,
                )

            assert snapshot is not None

            # ---- 截止收尾: 唯一一次撤单 ----
            if snapshot.status not in _TERMINAL_ORDER_STATUSES:
                try:
                    snapshot = self._cancel_and_wait_for_terminal(
                        order_id, time.monotonic() + self.config.cancel_confirm_timeout_sec,
                    )
                except Exception as exc:
                    known_filled = max(0, min(snapshot.filled_qty, attempt_qty))
                    total_filled += known_filled
                    self.store.update_attempt(order_id, snapshot.status.value, snapshot.filled_qty)
                    halt_reason = self._set_trading_halt(
                        f"order {order_id} cancel state is uncertain: {exc}; "
                        "restart only after manual MiniQMT reconciliation"
                    )
                    logger.critical(
                        "%s | %s | 排队单撤单终态未确认，停止后续交易 | QMT单号=%s | %s",
                        signal.console_event("停止"), code_label, order_id, exc,
                    )
                    status = (
                        ExecutionStatus.PARTIALLY_FILLED_TIMEOUT
                        if total_filled
                        else ExecutionStatus.FAILED_BROKER
                    )
                    return self._finish(
                        signal, status, total_filled, attempts, halt_reason,
                    )

            self.store.update_attempt(order_id, snapshot.status.value, snapshot.filled_qty)
            filled_this_attempt = max(0, min(snapshot.filled_qty, attempt_qty))
            total_filled += filled_this_attempt

            if snapshot.status == BrokerOrderStatus.FILLED or filled_this_attempt >= attempt_qty:
                logger.info(
                    "%s | %s | 跌停开板成交 | %.3f×%s | 全成 %s股",
                    signal.console_event("成交"), code_label, low_limit, attempt_qty, total_filled,
                )
                return self._finish(
                    signal, ExecutionStatus.FILLED, total_filled, attempts,
                    "filled after limit-down reopen",
                )

            if snapshot.status == BrokerOrderStatus.REJECTED:
                logger.error(
                    "%s | %s | 跌停排队被拒单 | 累计成交 %s股",
                    signal.console_event("失败"), code_label, total_filled,
                )
                status = (
                    ExecutionStatus.PARTIALLY_FILLED_TIMEOUT
                    if total_filled
                    else ExecutionStatus.FAILED_BROKER
                )
                return self._finish(
                    signal, status, total_filled, attempts, "broker rejected queue sell order",
                )

            # CANCELED: 截止收尾撤单, 或人工在 QMT 客户端撤单 —— 都按排队到期记账。
            logger.warning(
                "%s | %s | 跌停排队到期 | 成交 %s/%s | 截止 %s",
                signal.console_event("超时"), code_label, total_filled, attempt_qty,
                self.config.queue_sell_deadline,
            )
            status = (
                ExecutionStatus.PARTIALLY_FILLED_TIMEOUT
                if total_filled
                else ExecutionStatus.LIMIT_DOWN_QUEUE_EXPIRED
            )
            return self._finish(
                signal, status, total_filled, attempts,
                "limit-down queue expired without full fill",
            )
        finally:
            with self._queue_sell_lock:
                self._active_queue_sells -= 1

    def _submit_buy_with_cash_lock(
        self,
        signal: TradeSignal,
        requested_qty: int,
        order_price: float,
    ) -> tuple[str | None, int]:
        """原子完成买单的资金查询、整手裁剪和提交，避免并发重复占用现金。"""
        with self._buy_cash_submit_lock:
            quantity = self._cap_attempt_to_available_resources(
                signal, requested_qty, order_price,
            )
            if quantity <= 0:
                return None, 0
            return self._submit_order_with_halt_check(
                signal, quantity, order_price,
            ), quantity

    def _current_trading_halt_reason(self) -> str | None:
        with self._broker_submission_lock:
            return self._trading_halt_reason

    def _set_trading_halt(self, reason: str) -> str:
        with self._broker_submission_lock:
            if self._trading_halt_reason is None:
                self._trading_halt_reason = reason
            return self._trading_halt_reason

    def _submit_order_with_halt_check(
        self,
        signal: TradeSignal,
        quantity: int,
        order_price: float,
    ) -> str:
        """把熔断检查和委托提交放在同一临界区，关闭并发穿透窗口。"""
        with self._broker_submission_lock:
            if self._trading_halt_reason is not None:
                raise _TradingHalted(self._trading_halt_reason)
            try:
                return self.broker.submit_order(signal, quantity, order_price)
            except BrokerSubmissionUncertain as exc:
                self._trading_halt_reason = (
                    f"order submission state is uncertain: {exc}; "
                    "restart only after manual MiniQMT reconciliation"
                )
                raise

    def _cap_attempt_to_available_resources(
        self,
        signal: TradeSignal,
        requested_qty: int,
        order_price: float,
    ) -> int:
        """按本次实时委托价和 MiniQMT 当前资源计算可提交数量。"""
        code_label = signal.display_code
        if signal.action == Action.BUY:
            if order_price <= 0:
                raise ValueError(f"invalid order price: {order_price}")
            available_cash = self.broker.query_available_cash()
            max_shares = int(available_cash / order_price + 1e-9)
            capped_qty = (min(requested_qty, max_shares) // 100) * 100
            if capped_qty < requested_qty:
                logger.warning(
                    "%s | %s | 资金不足 | %s股 → %s股 | 可用 %.2f | 委托价 %.3f",
                    signal.console_event("风控"), code_label,
                    requested_qty, capped_qty, available_cash, order_price,
                )
            return capped_qty

        available_position = self.broker.query_available_position(signal.code)
        capped_qty = min(requested_qty, available_position)
        if capped_qty < requested_qty:
            logger.warning(
                "%s | %s | 持仓不足 | %s股 → %s股 | 可卖 %s股",
                signal.console_event("风控"), code_label,
                requested_qty, capped_qty, available_position,
            )
        return capped_qty

    def _finish(
        self,
        signal: TradeSignal,
        status: ExecutionStatus,
        filled_qty: int,
        attempts: int,
        message: str,
    ) -> ExecutionResult:
        """所有终态统一从这里写回 SQLite, 避免状态和返回结果不一致。"""
        self.store.update_signal_status(signal.signal_id, status, filled_qty=filled_qty)
        logger.debug(
            "💾 终态已写入SQLite | %s 状态=%s 成交=%s",
            signal.label, _status_label(status), filled_qty,
        )
        return ExecutionResult(
            signal_id=signal.signal_id,
            status=status,
            requested_qty=signal.amount,
            filled_qty=filled_qty,
            attempts=attempts,
            message=message,
        )

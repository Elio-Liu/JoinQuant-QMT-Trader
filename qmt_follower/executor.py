"""订单执行状态机。

核心职责: 接收 TradeSignal → 幂等去重 → 行情定价 → 下单 → 轮询 → 成交/撤单/重挂 → 终态记录。

对 BrokerAdapter / MarketDataAdapter 只依赖 Protocol, 不耦合具体实现。
"""

from __future__ import annotations

import datetime as dt
import logging
import time
from typing import Protocol

from qmt_follower.models import (
    Action,
    BrokerOrderStatus,
    ExecutionConfig,
    ExecutionResult,
    ExecutionStatus,
    OrderSnapshot,
    Quote,
    TradeSignal,
)
from qmt_follower.pricing import PriceDeviationError, calculate_order_price
from qmt_follower.store import SQLiteExecutionStore

logger = logging.getLogger(__name__)


class MarketDataAdapter(Protocol):
    """行情适配器协议。

    真实环境由 xtquant 提供行情快照, 测试环境可用假行情源替代。
    """

    def latest_quote(self, code: str) -> Quote:
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

    def submit_order(self, signal: TradeSignal, quantity: int, price: float) -> str:
        pass

    def get_order_snapshot(self, order_id: str) -> OrderSnapshot:
        pass

    def cancel_order(self, order_id: str) -> None:
        pass


# ---------------------------------------------------------------------------
# 状态名称中英文映射 —— 让日志中的状态值更易读
# ---------------------------------------------------------------------------

_STATUS_LABELS: dict[ExecutionStatus, str] = {
    ExecutionStatus.RECEIVED: "已接收",
    ExecutionStatus.ACCEPTED: "已登记",
    ExecutionStatus.DUPLICATE_IGNORED: "重复忽略",
    ExecutionStatus.ORDER_SUBMITTED: "已下单",
    ExecutionStatus.FILLED: "完全成交",
    ExecutionStatus.FAILED_TIMEOUT: "超时失败",
    ExecutionStatus.PARTIALLY_FILLED_TIMEOUT: "部分成交超时",
    ExecutionStatus.FAILED_RISK: "风控拒绝",
    ExecutionStatus.FAILED_BROKER: "券商失败",
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
    ):
        self.store = store
        self.market_data = market_data
        self.broker = broker
        self.config = config
        self._trading_halt_reason: str | None = None

    def execute(self, signal: TradeSignal) -> ExecutionResult:
        # 延迟打点起点: 从工作线程真正开始处理这条信号算起。
        exec_started = time.monotonic()
        short_code = signal.code.split(".", 1)[0]
        logger.debug("⚡ 开始执行信号 | signal_id=%s", signal.signal_id)

        # ---- 幂等去重 ----
        if not self.store.try_accept_signal(signal):
            existing = self.store.get_signal(signal.signal_id)
            logger.info(
                "%s | %s | 已有状态 %s | 成交 %s股",
                signal.console_event("重复"),
                short_code,
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

        # 上一笔订单撤单终态不明确时，禁止任何后续信号继续触达券商。
        if self._trading_halt_reason is not None:
            logger.critical(
                "%s | %s | 交易通道已停止 | %s",
                signal.console_event("停止"), short_code, self._trading_halt_reason,
            )
            return self._finish(
                signal,
                ExecutionStatus.FAILED_BROKER,
                0,
                0,
                self._trading_halt_reason,
            )

        # ---- 参数校验 ----
        if signal.amount <= 0:
            logger.warning("%s | %s | 数量非法 %s股", signal.console_event("风控"), short_code, signal.amount)
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
                signal.console_event("竞价"), short_code, auction_extra,
            )
        total_deadline = started + self.config.max_total_duration_sec + auction_extra

        while remaining_qty > 0 and attempts < self.config.max_attempts:
            # 总时长限制
            elapsed = time.monotonic() - started
            if elapsed > self.config.max_total_duration_sec + auction_extra:
                logger.warning(
                    "%s | %s | 总耗时 %.1fs | 超过上限 %ss",
                    signal.console_event("超时"), short_code, elapsed,
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
                order_price = calculate_order_price(signal, quote, self.config)
                attempt_qty = self._cap_attempt_to_available_resources(
                    signal, remaining_qty, order_price,
                )
                if attempt_qty <= 0:
                    logger.warning("%s | %s | 可用资源不足", signal.console_event("风控"), short_code)
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
                order_id = self.broker.submit_order(signal, attempt_qty, order_price)
                attempts = attempt_no
                if attempts == 1:
                    # 延迟打点: 开始处理 → 首笔委托到达柜台。抢单优化就看这个数。
                    logger.debug(
                        "⏱️ 首笔委托耗时 | %s 处理→委托=%.0fms",
                        signal.label, (time.monotonic() - exec_started) * 1000,
                    )
            except PriceDeviationError as exc:
                logger.warning(
                    "%s | %s | 行情偏离参考价超过 %.1f%% | %s",
                    signal.console_event("风控"), short_code,
                    self.config.max_deviation_from_signal_price_pct * 100,
                    exc,
                )
                status = ExecutionStatus.PARTIALLY_FILLED_TIMEOUT if total_filled else ExecutionStatus.FAILED_RISK
                return self._finish(signal, status, total_filled, attempts, str(exc))
            except Exception as exc:
                logger.error(
                    "%s | %s | 券商下单失败 | %s",
                    signal.console_event("失败"), short_code, exc,
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
            snapshot = self._wait_for_terminal_or_timeout(order_id)
            if snapshot.status not in _TERMINAL_ORDER_STATUSES:
                try:
                    snapshot = self._cancel_and_wait_for_terminal(order_id, total_deadline)
                except Exception as exc:
                    known_filled = max(0, min(snapshot.filled_qty, attempt_qty))
                    total_filled += known_filled
                    self.store.update_attempt(order_id, snapshot.status.value, snapshot.filled_qty)
                    self._trading_halt_reason = (
                        f"order {order_id} cancel state is uncertain: {exc}; "
                        "restart only after manual MiniQMT reconciliation"
                    )
                    logger.critical(
                        "%s | %s | 撤单终态未确认，停止后续交易 | QMT单号=%s | %s",
                        signal.console_event("停止"), short_code, order_id, exc,
                    )
                    status = (
                        ExecutionStatus.PARTIALLY_FILLED_TIMEOUT
                        if total_filled
                        else ExecutionStatus.FAILED_BROKER
                    )
                    return self._finish(
                        signal, status, total_filled, attempts, self._trading_halt_reason,
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
                    signal.console_event("成交"), short_code, attempts, order_price,
                    attempt_qty, total_filled, target_qty, time.monotonic() - started,
                )
                return self._finish(signal, ExecutionStatus.FILLED, total_filled, attempts, "filled")

            if snapshot.status == BrokerOrderStatus.REJECTED:
                logger.error(
                    "%s | %s | 第%02d次 | 券商拒单 | 累计 %s/%s",
                    signal.console_event("失败"), short_code, attempts, total_filled, target_qty,
                )
                status = ExecutionStatus.PARTIALLY_FILLED_TIMEOUT if total_filled else ExecutionStatus.FAILED_BROKER
                return self._finish(signal, status, total_filled, attempts, "broker rejected order")

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
                signal.console_event("重试"), short_code, attempts, quote.last_price,
                bid_label, ask_label, order_price, attempt_qty, outcome,
                filled_this_attempt, total_filled, target_qty, remaining_qty,
                time.monotonic() - started,
            )

        # ---- 次数或时间用尽 ----
        effective_target = target_qty if target_qty is not None else signal.amount
        if total_filled:
            logger.warning(
                "%s | %s | 部分成交 %s/%s | 尝试 %s次 | %.1fs",
                signal.console_event("超时"), short_code, total_filled, effective_target,
                attempts, time.monotonic() - started,
            )
        else:
            logger.warning(
                "%s | %s | 未成交 | 尝试 %s次 | %.1fs",
                signal.console_event("超时"), short_code, attempts, time.monotonic() - started,
            )
        status = ExecutionStatus.PARTIALLY_FILLED_TIMEOUT if total_filled else ExecutionStatus.FAILED_TIMEOUT
        return self._finish(signal, status, total_filled, attempts, "attempt or duration limit reached")

    # -------------------------------------------------------------------
    # 内部方法
    # -------------------------------------------------------------------

    def _wait_for_terminal_or_timeout(self, order_id: str) -> OrderSnapshot:
        """轮询订单直到终态或单次委托超时。

        使用自适应轮询策略: 前 1 秒用快速间隔, 之后降速。
        这样在流动性好的快速成交场景下能更快确认成交。
        竞价时段(9:15~9:30)提交的委托要排队到开盘才可能成交, deadline 顺延。
        """
        deadline = time.monotonic() + self.config.order_timeout_sec + _seconds_until_market_open()
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

    def _cap_attempt_to_available_resources(
        self,
        signal: TradeSignal,
        requested_qty: int,
        order_price: float,
    ) -> int:
        """按本次实时委托价和 MiniQMT 当前资源计算可提交数量。"""
        short_code = signal.code.split(".", 1)[0]
        if signal.action == Action.BUY:
            if order_price <= 0:
                raise ValueError(f"invalid order price: {order_price}")
            available_cash = self.broker.query_available_cash()
            max_shares = int(available_cash / order_price + 1e-9)
            capped_qty = (min(requested_qty, max_shares) // 100) * 100
            if capped_qty < requested_qty:
                logger.warning(
                    "%s | %s | 资金不足 | %s股 → %s股 | 可用 %.2f | 委托价 %.3f",
                    signal.console_event("风控"), short_code,
                    requested_qty, capped_qty, available_cash, order_price,
                )
            return capped_qty

        available_position = self.broker.query_available_position(signal.code)
        capped_qty = min(requested_qty, available_position)
        if capped_qty < requested_qty:
            logger.warning(
                "%s | %s | 持仓不足 | %s股 → %s股 | 可卖 %s股",
                signal.console_event("风控"), short_code,
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

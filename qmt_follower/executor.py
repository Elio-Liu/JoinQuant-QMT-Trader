"""订单执行状态机。

核心职责: 接收 TradeSignal → 幂等去重 → 行情定价 → 下单 → 轮询 → 成交/撤单/重挂 → 终态记录。

对 BrokerAdapter / MarketDataAdapter 只依赖 Protocol, 不耦合具体实现。
"""

from __future__ import annotations

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
    TradeSignal,
)
from qmt_follower.pricing import PriceDeviationError, calculate_order_price
from qmt_follower.store import SQLiteExecutionStore

logger = logging.getLogger(__name__)


class MarketDataAdapter(Protocol):
    """行情适配器协议。

    真实环境由 xtquant 提供最新价, 测试环境可用假行情源替代。
    """

    def latest_price(self, code: str) -> float:
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


def _status_label(status: ExecutionStatus) -> str:
    return _STATUS_LABELS.get(status, status.value)


def _broker_label(status: BrokerOrderStatus) -> str:
    return _BROKER_STATUS_LABELS.get(status, status.value)


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

    def execute(self, signal: TradeSignal) -> ExecutionResult:
        logger.debug("⚡ 开始执行信号 | signal_id=%s", signal.signal_id)

        # ---- 幂等去重 ----
        if not self.store.try_accept_signal(signal):
            existing = self.store.get_signal(signal.signal_id)
            logger.info(
                "⏭️ 重复信号已忽略 | signal_id=%s 已有状态=%s 已成交=%s股",
                signal.signal_id,
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

        logger.info(
            "🔖 信号已登记 | signal_id=%s 策略=%s 代码=%s 方向=%s 数量=%s 参考价=%.2f",
            signal.signal_id,
            signal.strategy_id,
            signal.code,
            signal.action.value,
            signal.amount,
            signal.reference_price,
        )

        # ---- 参数校验 ----
        if signal.amount <= 0:
            logger.warning("⚠️ 信号数量非法 | signal_id=%s amount=%s", signal.signal_id, signal.amount)
            return self._finish(signal, ExecutionStatus.FAILED_RISK, 0, 0, "amount must be positive")

        # ---- 数量上限适配: 买入看资金, 卖出看持仓 ----
        capped_amount = self._cap_to_available_resources(signal)
        if capped_amount <= 0:
            logger.warning(
                "⚠️ 可用资源不足, 无法执行 | signal_id=%s 方向=%s 请求=%s股",
                signal.signal_id, signal.action.value, signal.amount,
            )
            return self._finish(
                signal, ExecutionStatus.FAILED_RISK, 0, 0,
                "insufficient funds or position",
            )

        # ---- 主循环: 下单 → 轮询 → 成交/撤单/重挂 ----
        started = time.monotonic()
        target_qty = capped_amount
        remaining_qty = target_qty
        total_filled = 0
        attempts = 0

        while remaining_qty > 0 and attempts < self.config.max_attempts:
            # 总时长限制
            elapsed = time.monotonic() - started
            if elapsed > self.config.max_total_duration_sec:
                logger.warning(
                    "⏰ 总执行时间超限 | signal_id=%s 已耗时=%.1f秒 上限=%s秒",
                    signal.signal_id, elapsed, self.config.max_total_duration_sec,
                )
                break

            attempts += 1
            logger.debug(
                "🔄 第%d次尝试开始 | signal_id=%s 剩余=%s股 已成交=%s股",
                attempts, signal.signal_id, remaining_qty, total_filled,
            )

            # ---- 行情定价 ----
            try:
                latest_price = self.market_data.latest_price(signal.code)
                order_price = calculate_order_price(signal, latest_price, self.config)
                logger.info(
                    "💰 第%d次定价 | signal_id=%s 最新价=%.2f 委托价=%.2f 数量=%s",
                    attempts, signal.signal_id, latest_price, order_price, remaining_qty,
                )
                order_id = self.broker.submit_order(signal, remaining_qty, order_price)
            except PriceDeviationError as exc:
                logger.warning(
                    "⚠️ 价格偏离过大 | signal_id=%s 最新价偏离参考价超过%.1f%% | %s",
                    signal.signal_id,
                    self.config.max_deviation_from_signal_price_pct * 100,
                    exc,
                )
                status = ExecutionStatus.PARTIALLY_FILLED_TIMEOUT if total_filled else ExecutionStatus.FAILED_RISK
                return self._finish(signal, status, total_filled, attempts, str(exc))
            except Exception as exc:
                logger.error(
                    "❌ 券商下单失败 | signal_id=%s 错误=%s",
                    signal.signal_id, exc,
                )
                return self._finish(signal, ExecutionStatus.FAILED_BROKER, total_filled, attempts, str(exc))

            # ---- 记录委托 ----
            self.store.record_attempt(
                signal.signal_id,
                attempts,
                order_id,
                remaining_qty,
                order_price,
                ExecutionStatus.ORDER_SUBMITTED.value,
            )

            # ---- 轮询等终态 ----
            snapshot = self._wait_for_terminal_or_timeout(order_id)
            self.store.update_attempt(order_id, snapshot.status.value, snapshot.filled_qty)

            filled_this_attempt = max(0, min(snapshot.filled_qty, remaining_qty))
            total_filled += filled_this_attempt
            remaining_qty = target_qty - total_filled

            logger.info(
                "📊 第%d次尝试结果 | signal_id=%s broker单号=%s 状态=%s 本次成交=%s 累计=%s/%s",
                attempts,
                signal.signal_id,
                order_id,
                _broker_label(snapshot.status),
                filled_this_attempt,
                total_filled,
                target_qty,
            )

            # ---- 终态判断 ----
            if snapshot.status == BrokerOrderStatus.FILLED or remaining_qty <= 0:
                logger.info(
                    "🎯 完全成交 | signal_id=%s 成交=%s股 尝试=%s次 总耗时=%.1f秒",
                    signal.signal_id, total_filled, attempts, time.monotonic() - started,
                )
                return self._finish(signal, ExecutionStatus.FILLED, total_filled, attempts, "filled")

            if snapshot.status == BrokerOrderStatus.REJECTED:
                logger.error(
                    "❌ 券商拒单 | signal_id=%s broker单号=%s 已成交=%s股",
                    signal.signal_id, order_id, total_filled,
                )
                status = ExecutionStatus.PARTIALLY_FILLED_TIMEOUT if total_filled else ExecutionStatus.FAILED_BROKER
                return self._finish(signal, status, total_filled, attempts, "broker rejected order")

            # ---- 部分成交: 撤单, 剩余数量下一轮重挂 ----
            logger.info(
                "🔙 撤单重挂 | signal_id=%s 已成交=%s 剩余=%s broker单号=%s",
                signal.signal_id, total_filled, remaining_qty, order_id,
            )
            self.broker.cancel_order(order_id)

        # ---- 次数或时间用尽 ----
        if total_filled:
            logger.warning(
                "⏰ 部分成交超时 | signal_id=%s 已成交=%s/%s 尝试=%s次 总耗时=%.1f秒",
                signal.signal_id, total_filled, target_qty, attempts, time.monotonic() - started,
            )
        else:
            logger.warning(
                "⏰ 超时未成交 | signal_id=%s 尝试=%s次 总耗时=%.1f秒",
                signal.signal_id, attempts, time.monotonic() - started,
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
        """
        deadline = time.monotonic() + self.config.order_timeout_sec
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

    def _cap_to_available_resources(self, signal: TradeSignal) -> int:
        """根据账户实际可用资源（资金/持仓）上限适配下单数量。

        买入: 查询可用资金, 按参考价反算最大可买股数。
        卖出: 查询可用持仓, 不能超过可卖数量。
        若查询失败则保守返回原数量, 由券商侧兜底。
        """
        try:
            if signal.action == Action.BUY:
                available_cash = self.broker.query_available_cash()
                # 用参考价 + 滑点估算委托价, 反算最大可买股数（保守估计）
                order_price_est = signal.reference_price * (1 + self.config.buy_slippage_pct)
                if order_price_est <= 0:
                    return signal.amount
                max_shares = int(available_cash / order_price_est)
                if max_shares < signal.amount:
                    logger.warning(
                        "⚠️ 资金不足, 数量已调整 | signal_id=%s 原始=%s股 → 实际=%s股 可用资金=%.2f 估价=%.2f",
                        signal.signal_id, signal.amount, max_shares, available_cash, order_price_est,
                    )
                    return max_shares
            else:
                available_position = self.broker.query_available_position(signal.code)
                if available_position < signal.amount:
                    logger.warning(
                        "⚠️ 持仓不足, 数量已调整 | signal_id=%s 原始=%s股 → 实际=%s股 可用持仓=%s股",
                        signal.signal_id, signal.amount, available_position, available_position,
                    )
                    return available_position
        except Exception as exc:
            # 查询失败不阻塞执行: 保守使用原数量, 由券商侧在下单时兜底校验。
            logger.warning(
                "⚠️ 资源查询失败, 按原数量执行 | signal_id=%s 错误=%s",
                signal.signal_id, exc,
            )
        return signal.amount

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
            "💾 终态已写入SQLite | signal_id=%s 状态=%s 成交=%s",
            signal.signal_id, _status_label(status), filled_qty,
        )
        return ExecutionResult(
            signal_id=signal.signal_id,
            status=status,
            requested_qty=signal.amount,
            filled_qty=filled_qty,
            attempts=attempts,
            message=message,
        )

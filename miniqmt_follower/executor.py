"""订单执行状态机。

核心职责: 接收 TradeSignal → 幂等去重 → 行情定价 → 下单 → 轮询 → 成交/撤单/重挂 → 终态记录。

对外只依赖 BrokerAdapter / MarketDataAdapter 两个 Protocol, 不耦合 miniQMT 具体实现;
测试可注入假适配器独立验证整个状态机。

本模块约定:
- signal_id 是唯一幂等键, 重复信号直接忽略、绝不二次下单。
- 撤单必须确认到终态才算完成, 终态不明时熔断整条交易通道。
- 每次委托都基于最新行情与最新资金/持仓重新定价、裁量数量。
"""

from __future__ import annotations

import datetime as dt
import logging
import threading
import time
from collections.abc import Callable
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass, replace
from typing import Protocol

from miniqmt_follower.config import MachineScheduleConfig
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
    is_terminal_execution_status,
)
from miniqmt_follower.opening import is_preopen_sell
from miniqmt_follower.pricing import calculate_order_price, tick_size_for
from miniqmt_follower.sizing import resolve_sell_all, resolve_sell_half, shares_for_budget
from miniqmt_follower.store import SQLiteExecutionStore
from miniqmt_follower.strategy_models import AccountSnapshot

logger = logging.getLogger(__name__)

_RECOVERY_VISIBILITY_CHECKS = 3
_RECOVERY_VISIBILITY_INTERVAL_SEC = 0.1


class MarketDataAdapter(Protocol):
    """行情适配器协议。

    真实环境由 xtquant 提供行情快照, 测试环境可用假行情源替代。
    """

    def latest_quote(self, code: str) -> Quote:
        """返回某只股票的最新行情快照(最新价 + 买卖一档)。"""
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

    def query_account_snapshot(self) -> AccountSnapshot:
        """单次查询资产与全部持仓快照, 供预算/plan 展开等批量读取场景。"""
        pass

    def submit_order(self, signal: TradeSignal, quantity: int, price: float) -> str:
        """向券商提交一笔委托, 返回券商订单号。"""
        pass

    def query_orders_by_signal_id(self, signal_id: str) -> tuple[OrderSnapshot, ...]:
        """按下单备注中的 signal_id 查询本账号订单，供崩溃恢复核对。"""
        pass

    def get_order_snapshot(self, order_id: str) -> OrderSnapshot:
        """按订单号查询券商最新的订单状态快照。"""
        pass

    def cancel_order(self, order_id: str) -> None:
        """对指定订单发起撤单请求。"""
        pass


class _TradingHalted(RuntimeError):
    """交易通道已熔断，禁止新的委托触达券商。"""


class _GhostOrderSuspected(RuntimeError):
    """订单超过宽限期仍不出现在 QMT 账户委托清单，疑似幽灵单。

    由适配器以 NOT_VISIBLE 快照表达、轮询路径翻译成此异常；执行引擎捕获后
    走三重校验决定重挂还是熔断，绝不当作普通轮询异常直接熔断。
    """

    def __init__(self, order_id: str, snapshot: OrderSnapshot):
        super().__init__(f"order {order_id} not visible in QMT order list")
        self.order_id = order_id
        self.snapshot = snapshot


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
    ExecutionStatus.RECOVERY_REQUIRED: "需要人工核对",
}

_BROKER_STATUS_LABELS: dict[BrokerOrderStatus, str] = {
    BrokerOrderStatus.OPEN: "等待成交",
    BrokerOrderStatus.PARTIALLY_FILLED: "部分成交",
    BrokerOrderStatus.FILLED: "完全成交",
    BrokerOrderStatus.CANCELED: "已撤销",
    BrokerOrderStatus.REJECTED: "已拒绝",
    BrokerOrderStatus.NOT_VISIBLE: "不可见",
}

_TERMINAL_ORDER_STATUSES = {
    BrokerOrderStatus.FILLED,
    BrokerOrderStatus.CANCELED,
    BrokerOrderStatus.REJECTED,
}

_QUEUE_BUY_POLL_INTERVAL_SEC = 3.0
_OPENING_SELL_RECONCILE_GRACE_SEC = 0.5
# 行情取数失败的重试次数与退避间隔: 行情抖动不应直接杀死信号。
_QUOTE_RETRY_MAX = 3
_QUOTE_RETRY_DELAY_SEC = 0.2
# 连续竞价时段行情快照超龄时的有界重取次数与间隔(合计约 1 秒)。
_QUOTE_STALE_RETRY_MAX = 10
_QUOTE_STALE_RETRY_DELAY_SEC = 0.1
# 开盘首挂窗口内行情源繁忙, 超龄重取从 10 次收紧到 3 次, 不拖累首笔委托耗时。
_QUOTE_STALE_RETRY_OPENING_MAX = 3
# 集合竞价期间(9:15-9:30)没有成交, QMT 快照时间戳冻结在最后一笔成交,
# 9:30 开盘时读到的快照天然"超龄"。开盘后该宽限期内直接采信竞价快照。
_OPENING_SNAPSHOT_GRACE_SEC = 60
# 幽灵单自动重挂的连续上限: 超过即熔断。防交易通道坏死时空转刷单
# (每次报单都秒回单号却从不进柜台, 竞价时段重试上限会被放大到数百次)。
_GHOST_RESUBMIT_MAX = 2
# 价格类拒单后的强制刷新: 宽限豁免不再适用, 快照时间戳不前进就持续重取,
# 最多等这么久; 行情源确实停摆则带告警返回现有快照交盘口锚定兜底。
_PRICE_REJECT_QUOTE_REFRESH_SEC = 2.0
_PRICE_REJECT_QUOTE_REFRESH_POLL_SEC = 0.3
# 价格类拒单后的退避下限: 给行情源一点推送时间, 别把全部尝试烧在同一旧快照上。
_PRICE_REJECT_BACKOFF_SEC = 0.2


def _current_datetime() -> dt.datetime:
    """当前本地时间; 独立成函数便于测试替换, 与真实时钟解耦。"""
    return dt.datetime.now()


def _in_continuous_session(machine_schedule: MachineScheduleConfig) -> bool:
    """时效门控只在连续竞价时段生效: 盘前/集合竞价快照本来就不更新。

    测试可替换本模块级函数与真实时钟解耦。
    """
    session = machine_schedule.market_session
    now = dt.datetime.now().time()
    return (
        session.continuous_trading_start_at
        <= now
        < session.closing_call_auction_start_at
    )


def _in_opening_timeout_window(
    machine_schedule: MachineScheduleConfig, window_sec: float
) -> bool:
    """首挂耐心窗口: 连续竞价开始后的前 window_sec 秒。测试可替换本模块级函数。"""
    if window_sec <= 0:
        return False
    session = machine_schedule.market_session
    now = dt.datetime.now()
    if now.time() < session.continuous_trading_start_at:
        return False
    open_dt = now.replace(
        hour=session.continuous_trading_start_at.hour,
        minute=session.continuous_trading_start_at.minute,
        second=session.continuous_trading_start_at.second,
        microsecond=0,
    )
    return 0 <= (now - open_dt).total_seconds() <= window_sec


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

def _seconds_until_market_open(machine_schedule: MachineScheduleConfig) -> float:
    """配置的竞价时段内返回距连续竞价的秒数, 其他时间返回 0。

    竞价时段提交的委托会在券商排队、连续竞价开始才撮合。若仍按
    order_timeout_sec 判超时, 会把排队中的开盘单撤掉。因此竞价时段把
    单次轮询和总时长限制都顺延到连续竞价之后。
    测试可通过替换本模块级函数关闭该行为。
    """
    now = dt.datetime.now()
    market_session = machine_schedule.market_session
    if (
        market_session.call_auction_start_at
        <= now.time()
        < market_session.continuous_trading_start_at
    ):
        open_at = market_session.continuous_trading_start_at
        open_dt = now.replace(
            hour=open_at.hour,
            minute=open_at.minute,
            second=open_at.second,
            microsecond=0,
        )
        return (open_dt - now).total_seconds()
    return 0.0


def _seconds_until_queue_sell_cancel(cancel_at: dt.time) -> float:
    """返回距跌停排队截止时刻的秒数; 已过截止返回 0。

    截止时刻已在启动前校验为早于尾盘集合竞价; 到点主动撤单,
    让本地台账在收盘前落终态 (券商收盘也会自动废单, 主动撤是为了记账确定性)。
    测试可通过替换本模块级函数控制排队时长。
    """
    now = dt.datetime.now()
    cancel_dt = now.replace(
        hour=cancel_at.hour,
        minute=cancel_at.minute,
        second=cancel_at.second,
        microsecond=0,
    )
    return max(0.0, (cancel_dt - now).total_seconds())


def _seconds_until_queue_buy_cancel(cancel_at: dt.time) -> float:
    """返回距涨停买单排队截止时刻的秒数；已过截止返回 0。"""
    now = dt.datetime.now()
    cancel_dt = now.replace(
        hour=cancel_at.hour,
        minute=cancel_at.minute,
        second=cancel_at.second,
        microsecond=0,
    )
    return max(0.0, (cancel_dt - now).total_seconds())


# 批次预算闩锁保留的最近批次数。一天一个 plan, 32 条足够覆盖任何回看窗口。
_BUDGET_CACHE_MAX_ENTRIES = 32


def _budget_batch_key(signal: TradeSignal) -> str:
    """从派生买单 id 还原它所属的批次标识。

    PlanExecutor 生成的 id 形如 "<plan_id>-buy-<code>"; 去掉尾部即同一份日计划
    展开出来的所有买单共享的 key。不符合该形态(旧协议单发买单)时返回空串,
    表示不参与批次预算闩锁, 保持每条各自计算的原行为。
    """
    marker = f"-{Action.BUY.value}-"
    head, sep, _tail = signal.signal_id.rpartition(marker)
    if not sep or not head:
        return ""
    return head


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
        machine_schedule: MachineScheduleConfig,
        *,
        on_limit_down_queued: Callable[[str], None] | None = None,
        queue_executor: ThreadPoolExecutor | None = None,
        stop_event: threading.Event | None = None,
    ):
        self.store = store
        self.market_data = market_data
        self.broker = broker
        self.config = config
        self.machine_schedule = machine_schedule
        self._on_limit_down_queued = on_limit_down_queued or (lambda _signal_id: None)
        # 排队单专用线程池: 配置后跌停排队卖出挂单落库即移交慢轮询,
        # 卖出 worker 不再被全天占用; None 时保持原同步行为。
        self._queue_executor = queue_executor
        # 优雅退出事件: 排队慢轮询必须可被打断, 否则挂着排队单的进程
        # 退出时要等到排队截止(14:56:30)才肯收工。
        self._stop_event = stop_event
        self._queued_futures: dict[str, Future] = {}
        self._queued_futures_lock = threading.Lock()
        self._trading_halt_reason: str | None = None
        self._broker_submission_lock = threading.Lock()
        # 跌停排队卖出的并发闸: 排队单占用 worker 直到成交或截止,
        # 超过上限的新排队请求降级为 skip, 保证有 worker 留给正常信号。
        self._queue_sell_lock = threading.Lock()
        self._active_queue_sells = 0
        self._queue_buy_lock = threading.Lock()
        self._active_queue_buys = 0
        self._buy_cash_submit_lock = threading.Lock()
        # auto_buy 预算闩锁: 同一批派生买单只在第一条进来时快照一次资金,
        # 兄弟信号复用同一个绝对预算。否则分母固定为 N 而分子(可用资金)随
        # 兄弟报单递减, "等分可用资金"会退化成公比 (N-1)/N 的等比数列 ——
        # 5 只票只投得出约 67% 的资金, 且分配顺序随线程调度每天都不一样。
        self._budget_lock = threading.Lock()
        self._budget_cache: dict[str, float] = {}

    def _per_stock_budget(self, signal: TradeSignal) -> float:
        """取得(或首次快照并缓存)同一批 auto_buy 的单票预算。

        闩锁的 key 用派生信号 id 去掉 "-buy-<code>" 后缀得到的批次标识,
        同一个 plan 展开出来的买单共用一份预算快照。拿不到批次标识(旧协议
        单发买单)时退化为每条各自计算, 与原行为一致。
        """
        batch_key = _budget_batch_key(signal)
        with self._budget_lock:
            cached = self._budget_cache.get(batch_key) if batch_key else None
            if cached is not None:
                return cached
            # 单次账户快照一次带回可用资金与总资产, 省掉第二次锁获取与跨进程查询。
            account = self.broker.query_account_snapshot()
            available_cash = account.available_cash
            total_assets = account.total_assets
            budget = min(
                available_cash / max(signal.budget_group_size or 1, 1),
                total_assets * self.config.max_single_position_pct,
            )
            if batch_key:
                # 跟单进程一跑就是几个月, 而 key 里带日期 => 每个交易日新增一条,
                # 只增不删。批次标识按插入序保留最近若干个即可: 老批次早已终态,
                # 再被查到只会是重投, 那条路上幂等闸门已经先拦住了。
                while len(self._budget_cache) >= _BUDGET_CACHE_MAX_ENTRIES:
                    self._budget_cache.pop(next(iter(self._budget_cache)))
                self._budget_cache[batch_key] = budget
                logger.info(
                    "【买单】💰 批次预算已锁定 | %s | 可用 %.2f ÷ %s 只 | 单票 %.2f",
                    batch_key, available_cash, signal.budget_group_size or 1, budget,
                )
            return budget

    def recover(self, signal: TradeSignal) -> ExecutionResult:
        """核对 Redis 遗留信号与 QMT 订单；先找旧单，绝不直接重复提交。"""
        stored = self.store.get_signal_optional(signal.signal_id)
        if stored is None:
            return self.execute(signal)
        attempts = self.store.list_attempts(signal.signal_id)
        # 排队单已移交本进程专用线程跟踪时, pending 扫描的重投不得再派工:
        # 否则 recover 会以排队截止为限在卖出 worker 里同步轮询, 数十秒内
        # 占满整个卖出池, 盘中止损卖单全部饿死。占位结果非终态, 上层会
        # 重新挂接排队 future 的补 ACK 回调(幂等)。
        if stored.status in {
            ExecutionStatus.QUEUED_LIMIT_DOWN,
            ExecutionStatus.QUEUED_LIMIT_UP,
        }:
            with self._queued_futures_lock:
                queued = self._queued_futures.get(signal.signal_id)
            if queued is not None and not queued.done():
                logger.info(
                    "%s | %s | 排队单仍由专用线程跟踪 | 重投不重复派工",
                    signal.console_event("重试"), signal.display_code,
                )
                return ExecutionResult(
                    signal_id=signal.signal_id,
                    status=stored.status,
                    requested_qty=signal.amount,
                    filled_qty=stored.filled_qty,
                    attempts=len(attempts),
                    message="queue order still tracked by dedicated worker",
                )
        if stored.status == ExecutionStatus.RECOVERY_REQUIRED:
            halt_reason = self._set_trading_halt(
                f"unresolved recovery required for {signal.signal_id}"
            )
            return ExecutionResult(
                signal_id=signal.signal_id,
                status=ExecutionStatus.RECOVERY_REQUIRED,
                requested_qty=signal.amount,
                filled_qty=stored.filled_qty,
                attempts=len(attempts),
                message=halt_reason,
            )
        if is_terminal_execution_status(stored.status):
            return ExecutionResult(
                signal_id=signal.signal_id,
                status=stored.status,
                requested_qty=signal.amount,
                filled_qty=stored.filled_qty,
                attempts=len(attempts),
                message="recovered terminal signal",
            )

        try:
            snapshots: tuple[OrderSnapshot, ...] = ()
            for check in range(_RECOVERY_VISIBILITY_CHECKS):
                snapshots = self.broker.query_orders_by_signal_id(signal.signal_id)
                if snapshots or check + 1 >= _RECOVERY_VISIBILITY_CHECKS:
                    break
                time.sleep(_RECOVERY_VISIBILITY_INTERVAL_SEC)
        except Exception as exc:
            return self._recovery_required(
                signal, attempts, f"QMT订单核对失败: {exc}"
            )

        if not snapshots:
            return self._recovery_required(
                signal,
                attempts,
                "信号已开始处理，但QMT未返回对应订单；"
                "无法排除已报单但备注未可见",
            )

        by_id = {snapshot.order_id: snapshot for snapshot in snapshots}
        for snapshot in snapshots:
            self.store.upsert_recovered_attempt(
                signal.signal_id,
                snapshot.order_id,
                max(snapshot.quantity, snapshot.filled_qty),
                snapshot.price,
                snapshot.status.value,
                snapshot.filled_qty,
            )

        active = [
            snapshot for snapshot in snapshots
            if snapshot.status not in _TERMINAL_ORDER_STATUSES
        ]
        if len(active) > 1:
            return self._recovery_required(
                signal,
                self.store.list_attempts(signal.signal_id),
                "同一信号发现多笔未终态QMT订单",
            )
        if active:
            current = active[0]
            try:
                if stored.status in {
                    ExecutionStatus.QUEUED_LIMIT_DOWN,
                    ExecutionStatus.QUEUED_LIMIT_UP,
                }:
                    current = self._wait_recovered_queue_order(current, stored.status)
                else:
                    current = self._wait_for_terminal_or_timeout(current.order_id)
                if current.status not in _TERMINAL_ORDER_STATUSES:
                    current = self._cancel_and_wait_for_terminal(
                        current.order_id,
                        time.monotonic() + self.config.cancel_confirm_timeout_sec,
                    )
            except Exception as exc:
                return self._recovery_required(
                    signal,
                    self.store.list_attempts(signal.signal_id),
                    f"遗留QMT订单终态不明: {exc}",
                )
            current = replace(
                current,
                quantity=current.quantity or active[0].quantity,
                price=current.price or active[0].price,
            )
            by_id[current.order_id] = current
            self.store.upsert_recovered_attempt(
                signal.signal_id,
                current.order_id,
                max(current.quantity, current.filled_qty),
                current.price,
                current.status.value,
                current.filled_qty,
            )

        recovered_attempts = self.store.list_attempts(signal.signal_id)
        quantity_by_id = {
            attempt.broker_order_id: attempt.quantity for attempt in recovered_attempts
        }
        total_filled = sum(
            max(
                0,
                min(
                    snapshot.filled_qty,
                    snapshot.quantity or quantity_by_id.get(snapshot.order_id, 0),
                ),
            )
            for snapshot in by_id.values()
        )
        target_qty = signal.amount if signal.quantity_mode == "exact" else 0
        if target_qty <= 0 and recovered_attempts:
            target_qty = recovered_attempts[0].quantity
        if target_qty > 0 and total_filled >= target_qty:
            status = ExecutionStatus.FILLED
        elif total_filled > 0:
            status = ExecutionStatus.PARTIALLY_FILLED_TIMEOUT
        elif any(
            snapshot.status == BrokerOrderStatus.REJECTED
            for snapshot in by_id.values()
        ):
            status = ExecutionStatus.FAILED_BROKER
        else:
            status = ExecutionStatus.FAILED_TIMEOUT
        logger.warning(
            "%s | %s | 遗留订单核对完成 | 状态=%s | 成交=%s/%s",
            signal.console_event("重试"), signal.display_code,
            status.value, total_filled, target_qty,
        )
        return self._finish(
            signal,
            status,
            total_filled,
            len(recovered_attempts),
            "recovered from QMT order remarks",
        )

    def _wait_recovered_queue_order(
        self,
        initial: OrderSnapshot,
        stored_status: ExecutionStatus,
    ) -> OrderSnapshot:
        """恢复核单时等待排队单进入终态或到达排队截止, 复用正常排队的慢轮询语义。"""
        if stored_status == ExecutionStatus.QUEUED_LIMIT_DOWN:
            wait_sec = _seconds_until_queue_sell_cancel(
                self.machine_schedule.order_guard.limit_down_queue_cancel_at
            )
        else:
            wait_sec = _seconds_until_queue_buy_cancel(
                self.machine_schedule.order_guard.limit_up_queue_cancel_at
            )
        deadline = time.monotonic() + max(0.0, wait_sec)
        current = initial
        while (
            current.status not in _TERMINAL_ORDER_STATUSES
            and time.monotonic() < deadline
            and not self._stop_requested()
        ):
            interval = self.config.queue_sell_poll_interval_sec
            if interval > 0:
                time.sleep(min(interval, max(0.0, deadline - time.monotonic())))
            current = self.broker.get_order_snapshot(current.order_id)
        return current

    def _recovery_required(
        self,
        signal: TradeSignal,
        attempts,
        reason: str,
    ) -> ExecutionResult:
        halt_reason = self._set_trading_halt(
            f"recovery required for {signal.signal_id}: {reason}"
        )
        logger.critical(
            "%s | %s | 需要人工核对 | %s",
            signal.console_event("停止"), signal.display_code, reason,
        )
        stored_filled = self.store.get_signal(signal.signal_id).filled_qty
        attempts_filled = sum(
            max(0, min(attempt.filled_qty, attempt.quantity))
            for attempt in attempts
        )
        return self._finish(
            signal,
            ExecutionStatus.RECOVERY_REQUIRED,
            max(stored_filled, attempts_filled),
            len(attempts),
            halt_reason,
        )

    @staticmethod
    def _defer_for_recovery(
        signal: TradeSignal,
        filled_qty: int,
        attempts: int,
        message: str,
    ) -> ExecutionResult:
        """订单状态当下不明：保留 SQLite 中间态，交给 pending 恢复核单。

        不能在这里落 RECOVERY_REQUIRED，否则恢复分支会把它当成
        “已经人工介入”而不再自动查 QMT。只有恢复查询仍不能
        证明旧单状态时，_recovery_required 才把它持久化。
        """
        return ExecutionResult(
            signal_id=signal.signal_id,
            status=ExecutionStatus.RECOVERY_REQUIRED,
            requested_qty=signal.amount,
            filled_qty=filled_qty,
            attempts=attempts,
            message=message,
        )

    def execute(self, signal: TradeSignal) -> ExecutionResult:
        """执行一条信号的完整生命周期: 幂等去重 → 过期/意图解析 → 下单轮询主循环 → 终态落库。

        幂等闸与过期判断先于任何行情/账户查询, 重复或过期信号不触达券商。
        """
        # 延迟打点起点: 从工作线程真正开始处理这条信号算起。
        exec_started = time.monotonic()
        logger.debug("⚡ 开始执行信号 | signal_id=%s", signal.signal_id)

        # ---- 幂等去重(最优先): 重复信号连中文名解析与任何行情/账户查询都不做 ----
        if not self.store.try_accept_signal(signal):
            existing = self.store.get_signal(signal.signal_id)
            logger.info(
                "%s | %s | 已有状态 %s | 成交 %s股",
                signal.console_event("重复"),
                signal.display_code,
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

        # 日志展示用中文名: 只在首次受理时解析一次, 整条时间线统一显示 名称(代码)。
        if not signal.stock_name:
            signal = signal.with_stock_name(self.market_data.instrument_name(signal.code))
        code_label = signal.display_code

        logger.debug(
            "🔖 信号已登记 | %s @%.2f 策略=%s",
            signal.label,
            signal.reference_price,
            signal.strategy_id,
        )

        # 过期判断先于意图解析: 过期信号不做任何持仓/资金/行情查询, 也不触达券商。
        expire_status = self._signal_expiry_status(signal)
        if expire_status is not None:
            status, message = expire_status
            if status == ExecutionStatus.FAILED_RISK:
                logger.error(
                    "%s | %s | 过期时间非法 %r | 未下单",
                    signal.console_event("失败"), code_label, signal.expire_at,
                )
            else:
                logger.warning(
                    "%s | %s | %s | 未下单",
                    signal.console_event("过期"), code_label, message,
                )
            return self._finish(signal, status, 0, 0, message)

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
                budget_amount=None,
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

        return self._execute_main_loop(
            signal,
            exec_started=exec_started,
            remaining_qty=signal.amount,
            total_filled=0,
            attempts=0,
            target_qty=None,
        )

    def _execute_main_loop(
        self,
        signal: TradeSignal,
        *,
        exec_started: float,
        remaining_qty: int,
        total_filled: int,
        attempts: int,
        target_qty: int | None,
        initial_quote: Quote | None = None,
    ) -> ExecutionResult:
        """下单 → 轮询 → 成交/撤单/重挂主循环。

        从 execute() 的前置部分(幂等/意图/过期/熔断)之后进入; 涨停排队续跑
        也携带余量从这里重入, 开板后的常规定价语义与整体执行完全一致。
        initial_quote 供续跑场景复用刚取到的行情, 避免重复取数(测试里
        FakeMarketData 按队列消费, 重复取数会改变消费序列)。
        """
        code_label = signal.display_code

        # ---- 主循环: 下单 → 轮询 → 成交/撤单/重挂 ----
        started = time.monotonic()

        # 竞价时段收到的信号: 委托在券商排队至开盘撮合, 总时长限制顺延到开盘之后。
        auction_extra = _seconds_until_market_open(self.machine_schedule)
        if auction_extra > 0:
            logger.info(
                "%s | %s | 距开盘 %.0f秒 | 委托排队等待撮合",
                signal.console_event("竞价"), code_label, auction_extra,
            )
        # 连续幽灵单重挂计数: 超过上限熔断, 防交易通道坏死时按竞价放大后的
        # 重试次数空转刷单。
        ghost_resubmits = 0
        # 本信号执行内的价格类拒单计数: >0 时后续重挂强制刷新行情并改盘口锚定
        # 报价, 不再沿用竞价/开盘的百分比激进报价(旧快照+激进价=连续拒单)。
        price_rejects = 0
        while remaining_qty > 0 and attempts < self._effective_max_attempts():
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
                if initial_quote is not None:
                    quote = initial_quote
                    initial_quote = None
                else:
                    quote = self._latest_quote_with_retry(signal)
                    if price_rejects > 0:
                        # 价格拒单后必须刷新到新行情才允许重挂(开盘宽限豁免
                        # 不再适用), 拿不到新行情则保留现有快照交盘口锚定兜底。
                        quote = self._refresh_quote_after_price_rejection(
                            signal, quote
                        )

                # ---- 跌停锁盘卖单: 排队模式 ----
                # 开板窗口往往只有几秒, 放弃排队 = 放弃唯一逃生口。挂跌停价
                # 排队至截止时间, 期间不撤不重挂 (重挂丢队列位置); 无法排队
                # 的各种回退 (取不到跌停价/过截止/并发满) 在方法内降级为 skip。
                if (
                    signal.action == Action.SELL
                    and quote.bid1 is None
                    and self.config.effective_limit_down_sell_mode() == "queue"
                ):
                    # 取不到跌停价时既无法确认是否真跌停, 也无法安全定价
                    # (单边盘口下滑点定价会报到跌停价以下被交易所废单),
                    # 交给 _queue_sell_at_limit_down 内部走保守的降级跳过。
                    # 拿得到跌停价时, 必须确认价格真的贴在跌停上才进排队 ——
                    # 否则一次普通的买一档缺失就会把清仓单挂成跌停价并锁死
                    # 一个排队 worker 占用到配置的排队撤单时刻，且全天不重新定价。
                    if quote.low_limit is None or quote.low_limit <= 0 or (
                        self._is_confirmed_limit_down(signal, quote)
                    ):
                        return self._queue_sell_at_limit_down(
                            signal, quote, remaining_qty, total_filled, attempts,
                        )
                    logger.info(
                        "%s | %s | 买一无档但价格未触及跌停(最新 %.3f vs 跌停 %.3f) | 走常规定价",
                        signal.console_event("重试"), code_label,
                        quote.last_price, quote.low_limit,
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

                order_price = calculate_order_price(
                    signal, quote, self.config, self.machine_schedule,
                    prefer_book=price_rejects > 0,
                )
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
                if exc.kind == BrokerRejectionKind.PRICE:
                    price_rejects += 1
                    logger.warning(
                        "%s | %s | 价格拒单×%d | 下一挂强制刷新行情并改盘口锚定报价",
                        signal.console_event("重试"), code_label, price_rejects,
                    )
                self._sleep_before_retry(
                    price_rejection=exc.kind == BrokerRejectionKind.PRICE
                )
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
                return self._defer_for_recovery(
                    signal, total_filled, attempts, halt_reason,
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
                first_timeout = (
                    self._first_attempt_timeout(signal) if attempts == 1 else None
                )
                snapshot = self._wait_for_terminal_or_timeout(
                    order_id, timeout=first_timeout,
                )
                # 开盘首挂价格感知等待: 首笔未成交先看价格再决定撤不撤。
                # 等待时间有上限且从总预算里扣, 异常沿既有路径处置(查单异常
                # 仍会走到下面的 except → 交易熔断)。
                snapshot = self._maybe_wait_on_opening_gap(
                    signal, order_id, order_price, snapshot, first_timeout,
                    budget_room=max(
                        0.0,
                        self.config.max_total_duration_sec
                        + auction_extra
                        - (time.monotonic() - started),
                    ),
                    attempt_qty=attempt_qty,
                )
            except _GhostOrderSuspected as exc:
                # 疑似幽灵单: 报单受理后宽限期内从未出现在 QMT 委托清单。
                # 三重校验全空才确认并重挂(无单可撤, 直接走下一 attempt);
                # 直查可见 = 回报滞后, 恢复轮询原单; 任一有疑 = 熔断对账。
                verdict, detail, visible_snapshot = self._diagnose_ghost_order(
                    signal, exc.order_id, attempt_qty,
                )
                if verdict == "visible":
                    logger.warning(
                        "%s | %s | 幽灵单警报解除: 直查委托可见(回报滞后) | %s",
                        signal.console_event("重试"), code_label, detail,
                    )
                    assert visible_snapshot is not None
                    try:
                        snapshot = self._wait_for_terminal_or_timeout(
                            exc.order_id,
                            timeout=(
                                self._first_attempt_timeout(signal)
                                if attempts == 1 else None
                            ),
                        )
                    except _GhostOrderSuspected as second:
                        verdict, detail, visible_snapshot = (
                            "doubt", f"直查后二次不可见({second})", None,
                        )
                        snapshot = second.snapshot
                if verdict == "doubt":
                    halt_reason = self._set_trading_halt(
                        f"order {exc.order_id} not visible in QMT and {detail}; "
                        "restart only after manual MiniQMT reconciliation"
                    )
                    logger.critical(
                        "%s | %s | 订单不可见且%s, 停止后续交易 | QMT单号=%s",
                        signal.console_event("停止"), code_label, detail, exc.order_id,
                    )
                    self.store.update_attempt(
                        exc.order_id, exc.snapshot.status.value, exc.snapshot.filled_qty,
                    )
                    return self._defer_for_recovery(
                        signal, total_filled, attempts, halt_reason,
                    )
                if verdict == "ghost":
                    ghost_resubmits += 1
                    if (
                        not self.config.ghost_order_auto_resubmit
                        or ghost_resubmits > _GHOST_RESUBMIT_MAX
                    ):
                        stop_reason = (
                            "自动重挂已关闭"
                            if not self.config.ghost_order_auto_resubmit
                            else f"连续幽灵重挂超过{_GHOST_RESUBMIT_MAX}次"
                        )
                        halt_reason = self._set_trading_halt(
                            f"order {exc.order_id} is a ghost order ({detail}); "
                            f"{stop_reason} — restart only after manual "
                            "MiniQMT reconciliation"
                        )
                        logger.critical(
                            "%s | %s | 幽灵单已确认(%s), 但%s, 停止后续交易 | QMT单号=%s",
                            signal.console_event("停止"), code_label, detail,
                            stop_reason, exc.order_id,
                        )
                        self.store.update_attempt(
                            exc.order_id, exc.snapshot.status.value, exc.snapshot.filled_qty,
                        )
                        return self._defer_for_recovery(
                            signal, total_filled, attempts, halt_reason,
                        )
                    logger.warning(
                        "%s | %s | ⚠️ 幽灵单已确认 | %s | 本次0成交, 无单可撤, "
                        "立即重挂剩余%s股(第%d次幽灵重挂)",
                        signal.console_event("重试"), code_label, detail,
                        remaining_qty, ghost_resubmits,
                    )
                    self.store.update_attempt(
                        exc.order_id, exc.snapshot.status.value, exc.snapshot.filled_qty,
                    )
                    self._sleep_before_retry()
                    continue
            except Exception as exc:
                halt_reason = self._set_trading_halt(
                    f"order {order_id} state is uncertain: {exc}; "
                    "restart only after manual MiniQMT reconciliation"
                )
                logger.critical(
                    "%s | %s | 查单失败，订单终态不明，停止后续交易 | QMT单号=%s | %s",
                    signal.console_event("停止"), code_label, order_id, exc,
                )
                return self._defer_for_recovery(
                    signal, total_filled, attempts, halt_reason,
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
                    return self._defer_for_recovery(
                        signal, total_filled, attempts, halt_reason,
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
                if rejection_kind == BrokerRejectionKind.PRICE:
                    price_rejects += 1
                    logger.warning(
                        "%s | %s | 价格拒单×%d | 下一挂强制刷新行情并改盘口锚定报价",
                        signal.console_event("重试"), code_label, price_rejects,
                    )
                self._sleep_before_retry(
                    price_rejection=rejection_kind == BrokerRejectionKind.PRICE
                )
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

    # ---------------------------------------------------------------------------
    # 内部方法
    # ---------------------------------------------------------------------------

    def _effective_max_attempts(self) -> int:
        """竞价时段放宽尝试次数上限。

        盘前(9:15~9:30)柜台若拒收申报, 每次拒单都消耗一次 attempt。原先固定
        0.05 秒退避配 max_attempts=20, 意味着 20 次尝试在 1 秒内烧光 —— 盘前
        提交的开盘清仓单可能在距开盘还有很久时就已经放弃, 而卖单一进终态就会
        释放开盘买入屏障, 账户直接变成"只买不卖"。

        竞价时段改为按"剩余秒数 ÷ 退避间隔"给出足够次数, 让重试均匀铺满到开盘。
        总时长仍受 max_total_duration_sec + auction_extra 约束, 不会无限重试。
        """
        auction_extra = _seconds_until_market_open(self.machine_schedule)
        if auction_extra <= 0:
            return self.config.max_attempts
        interval = max(self._retry_backoff_sec(), 0.05)
        return max(self.config.max_attempts, int(auction_extra / interval) + 1)

    def _retry_backoff_sec(self) -> float:
        """拒单重试的退避间隔。

        竞价时段用较长间隔(把重试铺满到开盘, 顺便少打柜台); 连续竞价时段保持
        原来的快速重试 —— 抢单场景下每一次行情刷新都值钱。
        """
        auction_extra = _seconds_until_market_open(self.machine_schedule)
        if auction_extra > 0:
            return min(1.0, max(0.2, auction_extra / 20.0))
        return min(self.config.poll_interval_sec, 0.05)

    def _sleep_before_retry(self, *, price_rejection: bool = False) -> None:
        delay = self._retry_backoff_sec()
        if price_rejection:
            # 价格拒单后给行情源一点推送时间, 别把全部尝试烧在同一旧快照上。
            delay = max(delay, _PRICE_REJECT_BACKOFF_SEC)
        if delay > 0:
            time.sleep(delay)

    def _latest_quote_with_retry(self, signal: TradeSignal) -> Quote:
        """取行情快照, 失败短暂退避重试 —— 行情抖动不应直接杀死信号。

        对比券商拒单有分类重试, 行情一次性取数失败直接终态是对称性缺陷:
        9:25~9:30 行情源丢档、未订阅代码首次取价失败都会命中, 改为
        退避重试几次; 全部失败仍抛异常, 终态语义与改动前一致。
        成功路径再交给时效门控(连续竞价时段不用旧盘口报价)。
        """
        last_error: Exception | None = None
        for attempt in range(1, _QUOTE_RETRY_MAX + 1):
            try:
                quote = self.market_data.latest_quote(signal.code)
            except Exception as exc:
                last_error = exc
                logger.warning(
                    "%s | %s | 行情取数失败 第%d/%d次 | %s",
                    signal.console_event("重试"), signal.display_code,
                    attempt, _QUOTE_RETRY_MAX, exc,
                )
                if attempt < _QUOTE_RETRY_MAX:
                    time.sleep(_QUOTE_RETRY_DELAY_SEC)
                continue
            return self._refresh_stale_quote(signal, quote)
        raise last_error  # type: ignore[misc]

    def _refresh_stale_quote(self, signal: TradeSignal, quote: Quote) -> Quote:
        """快照超龄时有界重取; 仍不新鲜则带告警提交, 不让时效门控杀死信号。"""
        max_age = self.config.quote_max_age_sec
        if (
            max_age <= 0
            or quote.quote_time is None
            or not _in_continuous_session(self.machine_schedule)
        ):
            return quote
        now = _current_datetime()
        if self._auction_quote_within_opening_grace(quote, now):
            return quote
        retry_max = (
            _QUOTE_STALE_RETRY_OPENING_MAX
            if _in_opening_timeout_window(
                self.machine_schedule, self.config.opening_aggressive_window_sec
            )
            else _QUOTE_STALE_RETRY_MAX
        )
        for attempt in range(1, retry_max + 1):
            age = (_current_datetime() - quote.quote_time).total_seconds()
            if age <= max_age:
                if attempt > 1:
                    logger.debug(
                        "%s | %s | 行情快照已更新 | 第%d次重取 | 时效 %.2fs",
                        signal.console_event("重试"), signal.display_code,
                        attempt, age,
                    )
                return quote
            logger.warning(
                "%s | %s | 行情快照超龄 %.1fs(>%ss) | 第%d/%d次重取",
                signal.console_event("重试"), signal.display_code,
                age, max_age, attempt, retry_max,
            )
            time.sleep(_QUOTE_STALE_RETRY_DELAY_SEC)
            try:
                quote = self.market_data.latest_quote(signal.code)
            except Exception as exc:
                logger.warning(
                    "%s | %s | 快照重取失败, 保留现有快照 | %s",
                    signal.console_event("重试"), signal.display_code, exc,
                )
                return quote
        logger.warning(
            "%s | %s | 行情快照仍超龄 | 带告警提交",
            signal.console_event("重试"), signal.display_code,
        )
        return quote

    def _refresh_quote_after_price_rejection(
        self, signal: TradeSignal, quote: Quote
    ) -> Quote:
        """价格拒单后的强制刷新: 冻结的竞价快照不再豁免, 等行情推进或变新鲜。

        2026-08-31 复盘: 开盘宽限豁免让重挂反复用 09:25 的冻结盘口报同
        一个价, 连拒 20 次全被交易所价格笼子拒掉。这里反过来——快照时间戳不
        前进就持续重取(有界), 拿到推进后的行情才交给下一挂定价; 行情源确实
        停摆时带告警返回现有快照, 由盘口锚定报价兜底, 不新增熔断面。
        """
        if quote.quote_time is None:
            return quote
        deadline = time.monotonic() + _PRICE_REJECT_QUOTE_REFRESH_SEC
        prev_time = quote.quote_time
        while time.monotonic() < deadline:
            if quote.quote_time != prev_time and quote.last_price > 0:
                logger.debug(
                    "%s | %s | 价格拒单后行情已推进 | 重挂采用新快照",
                    signal.console_event("重试"), signal.display_code,
                )
                return quote
            age = (_current_datetime() - quote.quote_time).total_seconds()
            if 0 <= age <= self.config.quote_max_age_sec:
                return quote
            time.sleep(_PRICE_REJECT_QUOTE_REFRESH_POLL_SEC)
            try:
                fresh = self.market_data.latest_quote(signal.code)
            except Exception as exc:
                logger.warning(
                    "%s | %s | 价格拒单后行情重取失败 | %s",
                    signal.console_event("重试"), signal.display_code, exc,
                )
                continue
            if fresh is not None and fresh.last_price > 0:
                quote = fresh
        logger.warning(
            "%s | %s | 价格拒单后行情仍未推进 | 带告警按现有快照盘口锚定重挂",
            signal.console_event("重试"), signal.display_code,
        )
        return quote

    def _auction_quote_within_opening_grace(
        self, quote: Quote, now: dt.datetime
    ) -> bool:
        """竞价时段冻结的快照在开盘后宽限期内视为有效, 免于超龄重取。"""
        if quote.quote_time is None or quote.quote_time.date() != now.date():
            return False
        session = self.machine_schedule.market_session
        if not (
            session.call_auction_start_at
            <= quote.quote_time.time()
            < session.continuous_trading_start_at
        ):
            return False
        continuous_start = dt.datetime.combine(
            now.date(), session.continuous_trading_start_at
        )
        return (
            0
            <= (now - continuous_start).total_seconds()
            <= _OPENING_SNAPSHOT_GRACE_SEC
        )

    def _forget_queued_future(self, signal_id: str, future: Future) -> None:
        """从排队 future 表移除该 signal_id 的引用, 仅当登记的仍是同一个 future 时才删。"""
        with self._queued_futures_lock:
            if self._queued_futures.get(signal_id) is future:
                del self._queued_futures[signal_id]

    def queue_future_for(self, signal_id: str) -> Future | None:
        """排队单专用线程的 future, 供上层在终态时补发 ACK。"""
        with self._queued_futures_lock:
            return self._queued_futures.get(signal_id)

    def _resolve_intent_amount(self, signal: TradeSignal) -> int:
        """意图型信号 → 具体股数（sell_all/sell_half 查持仓, auto_buy 查资金+行情）。"""
        if signal.quantity_mode == "sell_all":
            return resolve_sell_all(
                self.broker.query_available_position(signal.code)
            )
        if signal.quantity_mode == "sell_half":
            return resolve_sell_half(
                self.broker.query_available_position(signal.code),
                signal.sell_half_insufficient_lot_mode
                or self.config.sell_half_insufficient_lot_mode,
            )
        if signal.quantity_mode == "auto_buy":
            quote = self._latest_quote_with_retry(signal)
            return shares_for_budget(
                budget=self._per_stock_budget(signal),
                price=quote.last_price,
                fee_buffer_pct=self.config.cash_fee_buffer_pct,
            )
        if signal.quantity_mode == "fixed_budget":
            if signal.action != Action.BUY or signal.budget_amount is None:
                raise ValueError("fixed_budget requires BUY and budget_amount")
            quote = self._latest_quote_with_retry(signal)
            # 协调器分配预算时已扣除策略 YAML 的现金预留；此处不重复扣费。
            return shares_for_budget(
                budget=signal.budget_amount,
                price=quote.last_price,
                fee_buffer_pct=0.0,
            )
        raise ValueError(f"invalid quantity_mode: {signal.quantity_mode}")

    def _signal_expiry_status(
        self, signal: TradeSignal
    ) -> tuple[ExecutionStatus, str] | None:
        """返回 (终态, 原因) 或 None。

        旧协议 expire_at(绝对时间字符串)优先兼容；新机制按
        sent_at_ms + execution.signal_expire_seconds 计算截止时间。
        sent_at_ms 缺失或配置为 0 时不做过期判断。
        """
        if signal.expire_at:
            try:
                expire_at = dt.datetime.strptime(
                    signal.expire_at, "%Y-%m-%d %H:%M:%S"
                )
            except ValueError:
                return (
                    ExecutionStatus.FAILED_RISK,
                    "invalid expire_at: %s" % signal.expire_at,
                )
            if dt.datetime.now() > expire_at:
                return (
                    ExecutionStatus.EXPIRED,
                    "signal expired at %s" % signal.expire_at,
                )
            return None

        seconds = self.config.signal_expire_seconds
        if seconds and signal.sent_at_ms is not None:
            deadline_ms = signal.sent_at_ms + int(seconds) * 1000
            now_ms = int(dt.datetime.now().timestamp() * 1000)
            if now_ms > deadline_ms:
                overdue_ms = now_ms - deadline_ms
                return (
                    ExecutionStatus.EXPIRED,
                    "signal expired: sent_at_ms=%s + %ss (overdue %sms)"
                    % (signal.sent_at_ms, seconds, overdue_ms),
                )
        return None
    def _first_attempt_timeout(self, signal: TradeSignal) -> float | None:
        """首笔委托的轮询超时; None 表示走默认 order_timeout_sec(+竞价顺延)。

        - 盘前卖单保持原有语义: 距开盘秒数 + 0.5s 报告宽限;
        - 盘前挂单买单(preopen_submit): 距开盘秒数 + opening_order_timeout_sec
          —— 开盘撮合的回执可能迟到 1~2 秒, 0.5s 就撤会向已成交的单发多余撤单;
        - 开盘窗口内的买单: opening_order_timeout_sec(>0 时) ——
          首挂即决战, 撤单确认慢的券商下 0.5s 就撤等于放弃。
        """
        if is_preopen_sell(signal, self.machine_schedule):
            return (
                _seconds_until_market_open(self.machine_schedule)
                + _OPENING_SELL_RECONCILE_GRACE_SEC
            )
        if (
            signal.action == Action.BUY
            and signal.preopen_submit
            and self.config.opening_order_timeout_sec > 0
        ):
            return (
                _seconds_until_market_open(self.machine_schedule)
                + self.config.opening_order_timeout_sec
            )
        if (
            signal.action == Action.BUY
            and self.config.opening_order_timeout_sec > 0
            and _in_opening_timeout_window(
                self.machine_schedule, self.config.opening_aggressive_window_sec
            )
        ):
            return self.config.opening_order_timeout_sec
        return None

    def _wait_for_terminal_or_timeout(
        self,
        order_id: str,
        *,
        timeout: float | None = None,
    ) -> OrderSnapshot:
        """轮询订单直到终态或单次委托超时。

        timeout=None 时用 order_timeout_sec + 竞价顺延; 显式传入则覆盖
        (开盘首挂耐心窗口用)。使用自适应轮询策略: 前 1 秒用快速间隔, 之后降速。
        这样在流动性好的快速成交场景下能更快确认成交。
        """
        if timeout is None:
            timeout = self.config.order_timeout_sec + _seconds_until_market_open(
                self.machine_schedule
            )
        deadline = time.monotonic() + timeout
        fast_deadline = time.monotonic() + 1.0  # 前 1 秒快速轮询
        last_snapshot = self.broker.get_order_snapshot(order_id)
        if last_snapshot.status == BrokerOrderStatus.NOT_VISIBLE:
            # 超时=0 的极端配置下 while 不执行, 首取也必须能上报幽灵单。
            raise _GhostOrderSuspected(order_id, last_snapshot)

        while time.monotonic() < deadline:
            if last_snapshot.status == BrokerOrderStatus.NOT_VISIBLE:
                # 疑似幽灵单: 报单受理后宽限期内从未出现在 QMT 委托清单。
                # 交给上层三重校验(直查/冻结资金/持仓), 不在这里当超时处理。
                raise _GhostOrderSuspected(order_id, last_snapshot)
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

    def _maybe_wait_on_opening_gap(
        self,
        signal: TradeSignal,
        order_id: str,
        order_price: float,
        snapshot: OrderSnapshot,
        first_timeout: float | None,
        budget_room: float,
        attempt_qty: int,
    ) -> OrderSnapshot:
        """开盘窗口内 BUY 首笔委托超时未成交时, 按最新价与挂单价的偏离决定
        是否再等一等; 不满足条件时原样返回, 行为与旧版完全一致。

        触发条件: 首笔(first_timeout 非 None)、BUY、订单非终态且零成交、
        两个配置项均 > 0、总预算仍有剩余。
        """
        if not (
            first_timeout is not None
            and signal.action == Action.BUY
            and snapshot.status not in _TERMINAL_ORDER_STATUSES
            and snapshot.filled_qty <= 0
            and self.config.opening_price_gap_wait_pct > 0
            and self.config.opening_price_gap_wait_max_sec > 0
            and budget_room > 0
        ):
            return snapshot
        return self._opening_price_gap_wait(
            signal, order_id, order_price, snapshot, budget_room, attempt_qty,
        )

    def _opening_price_gap_wait(
        self,
        signal: TradeSignal,
        order_id: str,
        order_price: float,
        snapshot: OrderSnapshot,
        budget_room: float,
        attempt_qty: int,
    ) -> OrderSnapshot:
        """开盘首挂未成交时的价格感知等待。

        首笔委托超时、零成交有两种可能: (a) 价格已经甩开挂单价, 单子等不回来;
        (b) 价格仍贴近挂单价, 只是回报迟到或被更高价买盘短暂插队(价格随时
        回来)。旧行为不区分一律撤单追价, (b) 场景会亲手撤掉一张健康的排队单。
        这里以 opening_price_gap_wait_poll_sec 为节奏(默认 0.2s, 开盘分秒必争;
        1.0 = 旧行为)用新鲜行情重判:
        - 最新价甩开挂单价超过阈值 → 立即结束等待, 交给上层走撤单追价;
        - 最新价偏离 ≤ opening_price_gap_wait_pct → 继续等, 等满
          opening_price_gap_wait_max_sec(再受总预算 budget_room 约束)为止;
        - 行情仍停在竞价旧快照(开盘宽限内的冻结时间戳) → 不下价格结论
          (旧价盲判曾让死单白等 6 秒), 并触发一次幽灵单三重校验;
        - 买单价格在挂单价下方却零成交 → 同样是"单子可能不在队列"的强信号
          (挂单高于市价而没成交, 只可能是回报丢失或死单), 每个等待窗口
          至多直查诊断一次: 健康单会被直查识别(可见)继续等, 死单立即交由
          主循环重挂, 不用等满窗口;
        - 行情取不到 → 宁等不撤: 撤掉健康排队单的代价 > 多等一秒, 同样有上限。
        等待期间订单进入终态(成交/撤单/拒单)或出现成交即返回;
        查单异常向上抛出, 沿既有"订单终态不明 → 交易熔断"路径处置。
        """
        code_label = signal.display_code
        event_label = signal.console_event("开盘耐心")
        wait_budget = min(
            self.config.opening_price_gap_wait_max_sec, budget_room,
        )
        poll_sec = self.config.opening_price_gap_wait_poll_sec or 1.0
        waited = 0.0
        ghost_checked = False
        logger.info(
            "%s | %s | 首挂未成交, 进入价格感知等待 | 挂单价=%.3f 阈值=%.1f%% 上限=%.1fs 重判=%.2fs",
            event_label, code_label, order_price,
            self.config.opening_price_gap_wait_pct * 100, wait_budget, poll_sec,
        )
        while waited < wait_budget:
            round_start = time.monotonic()
            try:
                quote = self._latest_quote_with_retry(signal)
            except Exception as exc:
                quote = None
                logger.warning(
                    "%s | %s | 行情取不到, 本轮继续等 | 已等 %.1fs | %s",
                    event_label, code_label, waited, exc,
                )
            stale_auction = (
                quote is not None
                and quote.quote_time is not None
                and self._auction_quote_within_opening_grace(
                    quote, _current_datetime(),
                )
            )
            gap: float | None = None
            if quote is not None:
                gap = (quote.last_price - order_price) / order_price
                if not stale_auction:
                    if gap > self.config.opening_price_gap_wait_pct:
                        logger.info(
                            "%s | %s | 价格已甩开挂单价 %.2f%% > %.2f%%, 停止等待, 撤单追价",
                            event_label, code_label, gap * 100,
                            self.config.opening_price_gap_wait_pct * 100,
                        )
                        break
                    logger.debug(
                        "%s | %s | 价格仍贴近挂单价(偏离 %.2f%% ≤ %.2f%%) | 剩余 %.1fs",
                        event_label, code_label, gap * 100,
                        self.config.opening_price_gap_wait_pct * 100,
                        wait_budget - waited,
                    )
                else:
                    logger.debug(
                        "%s | %s | 行情仍停在竞价旧快照, 本轮不下价格结论 | 剩余 %.1fs",
                        event_label, code_label, wait_budget - waited,
                    )
            # 死单/旧价即时诊断(每窗口至多一次): 价格站在成交侧却零成交,
            # 或判断数据仍是竞价旧快照时, 用三重校验确认订单是否真的在册。
            # 健康单被直查识别后继续等(毫秒级成本), 死单立即交给主循环重挂。
            if (
                not ghost_checked
                and snapshot.filled_qty <= 0
                and gap is not None
                and (stale_auction or gap < 0)
            ):
                ghost_checked = True
                verdict, detail, _visible = self._diagnose_ghost_order(
                    signal, order_id, attempt_qty,
                )
                if verdict in {"ghost", "doubt"}:
                    # 统一由主循环的幽灵单处置(重挂或熔断)收口, 这里只上报。
                    raise _GhostOrderSuspected(order_id, snapshot)
                logger.debug(
                    "%s | %s | 零成交原因排查: %s | 继续等完窗口",
                    event_label, code_label, detail,
                )
            step = min(poll_sec, wait_budget - waited)
            snapshot = self._wait_for_terminal_or_timeout(order_id, timeout=step)
            waited += time.monotonic() - round_start
            if snapshot.status in _TERMINAL_ORDER_STATUSES:
                return snapshot
            if snapshot.filled_qty > 0:
                logger.info(
                    "%s | %s | 等待期间已成交 %s股, 价格确在挂单价附近, 继续等完窗口",
                    event_label, code_label, snapshot.filled_qty,
                )
        return snapshot

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

    def _diagnose_ghost_order(
        self,
        signal: TradeSignal,
        order_id: str,
        attempt_qty: int,
    ) -> tuple[str, str, OrderSnapshot | None]:
        """疑似幽灵单的三重校验: 直查委托 → 冻结资金 → 成交痕迹。

        verdict:
        - "visible": 绕过缓存直查 QMT 委托清单, 订单其实在册 —— 只是回报/
          扫描滞后, 恢复轮询原单, 不重挂也不熔断;
        - "doubt":   存在订单在途或已成交的证据(冻结资金未清零/持仓痕迹),
          不能排除重挂双成交风险 —— 交给上层熔断 + 人工对账;
        - "ghost":   直查无单、无冻结资金、无成交痕迹 —— 确认报单未达柜台,
          可安全重挂剩余数量(无单可撤)。

        残留风险(诚实声明): 冻结资金回传本身可能延迟, 理论存在"真单在途但
        三查全空"的极小窗口; 因此重挂有连续上限, 超限即熔断。
        """
        code_label = signal.display_code
        try:
            visible_orders = self.broker.query_orders_by_signal_id(signal.signal_id)
        except Exception as exc:
            return "doubt", f"直查委托失败({exc})", None
        for order in visible_orders:
            if order.order_id == order_id:
                return "visible", "直查委托可见, 回报滞后", order
        try:
            account = self.broker.query_account_snapshot()
        except Exception as exc:
            return "doubt", f"账户快照失败({exc})", None
        if account.frozen_cash > 0:
            return "doubt", f"冻结资金 {account.frozen_cash:.2f} 未清零", None
        position = account.position_of(signal.code)
        if signal.action == Action.BUY:
            # 买单成交必然留下持仓: 有持仓 = 疑似已成交但回报丢失。
            # 账户本就持有该代码的边缘场景会误判为 doubt(fail-closed, 可接受;
            # 正常流程里已有持仓不补仓, 不会走到这里)。
            if position is not None and position.total_qty > 0:
                return (
                    "doubt",
                    f"持仓 {position.total_qty}股(疑似已成交回报丢失)",
                    None,
                )
        else:
            # 卖单成交必然消耗可用持仓: 可用持仓少于本次委托量 = 有成交痕迹。
            # 未减少 = 无成交发生, 重挂安全; 且重挂数量仍受可用持仓封顶,
            # 双卖不可能超出账户持仓。
            try:
                available = self.broker.query_available_position(signal.code)
            except Exception as exc:
                return "doubt", f"持仓查询失败({exc})", None
            if available < attempt_qty:
                return (
                    "doubt",
                    f"可用持仓 {available}股 < 委托 {attempt_qty}股(疑似已成交)",
                    None,
                )
        logger.warning(
            "%s | %s | 幽灵单三重校验通过 | 直查无单、无冻结资金、无成交痕迹",
            signal.console_event("重试"), code_label,
        )
        return "ghost", "直查无单、无冻结、无成交痕迹", None

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

    def _is_confirmed_limit_down(self, signal: TradeSignal, quote: Quote) -> bool:
        """仅在跌停价可用且最新价/卖一已触及跌停时确认锁盘。

        与买入侧 _is_confirmed_limit_up 对称。此前这里只判断 "bid1 is None",
        于是任何买一档缺失的行情 —— 停牌、盘口不连续发布的 9:25~9:30 时段、
        行情源丢档 —— 都会被当成跌停封死, 把一笔普通清仓单挂成跌停价、
        并锁死一个排队 worker 到配置的排队撤单时刻，且全天不重新定价。
        """
        if (
            signal.action != Action.SELL
            or quote.bid1 is not None
            or quote.low_limit is None
            or quote.low_limit <= 0
        ):
            return False
        tolerance = tick_size_for(signal.code) + 1e-9
        return any(
            price is not None and abs(price - quote.low_limit) <= tolerance
            for price in (quote.last_price, quote.ask1)
        )

    def _queue_buy_at_limit_up(
        self,
        signal: TradeSignal,
        quote: Quote,
        remaining_qty: int,
        total_filled: int,
        attempts: int,
        target_qty: int | None,
        *,
        force_sync: bool = False,
    ) -> ExecutionResult | _QueueBuyRetry:
        """涨停买单入场: 挂一笔涨停价委托并保留队列位置。

        - 全额接纳且配置了队列池时, 挂单落库后把慢轮询移交给 qmt-queue,
          返回 QUEUED_LIMIT_UP 占位结果 —— 买入 worker 立即空出;
        - 资金截断(非全额)保持同步路径, 由本函数内联慢轮询;
        - 提交期非硬拒单仍返回 _QueueBuyRetry, 由主循环刷新行情重试入场,
          保持既有语义。
        """
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

        queue_cancel_at = (
            self.machine_schedule.order_guard.limit_up_queue_cancel_at
        )
        wait_sec = _seconds_until_queue_buy_cancel(queue_cancel_at)
        if wait_sec <= 0:
            logger.warning(
                "%s | %s | 涨停排队降级跳过 | 已过排队截止 %s",
                signal.console_event("跳过"), code_label, queue_cancel_at,
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

        # 名额要么随本函数早退归还, 要么移交 _park_queued_buy 在终态后归还。
        slot_transferred = False
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
                return self._defer_for_recovery(
                    signal, total_filled, attempt_no, halt_reason,
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
                queue_cancel_at,
            )

            if (
                self._queue_executor is not None
                and not force_sync
                and attempt_qty >= remaining_qty
            ):
                # 全额接纳: 挂单落库后移交专用线程慢轮询, 买入 worker 立即空出。
                slot_transferred = True
                future = self._queue_executor.submit(
                    self._park_queued_buy,
                    signal, order_id, attempt_qty,
                    effective_target, total_filled, attempts, queue_cancel_at,
                )
                with self._queued_futures_lock:
                    self._queued_futures[signal.signal_id] = future
                future.add_done_callback(
                    lambda f: self._forget_queued_future(signal.signal_id, f)
                )
                logger.info(
                    "%s | %s | 涨停排队已转交专用线程 | 截止 %s",
                    signal.console_event("竞价"), code_label, queue_cancel_at,
                )
                return ExecutionResult(
                    signal_id=signal.signal_id,
                    status=ExecutionStatus.QUEUED_LIMIT_UP,
                    requested_qty=signal.amount,
                    filled_qty=total_filled,
                    attempts=attempts,
                    message="limit-up queue handed to dedicated worker",
                )
            # 资金截断或未配置专用池: 同步慢轮询, 名额由 _park_queued_buy 归还。
            slot_transferred = True
            return self._park_queued_buy(
                signal, order_id, attempt_qty,
                effective_target, total_filled, attempts, queue_cancel_at,
            )
        finally:
            if not slot_transferred:
                with self._queue_buy_lock:
                    self._active_queue_buys -= 1

    def _park_queued_buy(
        self,
        signal: TradeSignal,
        order_id: str,
        attempt_qty: int,
        effective_target: int,
        total_filled: int,
        attempts: int,
        queue_cancel_at: dt.time,
    ) -> ExecutionResult:
        """涨停排队买单的专用线程执行体: 慢轮询到终态或截止, 续跑在队列线程内闭环。

        与 _run_queued_sell 同构, 但买入侧有续跑语义: 非硬拒单与部分成交剩余
        需要重新入场排队(仍封板)或转常规定价主循环(开板)。所有续跑都在本线程
        内迭代完成 —— 不递归、不跨池, _queued_futures 的 future 只在真正终态
        完成, 上层补 ACK 语义不变。
        """
        code_label = signal.display_code
        try:
            current_order_id = order_id
            current_qty = attempt_qty
            target_qty = effective_target
            filled_total = total_filled
            tries = attempts
            while True:
                # ---- 慢轮询当前挂单到终态 / 截止 / 停机 ----
                deadline = time.monotonic() + max(
                    0.0, _seconds_until_queue_buy_cancel(queue_cancel_at),
                )
                snapshot: OrderSnapshot | None = None
                try:
                    snapshot = self.broker.get_order_snapshot(current_order_id)
                    while (
                        snapshot.status not in _TERMINAL_ORDER_STATUSES
                        and time.monotonic() < deadline
                        and not self._stop_requested()
                    ):
                        interval = _QUEUE_BUY_POLL_INTERVAL_SEC
                        if interval > 0:
                            time.sleep(
                                min(interval, max(0.0, deadline - time.monotonic()))
                            )
                        snapshot = self.broker.get_order_snapshot(current_order_id)
                except Exception as exc:
                    known_filled = (
                        max(0, min(snapshot.filled_qty, current_qty))
                        if snapshot is not None
                        else 0
                    )
                    reconciled_total = filled_total + known_filled
                    if snapshot is not None:
                        self.store.update_attempt(
                            current_order_id, snapshot.status.value,
                            snapshot.filled_qty,
                        )
                    halt_reason = self._set_trading_halt(
                        f"order {current_order_id} state is uncertain: {exc}; "
                        "restart only after manual MiniQMT reconciliation"
                    )
                    logger.critical(
                        "%s | %s | 涨停排队查单失败，订单终态不明，停止后续交易 | "
                        "QMT单号=%s | %s",
                        signal.console_event("停止"), code_label,
                        current_order_id, exc,
                    )
                    return self._defer_for_recovery(
                        signal, reconciled_total, tries, halt_reason,
                    )

                assert snapshot is not None

                if snapshot.status not in _TERMINAL_ORDER_STATUSES:
                    try:
                        snapshot = self._cancel_and_wait_for_terminal(
                            current_order_id,
                            time.monotonic() + self.config.cancel_confirm_timeout_sec,
                        )
                    except Exception as exc:
                        known_filled = max(0, min(snapshot.filled_qty, current_qty))
                        reconciled_total = filled_total + known_filled
                        self.store.update_attempt(
                            current_order_id, snapshot.status.value,
                            snapshot.filled_qty,
                        )
                        halt_reason = self._set_trading_halt(
                            f"order {current_order_id} cancel state is uncertain: "
                            f"{exc}; restart only after manual MiniQMT reconciliation"
                        )
                        logger.critical(
                            "%s | %s | 涨停排队撤单终态未确认，停止后续交易 | "
                            "QMT单号=%s | %s",
                            signal.console_event("停止"), code_label,
                            current_order_id, exc,
                        )
                        return self._defer_for_recovery(
                            signal, reconciled_total, tries, halt_reason,
                        )

                self.store.update_attempt(
                    current_order_id, snapshot.status.value, snapshot.filled_qty,
                )
                filled_this_attempt = max(0, min(snapshot.filled_qty, current_qty))
                filled_total += filled_this_attempt

                # ---- 终态分派 ----
                if (
                    snapshot.status == BrokerOrderStatus.FILLED
                    or filled_this_attempt >= current_qty
                ):
                    if filled_total >= target_qty:
                        logger.info(
                            "%s | %s | 涨停排队成交 | %s×%s | 全成 %s/%s",
                            signal.console_event("成交"), code_label,
                            snapshot.price, current_qty, filled_total, target_qty,
                        )
                        return self._finish(
                            signal, ExecutionStatus.FILLED, filled_total, tries,
                            "filled from limit-up queue",
                        )
                    # 部分成交剩余: 落入续跑
                elif snapshot.status == BrokerOrderStatus.REJECTED:
                    rejection_kind = (
                        snapshot.rejection_kind or BrokerRejectionKind.UNKNOWN
                    )
                    rejection_reason = (
                        snapshot.rejection_reason or "券商未返回拒单原因"
                    )
                    if rejection_kind == BrokerRejectionKind.HARD_STOP:
                        logger.error(
                            "%s | %s | 涨停排队硬拒单终止 | 分类=%s | 原因=%s | "
                            "累计 %s/%s",
                            signal.console_event("失败"), code_label,
                            rejection_kind.value, rejection_reason,
                            filled_total, target_qty,
                        )
                        status = (
                            ExecutionStatus.PARTIALLY_FILLED_TIMEOUT
                            if filled_total
                            else ExecutionStatus.FAILED_BROKER
                        )
                        return self._finish(
                            signal, status, filled_total, tries, rejection_reason,
                        )
                    logger.warning(
                        "%s | %s | 涨停排队被拒 | 分类=%s | 原因=%s | 刷新重试",
                        signal.console_event("重试"), code_label,
                        rejection_kind.value, rejection_reason,
                    )
                    # 落入续跑
                else:
                    # CANCELED: 截止/停机收尾撤单, 或人工在 QMT 客户端撤单
                    logger.warning(
                        "%s | %s | 涨停排队到期 | 成交 %s/%s | 截止 %s",
                        signal.console_event("超时"), code_label,
                        filled_total, target_qty, queue_cancel_at,
                    )
                    status = (
                        ExecutionStatus.PARTIALLY_FILLED_TIMEOUT
                        if filled_total
                        else ExecutionStatus.LIMIT_UP_QUEUE_EXPIRED
                    )
                    return self._finish(
                        signal, status, filled_total, tries,
                        "limit-up queue expired without full fill",
                    )

                # ---- 续跑: 刷新行情决定重新排队还是转常规定价 ----
                remaining_qty = target_qty - filled_total
                if remaining_qty <= 0:
                    return self._finish(
                        signal, ExecutionStatus.FILLED, filled_total, tries,
                        "filled from limit-up queue",
                    )
                quote = self._latest_quote_with_retry(signal)
                if not self._is_confirmed_limit_up(signal, quote):
                    logger.info(
                        "%s | %s | 涨停已打开 | 余量 %s 转常规定价",
                        signal.console_event("重试"), code_label, remaining_qty,
                    )
                    return self._execute_main_loop(
                        signal,
                        exec_started=time.monotonic(),
                        remaining_qty=remaining_qty,
                        total_filled=filled_total,
                        attempts=tries,
                        target_qty=target_qty,
                        initial_quote=quote,
                    )
                if _seconds_until_queue_buy_cancel(queue_cancel_at) <= 0:
                    status = (
                        ExecutionStatus.PARTIALLY_FILLED_TIMEOUT
                        if filled_total
                        else ExecutionStatus.SKIPPED_LIMIT_UP
                    )
                    return self._finish(
                        signal, status, filled_total, tries,
                        "limit-up queue fallback: past queue deadline",
                    )
                # 仍封板: 本线程直接重新入场, 名额已由本任务持有, 不再占买卖 worker。
                attempt_no = tries + 1
                try:
                    current_order_id, current_qty = self._submit_buy_with_cash_lock(
                        signal, remaining_qty, quote.high_limit,
                    )
                except BrokerOrderRejected as exc:
                    if exc.kind == BrokerRejectionKind.HARD_STOP:
                        logger.error(
                            "%s | %s | 第%02d次 | 涨停排队硬拒单终止 | "
                            "分类=%s | 原因=%s",
                            signal.console_event("失败"), code_label, attempt_no,
                            exc.kind.value, exc.reason,
                        )
                        status = (
                            ExecutionStatus.PARTIALLY_FILLED_TIMEOUT
                            if filled_total
                            else ExecutionStatus.FAILED_BROKER
                        )
                        return self._finish(
                            signal, status, filled_total, attempt_no, exc.reason,
                        )
                    logger.warning(
                        "%s | %s | 第%02d次 | 涨停排队重挂未受理 | "
                        "分类=%s | 原因=%s | 刷新重试",
                        signal.console_event("重试"), code_label, attempt_no,
                        exc.kind.value, exc.reason,
                    )
                    tries = attempt_no
                    retry_delay = min(self.config.poll_interval_sec, 0.05)
                    if retry_delay > 0:
                        time.sleep(retry_delay)
                    continue
                except BrokerSubmissionUncertain as exc:
                    halt_reason = self._set_trading_halt(
                        f"limit-up queue submission state is uncertain: {exc}; "
                        "restart only after manual MiniQMT reconciliation"
                    )
                    logger.critical(
                        "%s | %s | 涨停排队重挂受理状态不明，停止后续交易 | %s",
                        signal.console_event("停止"), code_label, exc,
                    )
                    return self._defer_for_recovery(
                        signal, filled_total, attempt_no, halt_reason,
                    )
                except Exception as exc:
                    logger.error(
                        "%s | %s | 涨停排队重挂失败 | %s",
                        signal.console_event("失败"), code_label, exc,
                    )
                    status = (
                        ExecutionStatus.PARTIALLY_FILLED_TIMEOUT
                        if filled_total
                        else ExecutionStatus.FAILED_BROKER
                    )
                    return self._finish(
                        signal, status, filled_total, tries, str(exc),
                    )
                if current_qty <= 0:
                    logger.warning(
                        "%s | %s | 可用资金不足",
                        signal.console_event("风控"), code_label,
                    )
                    status = (
                        ExecutionStatus.PARTIALLY_FILLED_TIMEOUT
                        if filled_total
                        else ExecutionStatus.FAILED_RISK
                    )
                    return self._finish(
                        signal, status, filled_total, tries,
                        "insufficient funds for limit-up queue relist",
                    )
                tries = attempt_no
                self.store.record_attempt(
                    signal.signal_id,
                    tries,
                    current_order_id,
                    current_qty,
                    quote.high_limit,
                    ExecutionStatus.QUEUED_LIMIT_UP.value,
                )
                self.store.update_signal_status(
                    signal.signal_id, ExecutionStatus.QUEUED_LIMIT_UP,
                    filled_qty=filled_total,
                )
                logger.info(
                    "%s | %s | 涨停排队重新挂 | %s×%s | 保留队列位置",
                    signal.console_event("竞价"), code_label, quote.high_limit,
                    current_qty,
                )
                continue
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

        配置专用线程池时, 挂单落库后把慢轮询移交过去并立即返回占位结果 ——
        排队单不再全天占用卖出 worker。未配置时走原同步路径, 行为与旧版一致。

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

        queue_cancel_at = (
            self.machine_schedule.order_guard.limit_down_queue_cancel_at
        )
        wait_sec = _seconds_until_queue_sell_cancel(queue_cancel_at)
        if wait_sec <= 0:
            logger.warning(
                "%s | %s | 跌停排队降级跳过 | 已过排队截止 %s",
                signal.console_event("跳过"), code_label, queue_cancel_at,
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

        if self._queue_executor is None:
            # 未配置专用池: 完全保持旧行为(同步挂单+慢轮询)。
            return self._run_queued_sell(
                signal, remaining_qty, total_filled, attempts,
                low_limit, queue_cancel_at, wait_sec,
            )

        future = self._queue_executor.submit(
            self._run_queued_sell,
            signal, remaining_qty, total_filled, attempts,
            low_limit, queue_cancel_at, wait_sec,
        )
        with self._queued_futures_lock:
            self._queued_futures[signal.signal_id] = future
        future.add_done_callback(
            lambda f: self._forget_queued_future(signal.signal_id, f)
        )
        logger.info(
            "%s | %s | 跌停排队已转交专用线程 | 截止 %s",
            signal.console_event("竞价"), code_label, queue_cancel_at,
        )
        # 占位结果: 主 worker 立即返回, 队列慢轮询在专用线程继续。
        return ExecutionResult(
            signal_id=signal.signal_id,
            status=ExecutionStatus.QUEUED_LIMIT_DOWN,
            requested_qty=signal.amount,
            filled_qty=total_filled,
            attempts=attempts,
            message="limit-down queue handed to dedicated worker",
        )

    def _run_queued_sell(
        self,
        signal: TradeSignal,
        remaining_qty: int,
        total_filled: int,
        attempts: int,
        low_limit: float,
        queue_cancel_at: dt.time,
        wait_sec: float,
    ) -> ExecutionResult:
        """跌停排队卖出的完整执行体: 挂单 → 落库 → 慢轮询 → 截止撤单收尾。

        在主线程(未配置专用池)或 qmt-queue 专用线程里执行。无论走哪条路径
        都会在 finally 释放并发名额与开盘屏障(排队单挂上即不再阻塞买入)。
        """
        code_label = signal.display_code
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
                return self._defer_for_recovery(
                    signal, total_filled, attempt_no, halt_reason,
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
            # 挂单落库即释放开盘屏障(文档化不变量: 排队单不阻塞买入);
            # 移交专用线程后, 这里保持"挂上即释放"的原时序。
            self._on_limit_down_queued(signal.signal_id)
            logger.info(
                "%s | %s | 跌停排队已挂 | %.3f×%s | 截止 %s | 排队等待开板",
                signal.console_event("竞价"), code_label, low_limit, attempt_qty,
                queue_cancel_at,
            )

            # ---- 慢轮询直到终态或截止 ----
            mono_deadline = time.monotonic() + wait_sec
            snapshot: OrderSnapshot | None = None
            try:
                snapshot = self.broker.get_order_snapshot(order_id)
                while (
                    snapshot.status not in _TERMINAL_ORDER_STATUSES
                    and time.monotonic() < mono_deadline
                    and not self._stop_requested()
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
                return self._defer_for_recovery(
                    signal, reconciled_total, attempts, halt_reason,
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
                    return self._defer_for_recovery(
                        signal, total_filled, attempts, halt_reason,
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
                queue_cancel_at,
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
            # 未挂单就终止的降级路径(无持仓/拒单等)也必须释放开盘屏障,
            # 否则移交专用线程后买单会被一只已跳过的排队单永远挡住。
            # 成功挂单路径在上面已释放过一次, 此处重复调用是无害空操作。
            self._on_limit_down_queued(signal.signal_id)

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

    def _stop_requested(self) -> bool:
        """优雅退出时返回 True: 排队慢轮询据此提前收口, 而非等到排队截止。"""
        return self._stop_event is not None and self._stop_event.is_set()

    def _set_trading_halt(self, reason: str) -> str:
        """熔断原因首次落定后不再被后续原因覆盖, 保证停机根因可追溯。"""
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
            # 留出手续费缓冲: 柜台校验的是"委托金额 + 佣金/过户费 ≤ 可用资金",
            # 顶格算出的股数会被判定资金不足而废单, 且重试拿到的是同一个报价。
            usable_cash = available_cash * (1.0 - self.config.cash_fee_buffer_pct)
            max_shares = int(usable_cash / order_price + 1e-9)
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

"""本地策略引擎协调层：按专用账户策略日程在本地产生买卖决策并派发内部信号。

决策在此完成，实际下单仍走既有 OrderExecutionEngine 执行状态机；本模块负责
策略日生命周期（候选计划、开盘卖出/买入、盘中硬止损、定时卖出）、事件落账、
异常中断后的未决事件恢复，以及排队单（QUEUED_*）占位结果的收口。

本模块约定:
- 决策只读账户快照与行情，下单路径每次仍实时查询资金/持仓；
- 排队单占位结果视为受管中间态：事件保持 SUBMITTED 并转挂排队 future，不熔断策略日；
- 同一代码同时只保留一个活动卖单 future，活动卖单终态前不重复派单。
"""

from __future__ import annotations

import datetime as dt
import logging
import threading
import time
from concurrent.futures import Future
from dataclasses import asdict, replace
from typing import Any

from miniqmt_follower.config import MachineScheduleConfig
from miniqmt_follower.models import (
    Action,
    ExecutionStatus,
    TradeSignal,
    is_terminal_execution_status,
)
from miniqmt_follower.store import CandidatePlanConflict, SQLiteExecutionStore
from miniqmt_follower.strategy_config import StrategyEngineConfig
from miniqmt_follower.strategy_models import (
    CandidatePlan,
    CandidatePlanStatus,
    MarketSnapshot,
    PositionSnapshot,
    StrategyAction,
    StrategyDayStatus,
    StrategyEvent,
    StrategyEventStatus,
)
from miniqmt_follower.strategy_rules import (
    calculate_equal_budgets,
    calculate_topup_budgets,
    decide_afternoon_exit,
    decide_hard_stop,
    decide_morning_exit,
    decide_opening_exit,
    decide_trailing_exit,
    filter_deployable_topup_budgets,
)

logger = logging.getLogger(__name__)
_MISSED_DEADLINE = object()
_ACTIVE_WINDOW_CADENCE_SEC = 0.2
# 账户快照短缓存: 决策容忍秒级新鲜度, 不必每个 tick 都全量查两次 QMT
# (开盘窗口 0.2s 一个 tick, 每次都查是在和下单路径抢同一把交易锁)。
_ACCOUNT_SNAPSHOT_TTL_SEC = 1.0
# 集合竞价期间(9:15-9:30)没有成交, QMT 快照时间戳冻结在最后一笔成交,
# 9:30 开盘读到的快照天然"超龄"。开盘后该宽限期内按决策时刻重贴时间戳。
_OPENING_SNAPSHOT_GRACE_SEC = 60
# 收盘休眠后的盘前唤醒时刻: 引擎睡到次日该时刻醒来, 随后直接睡到当日第一个
# 调度边界(盘前挂单窗口起点), 不空转。候选计划的即时挂单走消费线程, 与此无关。
_PREOPEN_WAKEUP_TIME = dt.time(9, 0)


# ---------------------------------------------------------------------------
# 辅助函数
# ---------------------------------------------------------------------------


def _after_inclusive_second(current: dt.time, boundary: dt.time) -> bool:
    """返回 current 截断微秒后是否严格晚于 boundary，用于构造半开区间 [start, end)。"""
    return current.replace(microsecond=0) > boundary


def make_local_buy_signal(
    *,
    strategy_id: str,
    trading_date: dt.date,
    code: str,
    budget: float,
    reference_price: float,
    created_at: dt.datetime,
    preopen_submit: bool = False,
) -> TradeSignal:
    """构造开盘买入的内部 TradeSignal，金额以 fixed_budget 模式交给执行端按预算成交。"""
    safe_code = code.replace(".", "")
    return TradeSignal(
        signal_id=f"{strategy_id}-{trading_date:%Y%m%d}-opening-buy-{safe_code}",
        strategy_id=strategy_id,
        action=Action.BUY,
        code=code,
        amount=0,
        reference_price=reference_price,
        created_at=created_at.strftime("%Y-%m-%d %H:%M:%S"),
        quantity_mode="fixed_budget",
        budget_amount=float(budget),
        purpose="建仓",
        preopen_submit=preopen_submit,
    )


def make_local_topup_signal(
    *,
    strategy_id: str,
    trading_date: dt.date,
    code: str,
    budget: float,
    reference_price: float,
    created_at: dt.datetime,
    wave: int = 1,
) -> TradeSignal:
    """构造开盘回款补仓的内部 TradeSignal（第二波起, 每波独立编号幂等）。

    多波次补仓时 signal_id 带波次序号: 同一天同一候选可以产生多个补仓信号,
    各自独立走幂等闸; 波次序号从当日已落库的补仓信号数重建, 重启后不重发旧波。
    """
    safe_code = code.replace(".", "")
    return TradeSignal(
        signal_id=f"{strategy_id}-{trading_date:%Y%m%d}-topup{wave:02d}-{safe_code}",
        strategy_id=strategy_id,
        action=Action.BUY,
        code=code,
        amount=0,
        reference_price=reference_price,
        created_at=created_at.strftime("%Y-%m-%d %H:%M:%S"),
        quantity_mode="fixed_budget",
        budget_amount=float(budget),
        purpose="补仓",
    )


def _sell_purpose(rule_name: str, action: StrategyAction) -> str:
    """本地卖出信号的交易目的标签: 规则 + 动作 → 日志展示用词。"""
    if rule_name == "opening_exit":
        return "开盘止损"
    if rule_name == "hard_stop":
        return "止损"
    if rule_name == "afternoon_exit":
        return "清仓"
    if rule_name == "morning_exit":
        return "卖半锁盈" if action == StrategyAction.SELL_HALF else "止损"
    if rule_name == "trailing_exit":
        return "回落止盈"
    return ""


def _local_sell_signal(
    *,
    strategy_id: str,
    trading_date: dt.date,
    rule_name: str,
    code: str,
    action: StrategyAction,
    reference_price: float,
    created_at: dt.datetime,
    sell_half_insufficient_lot_mode: str | None = None,
) -> TradeSignal:
    """构造本地卖出信号，按 action 在 sell_all / sell_half 间选择 quantity_mode。"""
    safe_code = code.replace(".", "")
    quantity_mode = "sell_all" if action == StrategyAction.SELL_ALL else "sell_half"
    return TradeSignal(
        signal_id=f"{strategy_id}-{trading_date:%Y%m%d}-{rule_name}-{safe_code}",
        strategy_id=strategy_id,
        action=Action.SELL,
        code=code,
        amount=0,
        reference_price=reference_price,
        created_at=created_at.strftime("%Y-%m-%d %H:%M:%S"),
        quantity_mode=quantity_mode,
        sell_half_insufficient_lot_mode=sell_half_insufficient_lot_mode,
        purpose=_sell_purpose(rule_name, action),
    )


# ---------------------------------------------------------------------------
# 本地策略引擎
# ---------------------------------------------------------------------------


class LocalStrategyEngine:
    """专用账户策略协调层；决策在此完成，订单仍交给既有执行状态机。

    按日程逐 tick 评估开盘卖出/开盘买入/硬止损/定时卖出规则，把产生的内部
    信号派发到对应方向线程池执行，并维护策略日与事件落账、恢复与收口。
    """

    def __init__(
        self,
        *,
        config: StrategyEngineConfig,
        machine_schedule: MachineScheduleConfig,
        store: SQLiteExecutionStore,
        market_data,
        broker,
        executor,
        pools,
        opening_barrier,
        # 单票仓位上限: 来自 config.strategy.yaml 的 execution.max_single_position_pct
        # (合并后的单一键), 由 app 层注入; 开盘买入/回款补仓预算计算时作为
        # "单标的持仓上限"使用。
        single_position_limit_pct: float,
        stop_event: threading.Event | None = None,
        clock=dt.datetime.now,
        account_snapshot_ttl_sec: float = _ACCOUNT_SNAPSHOT_TTL_SEC,
    ) -> None:
        self.config = config
        self.machine_schedule = machine_schedule
        self.store = store
        self.market_data = market_data
        self.broker = broker
        self.executor = executor
        self.pools = pools
        self.opening_barrier = opening_barrier
        self._single_position_limit_pct = single_position_limit_pct
        self._external_stop = stop_event
        self._clock = clock
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._futures: set[Future[Any]] = set()
        self._futures_lock = threading.Lock()
        self._decision_lock = threading.RLock()
        self._active_sell_by_code: dict[str, Future[Any]] = {}
        self._active_event_keys: set[tuple[dt.date, str, str]] = set()
        self._account_halt_reason: str | None = None
        self._startup_recovery_complete = False
        self._last_hard_stop_check: dt.datetime | None = None
        self._last_trailing_check: dt.datetime | None = None
        # 两波开盘买入状态: 第一波(盘前挂单)的可用资金基线, 第二波起连续补仓
        # 用"当前可用资金 − 基线"识别卖款回笼; 基线随第一波接纳落库
        # (strategy_state), 重启后可重建并续跑补仓波次。
        self._wave1_cash_baseline: float | None = None
        self._baseline_state_loaded = False
        self._account_snapshot_ttl_sec = account_snapshot_ttl_sec
        self._account_cache: tuple[float, object] | None = None

    # ---------------------------------------------------------------------------
    # 生命周期与调度
    # ---------------------------------------------------------------------------

    def start(self) -> None:
        """启动后台调度线程；重复调用幂等。"""
        if self._thread is not None and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(
            target=self._run, name="qmt-local-strategy", daemon=True
        )
        self._thread.start()

    def stop(self) -> None:
        """停止后台调度线程并最多等待 5 秒让当前周期收尾。"""
        self._stop.set()
        if self._thread is not None and self._thread is not threading.current_thread():
            self._thread.join(timeout=5)
        self._thread = None

    def _run(self) -> None:
        """调度主循环：逐周期 tick，任一周期的未捕获异常重置恢复标记以便下周期重扫。"""
        while not self._stop.is_set() and not (
            self._external_stop is not None and self._external_stop.is_set()
        ):
            try:
                self.tick(self._clock())
            except Exception:
                self._startup_recovery_complete = False
                logger.exception("【策略】❌ 调度周期异常")
            now = self._clock()
            self._stop.wait(self._scheduler_delay(now))

    def _scheduler_delay(self, now: dt.datetime) -> float:
        """计算到下一调度边界的最小睡眠时长；活跃窗口内用短周期避免错过规则触发点。"""
        schedule = self.config.schedule
        timed_windows = (
            (
                self.config.opening_exit,
                schedule.opening_exit.trigger_at,
                schedule.opening_exit.retry_until,
            ),
            (
                self.config.opening_buy,
                schedule.opening_buy.start_at,
                schedule.opening_buy.admit_until,
            ),
            (
                self.config.morning_exit,
                schedule.morning_exit.trigger_at,
                schedule.morning_exit.retry_until,
            ),
            (
                self.config.afternoon_exit,
                schedule.afternoon_exit.trigger_at,
                schedule.afternoon_exit.retry_until,
            ),
        )
        active_timed_window = any(
            cfg.enabled and start_at <= now.time() < end_at
            for cfg, start_at, end_at in timed_windows
        )
        interval_delay = (
            _ACTIVE_WINDOW_CADENCE_SEC
            if active_timed_window
            else max(_ACTIVE_WINDOW_CADENCE_SEC, schedule.hard_stop.interval_sec)
        )
        boundaries = {
            _PREOPEN_WAKEUP_TIME,
            schedule.candidate_plan.accept_until,
            schedule.opening_exit.trigger_at,
            schedule.opening_exit.retry_until,
            schedule.opening_buy.preopen_start_at,
            schedule.opening_buy.start_at,
            schedule.opening_buy.admit_until,
            schedule.hard_stop.start_at,
            schedule.hard_stop.last_check_at,
            schedule.morning_exit.trigger_at,
            schedule.morning_exit.retry_until,
            schedule.afternoon_exit.trigger_at,
            schedule.afternoon_exit.retry_until,
            self.machine_schedule.lifecycle.daily_summary_at,
        }
        boundary_delays = []
        for day_offset in (0, 1):
            boundary_date = now.date() + dt.timedelta(days=day_offset)
            for boundary in boundaries:
                candidate = dt.datetime.combine(boundary_date, boundary)
                if now.tzinfo is not None:
                    candidate = candidate.replace(tzinfo=now.tzinfo)
                delay = (candidate - now).total_seconds()
                if delay > 0:
                    boundary_delays.append(delay)
            if boundary_delays:
                break
        active_start = min(
            schedule.opening_exit.trigger_at,
            schedule.opening_buy.preopen_start_at,
            schedule.opening_buy.start_at,
            schedule.hard_stop.start_at,
            schedule.morning_exit.trigger_at,
            schedule.afternoon_exit.trigger_at,
        )
        # 日结时刻已过: 直接睡到下一个边界(次日盘前唤醒点)。
        if now.time() >= self.machine_schedule.lifecycle.daily_summary_at:
            return min(boundary_delays)
        # 当日首个调度边界之前(盘前唤醒后): 直接睡到该边界, 不按秒空转。
        if now.time() < active_start:
            return min(boundary_delays)
        return min(interval_delay, min(boundary_delays))

    # ---------------------------------------------------------------------------
    # 候选计划与账户快照
    # ---------------------------------------------------------------------------

    def _account_snapshot_cached(self) -> object:
        """带短TTL的账户快照: 策略决策容忍秒级新鲜度, 不必每个tick都全量查QMT。

        下单路径不经过这里(执行引擎每次仍实时查资金/持仓), 缓存只服务决策;
        日结(15:02)在 _close_day 里另行全新查询, 不共用本缓存。TTL<=0 时每次必查。
        """
        if self._account_snapshot_ttl_sec <= 0:
            return self.broker.query_account_snapshot()
        now_mono = time.monotonic()
        if (
            self._account_cache is not None
            and now_mono - self._account_cache[0] < self._account_snapshot_ttl_sec
        ):
            return self._account_cache[1]
        snapshot = self.broker.query_account_snapshot()
        self._account_cache = (now_mono, snapshot)
        return snapshot

    def accept_candidate_plan(
        self, plan: CandidatePlan, *, now: dt.datetime | None = None
    ) -> CandidatePlanStatus:
        """校验并落库候选计划，超期/超量时标记 LATE/REJECTED，按需预订阅行情。"""
        now = now or self._clock()
        if plan.strategy_id != self.config.strategy_id:
            raise ValueError("candidate_plan strategy_id 与本地策略不匹配")
        if not self.config.candidate_plan.enabled:
            return self.store.accept_candidate_plan(
                plan, status=CandidatePlanStatus.REJECTED
            )
        late = (
            plan.trading_date != now.date()
            or now.time() >= self.config.schedule.candidate_plan.accept_until
            or now.timestamp() * 1000 - plan.sent_at_ms
            > self.config.candidate_plan.max_age_sec * 1000
        )
        if len(plan.candidates) > self.config.candidate_plan.max_candidates:
            status = CandidatePlanStatus.REJECTED
        elif late:
            status = CandidatePlanStatus.LATE
        else:
            status = CandidatePlanStatus.READY if plan.candidates else CandidatePlanStatus.EMPTY
        persisted = self.store.accept_candidate_plan(plan, status=status)
        if (
            persisted in {CandidatePlanStatus.READY, CandidatePlanStatus.EMPTY}
            and self.config.candidate_plan.subscribe_on_receive
        ):
            self.market_data.subscribe(plan.candidates)
        # 收到候选计划立即盘前挂单(不等 tick 调度): 竞价价已定, 早挂只赚
        # 队列位置, 开盘价撮合不受挂单时刻影响。数据未就绪(如竞价价尚未
        # 形成)由行情门控 BLOCK, tick 路径在盘前窗口内按既有重试语义补挂。
        if (
            persisted == CandidatePlanStatus.READY
            and self.config.opening_buy.enabled
            and now.time() < self.config.schedule.opening_buy.start_at
        ):
            with self._decision_lock:
                self._run_opening_buy_wave1(
                    now, plan, self._account_snapshot_cached(),
                    self.config.opening_buy, self.config.schedule.opening_buy,
                )
        return persisted

    # ---------------------------------------------------------------------------
    # 主循环与日结
    # ---------------------------------------------------------------------------

    def tick(self, now: dt.datetime) -> None:
        """执行一个决策周期：恢复→日结→过期兜底→按规则评估并派发，全程持决策锁。"""
        with self._decision_lock:
            day = self.store.ensure_strategy_day(
                self.config.strategy_id, now.date(), StrategyDayStatus.NO_PLAN
            )
            if self._account_halt_reason is not None:
                self.store.update_strategy_day(
                    self.config.strategy_id,
                    now.date(),
                    StrategyDayStatus.HALTED,
                    halt_reason=self._account_halt_reason,
                )
                day = self.store.get_strategy_day(
                    self.config.strategy_id, now.date()
                )
                if self._close_day(now, day):
                    return
                return
            if not self._startup_recovery_complete:
                if not self._recover_outstanding_events(now):
                    return
                self._startup_recovery_complete = True
            if self._close_day(now, day):
                return
            if day.status in {StrategyDayStatus.HALTED, StrategyDayStatus.CLOSED}:
                return
            self._finalize_missed_deadlines(now)
            schedule = self.config.schedule
            active_start = min(
                schedule.opening_exit.trigger_at,
                schedule.opening_buy.preopen_start_at,
                schedule.opening_buy.start_at,
                schedule.hard_stop.start_at,
                schedule.morning_exit.trigger_at,
                schedule.afternoon_exit.trigger_at,
            )
            active_end = max(
                schedule.opening_exit.retry_until,
                schedule.opening_buy.admit_until,
                schedule.hard_stop.last_check_at,
                schedule.morning_exit.retry_until,
                schedule.afternoon_exit.retry_until,
            )
            if not (
                active_start <= now.time()
                and not _after_inclusive_second(now.time(), active_end)
            ):
                return
            if not self._rules_due(now):
                return
            account = self._account_snapshot_cached()
            self.market_data.subscribe(position.code for position in account.positions)

            self._run_opening_exits(now, account.positions)
            self._run_opening_buys(now, account)
            self._run_hard_stops(now, account.positions)
            self._run_trailing_exits(now, account.positions)
            self._run_timed_exit(now, account.positions, "morning_exit")
            self._run_timed_exit(now, account.positions, "afternoon_exit")

    def _close_day(self, now: dt.datetime, day) -> bool:
        """日结时刻后汇总真实账户并关闭策略日；日结用全新查询而非决策缓存。"""
        daily_summary_at = self.machine_schedule.lifecycle.daily_summary_at
        if now.time() < daily_summary_at:
            return False
        if day.status == StrategyDayStatus.CLOSED:
            return True
        try:
            account = self.broker.query_account_snapshot()
        except Exception:
            # 收盘后 miniQMT 常已退出/断连, 这里不打印错误日志也不重试刷屏:
            # 日结失败只留一条 debug(文件日志), 策略日保持 OPEN, 由次日人工复盘。
            logger.debug(
                "【策略】日结未完成 | 账户查询失败 | 策略日保持 OPEN",
            )
            return True
        events = self.store.list_strategy_events(
            self.config.strategy_id, now.date()
        )
        status_counts: dict[str, int] = {}
        total_filled = 0
        for event in events:
            status_counts[event.status.value] = status_counts.get(event.status.value, 0) + 1
            if event.signal_id:
                stored = self.store.get_signal_optional(event.signal_id)
                if stored is not None:
                    total_filled += stored.filled_qty
        logger.info(
            "【策略】📊 QMT真实账户日结 | 日期=%s | 计划=%s | 事件=%s | "
            "成交股数=%s | 持仓=%s只 | 市值=%.2f | 可用资金=%.2f",
            now.date(),
            day.plan_id or "<无>",
            status_counts,
            total_filled,
            len(account.positions),
            account.market_value,
            account.available_cash,
        )
        self.store.update_strategy_day(
            self.config.strategy_id,
            now.date(),
            StrategyDayStatus.CLOSED,
            halt_reason=day.halt_reason,
        )
        return True

    def _rules_due(self, now: dt.datetime) -> bool:
        timed = (
            (
                self.config.opening_exit,
                self.config.schedule.opening_exit.trigger_at,
                self.config.schedule.opening_exit.retry_until,
            ),
            (
                self.config.opening_buy,
                self.config.schedule.opening_buy.start_at,
                self.config.schedule.opening_buy.admit_until,
            ),
            (
                self.config.morning_exit,
                self.config.schedule.morning_exit.trigger_at,
                self.config.schedule.morning_exit.retry_until,
            ),
            (
                self.config.afternoon_exit,
                self.config.schedule.afternoon_exit.trigger_at,
                self.config.schedule.afternoon_exit.retry_until,
            ),
        )
        if any(
            cfg.enabled and start_at <= now.time() < end_at
            for cfg, start_at, end_at in timed
        ):
            return True
        return self._hard_stop_due(now)

    def _hard_stop_due(self, now: dt.datetime) -> bool:
        """判定硬止损是否到点；最后一秒只查一次，避免同一 tick 内重复评估。"""
        cfg = self.config.intraday_hard_stop
        schedule = self.config.schedule.hard_stop
        if not cfg.enabled or not (
            schedule.start_at <= now.time()
            and not _after_inclusive_second(now.time(), schedule.last_check_at)
        ):
            return False
        last_check = self._last_hard_stop_check
        if last_check is None:
            return True
        in_final_second = (
            now.time().replace(microsecond=0) == schedule.last_check_at
        )
        last_was_final_check = (
            last_check.date() == now.date()
            and last_check.time().replace(microsecond=0) == schedule.last_check_at
        )
        if in_final_second:
            return not last_was_final_check
        return (now - last_check).total_seconds() >= schedule.interval_sec

    def _finalize_missed_deadlines(self, now: dt.datetime) -> None:
        deadlines = {
            "opening_exit": self.config.schedule.opening_exit.retry_until,
            "opening_buy_data": self.config.schedule.opening_buy.admit_until,
            "morning_exit": self.config.schedule.morning_exit.retry_until,
            "afternoon_exit": self.config.schedule.afternoon_exit.retry_until,
            "hard_stop": self.config.schedule.hard_stop.last_check_at,
            "trailing_exit": self.config.schedule.hard_stop.last_check_at,
        }
        for event in self.store.list_strategy_events(
            self.config.strategy_id,
            now.date(),
            status=StrategyEventStatus.BLOCKED_DATA,
        ):
            deadline = deadlines.get(event.rule_name)
            if deadline is None and event.rule_name.startswith("opening_buy_topup"):
                # 多波次补仓事件的规则名带波次后缀, 截止与开盘买入窗口同界。
                deadline = self.config.schedule.opening_buy.admit_until
            missed = (
                _after_inclusive_second(now.time(), deadline)
                if event.rule_name in ("hard_stop", "trailing_exit")
                and deadline is not None
                else deadline is not None and now.time() >= deadline
            )
            if missed:
                self.store.update_strategy_event(
                    event.strategy_id,
                    event.trading_date,
                    event.rule_name,
                    event.code,
                    StrategyEventStatus.MISSED_DEADLINE,
                )

    def _candidate_for_day(self, trading_date: dt.date):
        day = self.store.get_strategy_day(self.config.strategy_id, trading_date)
        if not day.plan_id:
            return None
        stored = self.store.get_candidate_plan(day.plan_id)
        if stored.status not in {CandidatePlanStatus.READY, CandidatePlanStatus.EMPTY}:
            return None
        return stored.plan

    # ---------------------------------------------------------------------------
    # 规则执行
    # ---------------------------------------------------------------------------

    def _run_opening_exits(self, now: dt.datetime, positions) -> None:
        cfg = self.config.opening_exit
        schedule = self.config.schedule.opening_exit
        if not cfg.enabled or not (
            schedule.trigger_at <= now.time() < schedule.retry_until
        ):
            return
        for position in positions:
            if self._event_exists(now.date(), "opening_exit", position.code):
                continue
            market = self.market_data.latest_strategy_snapshot(position.code)
            market = self._snapshot_for_decision(market, now)
            decision = decide_opening_exit(
                position, market, cfg.low_open_exit, self.config.limit_detection,
                self.config.data_safety, now=now,
                limit_down_enabled=cfg.limit_down_exit.enabled,
            )
            if decision is None and now.time() < (
                self.machine_schedule.market_session.continuous_trading_start_at
            ):
                # 仅盘前同步评估盘中硬止损(同一开关、同一阈值):
                # 竞价定盘后即检查, 命中票作为盘前卖单先于开盘买入执行,
                # 卖出回款进入买入批次预算(腾挪资金给新仓)。
                # 连续竞价开始后归 intraday_hard_stop 规则管辖, 避免同一持仓
                # 被两条规则名重复评估; 首查时已卖票自然跳过(事件去重+持仓清空)。
                decision = decide_hard_stop(
                    position, market, self.config.intraday_hard_stop,
                    self.config.limit_detection, self.config.data_safety, now=now,
                )
            if (
                decision is None
                and self.config.trailing_take_profit.enabled
                and now.time()
                < self.machine_schedule.market_session.continuous_trading_start_at
            ):
                # 盘前跨日回撤检查: 买入以来最高价(跨日 SQLite 记录)对今日
                # 开盘价回落≥8% → 盘前卖单, 先于开盘买入执行。独立落账为
                # trailing_exit 事件, 目的标签=回落止盈。
                high = self._position_high_for(position, market)
                trailing_decision = decide_trailing_exit(
                    position, market, high, self.config.trailing_take_profit,
                    self.config.data_safety, now=now,
                )
                if trailing_decision is not None:
                    self._handle_decision(
                        "trailing_exit", position, market, trailing_decision, now,
                    )
            self._handle_decision("opening_exit", position, market, decision, now)

    def _run_opening_buys(self, now: dt.datetime, account) -> None:
        """两波开盘买入调度:
        - 盘前窗口 [preopen_start_at, start_at): 第一波整批接纳, 盘前挂单排队,
          9:30:00 开盘价撮合(不等卖单屏障, 资金=盘前可用资金);
        - 开盘后 [start_at, admit_until): 卖单终态且卖款回笼后一次性补仓;
          第一波未发生(重启/旧配置)时退回原整批接纳路径。
        """
        cfg = self.config.opening_buy
        schedule = self.config.schedule.opening_buy
        if not cfg.enabled:
            return
        plan = self._candidate_for_day(now.date())
        if plan is None or not plan.candidates:
            return
        if schedule.preopen_start_at <= now.time() < schedule.start_at:
            self._run_opening_buy_wave1(now, plan, account, cfg, schedule)
        elif schedule.start_at <= now.time() < schedule.admit_until:
            if self._topup_cash_baseline(now.date()) is None:
                # 第一波未发生且无落库基线(重启/旧行为): 退回原整批接纳,
                # 语义与旧版一致; 该路径会把接纳时资金落为基线继续补仓。
                self._run_opening_buy_batch(
                    now, plan, account, cfg, schedule, preopen=False,
                )
            else:
                self._run_opening_buy_topup(now, plan, account, cfg, schedule)

    def _run_opening_buy_wave1(self, now, plan, account, cfg, schedule) -> None:
        """盘前第一波: 用盘前资金整批接纳并挂单排队, 不等卖单屏障。"""
        if all(
            self._event_exists(now.date(), "opening_buy", code)
            for code in plan.candidates
        ):
            return
        admitted, _baseline = self._admit_opening_buy_batch(
            now, plan, account, cfg, cutoff_until=schedule.start_at,
        )
        if not admitted:
            return
        self._wave1_cash_baseline = account.available_cash
        self.store.set_strategy_state(
            self.config.strategy_id,
            now.date(),
            "wave1_cash_baseline",
            f"{account.available_cash:.6f}",
        )
        self._submit_opening_buy_batch(
            now, admitted, rule_name="opening_buy", preopen=True,
        )

    def _run_opening_buy_batch(
        self, now, plan, account, cfg, schedule, *, preopen: bool
    ) -> None:
        """原整批接纳路径(等卖单屏障, 开盘后快照资金), 供第一波未发生时的回退。"""
        if all(
            self._event_exists(now.date(), "opening_buy", code)
            for code in plan.candidates
        ):
            return
        if cfg.wait_for_opening_sells and self.opening_barrier.pending_count():
            return
        admitted, _baseline = self._admit_opening_buy_batch(
            now, plan, account, cfg, cutoff_until=schedule.admit_until,
        )
        if not admitted:
            return
        # 回退整批接纳同样落基线: 后续窗口内的卖款回笼按此基线识别并续补。
        self._wave1_cash_baseline = account.available_cash
        self.store.set_strategy_state(
            self.config.strategy_id,
            now.date(),
            "wave1_cash_baseline",
            f"{account.available_cash:.6f}",
        )
        self._submit_opening_buy_batch(
            now, admitted, rule_name="opening_buy", preopen=preopen,
        )

    def _admit_opening_buy_batch(
        self, now, plan, account, cfg, *, cutoff_until: dt.time
    ) -> tuple[list[tuple[str, float, MarketSnapshot]], bool]:
        """整批评估: 任一票数据不满足即整批不接纳; 提交前再次复核截止线。"""
        budgets = calculate_equal_budgets(
            plan.candidates, account, cfg.capital_allocation,
            single_position_limit_pct=self._single_position_limit_pct,
        )
        admitted: list[tuple[str, float, MarketSnapshot]] = []
        for code, budget in budgets.items():
            if self._event_exists(now.date(), "opening_buy", code):
                continue
            market = self.market_data.latest_strategy_snapshot(code)
            issue = self._buy_market_issue(market, now)
            if issue is not None:
                logger.error("【策略】⛔ 买入批次未接纳 | %s | %s", code, issue)
                self._record_opening_buy_data_block(code, market, issue, now)
                return [], False
            admitted.append((code, budget, market))
        cutoff_check = self._clock()
        if (
            cutoff_check.date() != now.date()
            or cutoff_check.time() >= cutoff_until
        ):
            return [], False
        return admitted, True

    def _submit_opening_buy_batch(
        self,
        now,
        admitted: list[tuple[str, float, MarketSnapshot]],
        *,
        rule_name: str,
        preopen: bool,
    ) -> None:
        """把已接纳批次落事件并派单(兄弟订单全部提交, 不再逐票看时间)。"""
        submissions: list[tuple[TradeSignal, StrategyEvent]] = []
        for code, budget, market in admitted:
            blocked = self.store.get_strategy_event_optional(
                self.config.strategy_id,
                now.date(),
                "opening_buy_data",
                code,
            )
            if blocked is not None and blocked.status == StrategyEventStatus.BLOCKED_DATA:
                self.store.update_strategy_event(
                    blocked.strategy_id,
                    blocked.trading_date,
                    blocked.rule_name,
                    blocked.code,
                    StrategyEventStatus.TERMINAL,
                )
            signal = make_local_buy_signal(
                strategy_id=self.config.strategy_id,
                trading_date=now.date(),
                code=code,
                budget=budget,
                reference_price=market.last_price,
                created_at=now,
                preopen_submit=preopen,
            )
            event = StrategyEvent(
                self.config.strategy_id, now.date(), rule_name, code,
                "fixed_budget", StrategyEventStatus.TRIGGERED, signal.signal_id,
                f"固定预算 {budget:.2f}", asdict(market), {"budget_amount": budget},
            )
            submissions.append((signal, event))
        self.store.create_strategy_events(
            tuple(event for _, event in submissions)
        )
        for signal, event in submissions:
            self._submit(signal, event)

    def _wave1_terminal_or_absent(self, now: dt.datetime, code: str) -> bool:
        """第一波信号不存在或已终态 → 允许补仓; 在途时跳过防双计数。"""
        wave1_id = make_local_buy_signal(
            strategy_id=self.config.strategy_id,
            trading_date=now.date(),
            code=code,
            budget=0.0,
            reference_price=0.0,
            created_at=now,
        ).signal_id
        stored = self.store.get_signal_optional(wave1_id)
        if stored is None:
            return True
        return is_terminal_execution_status(stored.status)

    def _topup_cash_baseline(self, trading_date: dt.date) -> float | None:
        """补仓基线: 内存优先, 缺失时从落库的 strategy_state 重建(重启续跑)。"""
        if self._wave1_cash_baseline is not None:
            return self._wave1_cash_baseline
        if not self._baseline_state_loaded:
            self._baseline_state_loaded = True
            raw = self.store.get_strategy_state(
                self.config.strategy_id, trading_date, "wave1_cash_baseline"
            )
            if raw is not None:
                try:
                    self._wave1_cash_baseline = float(raw)
                except ValueError:
                    logger.warning(
                        "【策略】⛔ 补仓基线落库值非法, 当日跳过补仓 | %s", raw
                    )
        return self._wave1_cash_baseline

    def _run_opening_buy_topup(self, now, plan, account, cfg, schedule) -> None:
        """第二波起连续补仓: 窗口内每 tick 检查卖款回笼池, 池子够一手就发一波。

        与旧版一次性补仓的区别(2026-08-31 复盘):
        - 补仓池 = 当前可用资金 − 第一波基线, 每 tick 重算 —— 卖出回款到账
          即补, 不再只认"卖单屏障清空"那一刻(失败的卖单终态也会清空屏障,
          其回款可能迟到数十秒, 旧版会永久漏掉);
        - 多波次: 买单提交即冻结资金、池自然归零, 下一笔回款再起下一波,
          直到 admit_until 收口; 波次编号从落库信号数重建, 幂等且重启可续跑;
        - 单波内预算低于一手的候选留池累积, 下一 tick 与新增回款合并再分配。
        """
        if not cfg.topup_enabled:
            return
        if cfg.wait_for_opening_sells and self.opening_barrier.pending_count():
            return
        baseline = self._topup_cash_baseline(now.date())
        if baseline is None:
            return
        pool = max(0.0, account.available_cash - baseline)
        if pool <= 0:
            return
        candidates = tuple(
            code
            for code in plan.candidates
            if self._wave1_terminal_or_absent(now, code)
        )
        if not candidates:
            return
        # 逐候选行情门控: 不过者仅跳过该候选(记 BLOCKED_DATA), 不再整批返回。
        markets: dict[str, MarketSnapshot] = {}
        for code in candidates:
            market = self.market_data.latest_strategy_snapshot(code)
            issue = self._buy_market_issue(market, now)
            if issue is not None:
                logger.error(
                    "【策略】⛔ 补仓候选本轮跳过 | %s | %s | 下 tick 重试",
                    code, issue,
                )
                self._record_opening_buy_data_block(code, market, issue, now)
                continue
            markets[code] = market
        if not markets:
            return
        budgets = calculate_topup_budgets(
            tuple(markets), account, pool, cfg.capital_allocation,
            single_position_limit_pct=self._single_position_limit_pct,
        )
        wave_budgets = filter_deployable_topup_budgets(
            budgets, {code: m.last_price for code, m in markets.items()}
        )
        if not wave_budgets:
            return
        cutoff_check = self._clock()
        if (
            cutoff_check.date() != now.date()
            or cutoff_check.time() >= schedule.admit_until
        ):
            return
        wave_no = self.store.count_signals_like(
            f"{self.config.strategy_id}-{now.date():%Y%m%d}-topup"
        ) + 1
        rule_name = f"opening_buy_topup_{wave_no:02d}"
        submissions: list[tuple[TradeSignal, StrategyEvent]] = []
        for code, budget in wave_budgets.items():
            market = markets[code]
            signal = make_local_topup_signal(
                strategy_id=self.config.strategy_id,
                trading_date=now.date(),
                code=code,
                budget=budget,
                reference_price=market.last_price,
                created_at=now,
                wave=wave_no,
            )
            event = StrategyEvent(
                self.config.strategy_id, now.date(), rule_name, code,
                "fixed_budget", StrategyEventStatus.TRIGGERED, signal.signal_id,
                f"补仓预算 {budget:.2f}", asdict(market),
                {"budget_amount": budget, "wave": wave_no},
            )
            submissions.append((signal, event))
        self.store.create_strategy_events(
            tuple(event for _, event in submissions)
        )
        for signal, event in submissions:
            self._submit(signal, event)
        # 买单提交即冻结资金: 使账户快照缓存失效, 下一 tick 必须拿到含冻结的
        # 真实可用资金, 防止旧快照把同一笔回款再算一遍(双花)。
        self._account_cache = None
        logger.info(
            "【策略】💰 开盘回款补仓第%02d波已提交 | %s只 | 补仓池=%.2f | 本波预算=%s",
            wave_no, len(submissions), pool,
            ", ".join(f"{b:.2f}" for b in wave_budgets.values()),
        )

    def _record_opening_buy_data_block(
        self,
        code: str,
        market: MarketSnapshot,
        reason: str,
        now: dt.datetime,
    ) -> None:
        self.store.upsert_retryable_strategy_event(
            StrategyEvent(
                self.config.strategy_id,
                now.date(),
                "opening_buy_data",
                code,
                StrategyAction.BLOCK.value,
                StrategyEventStatus.BLOCKED_DATA,
                None,
                reason,
                asdict(market),
                {},
            )
        )

    def _snapshot_for_decision(
        self, market: MarketSnapshot, now: dt.datetime
    ) -> MarketSnapshot:
        """竞价时段冻结的快照在开盘宽限期内重贴时间戳, 免于误判超龄。

        集合竞价期间没有成交, QMT 快照时间戳停在最后一笔成交(如 09:25:01);
        9:30 开盘时读到的快照距离该时间戳已近 5 分钟, 会被 3s 时效门控整体
        拒掉。开盘后 60s 内属于同一交易时刻的有效行情, 直接把 quote_time
        视为决策时刻即可, 宽限期之外仍走严格的时效判定。
        """
        if market.quote_time is None or market.quote_time.date() != now.date():
            return market
        session = self.machine_schedule.market_session
        if not (
            session.call_auction_start_at
            <= market.quote_time.time()
            < session.continuous_trading_start_at
        ):
            return market
        continuous_start = dt.datetime.combine(
            now.date(), session.continuous_trading_start_at
        )
        # 竞价时段成交冻结, 快照时间戳停在最后一笔成交: 开盘前后宽限期内
        # 都按决策时刻重贴 —— 盘前(收到候选即挂单)与开盘后同样适用;
        # 时间戳晚于决策时刻(时钟异常)则不改, 交给上层封闭失败。
        if (
            now >= market.quote_time
            and (now - continuous_start).total_seconds()
            <= _OPENING_SNAPSHOT_GRACE_SEC
        ):
            return replace(market, quote_time=now)
        return market

    def _buy_market_issue(self, market: MarketSnapshot, now: dt.datetime) -> str | None:
        market = self._snapshot_for_decision(market, now)
        if market.quote_time is None or market.trading_date != now.date():
            return "行情日期或时间缺失"
        age = (now - market.quote_time).total_seconds()
        if age < -1 or age > self.config.data_safety.max_tick_age_sec:
            return f"行情超龄 {age:.3f}s"
        if market.last_price <= 0:
            return "最新价无效"
        if self.config.limit_detection.require_limit_prices and (
            market.high_limit is None or market.low_limit is None
        ):
            return "涨跌停价缺失"
        return None

    def _run_hard_stops(self, now: dt.datetime, positions) -> None:
        if not self._hard_stop_due(now):
            return
        cfg = self.config.intraday_hard_stop
        for position in positions:
            if self._event_exists(now.date(), "hard_stop", position.code):
                continue
            market = self.market_data.latest_strategy_snapshot(position.code)
            market = self._snapshot_for_decision(market, now)
            decision = decide_hard_stop(
                position, market, cfg, self.config.limit_detection,
                self.config.data_safety, now=now,
            )
            self._handle_decision("hard_stop", position, market, decision, now)
        self._last_hard_stop_check = now

    def _trailing_due(self, now: dt.datetime) -> bool:
        """回落止盈与硬止损同窗口同节奏; 最后一秒只查一次。"""
        schedule = self.config.schedule.hard_stop
        if not (
            schedule.start_at <= now.time()
            and not _after_inclusive_second(now.time(), schedule.last_check_at)
        ):
            return False
        last_check = self._last_trailing_check
        if last_check is None:
            return True
        in_final_second = (
            now.time().replace(microsecond=0) == schedule.last_check_at
        )
        last_was_final_check = (
            last_check.date() == now.date()
            and last_check.time().replace(microsecond=0) == schedule.last_check_at
        )
        if in_final_second:
            return not last_was_final_check
        return (now - last_check).total_seconds() >= schedule.interval_sec

    def _position_high_for(
        self, position, market: MarketSnapshot
    ) -> float | None:
        """买入以来最高价: 无记录时用当日最高播种, 有记录时取新高并落库。"""
        strategy_id = self.config.strategy_id
        record = self.store.get_position_high(strategy_id, position.code)
        day_high = market.day_high
        if record is None:
            if day_high is None:
                return None
            self.store.upsert_position_high(strategy_id, position.code, day_high)
            return day_high
        if day_high is not None and day_high > record:
            self.store.upsert_position_high(strategy_id, position.code, day_high)
            return day_high
        return record

    def _run_trailing_exits(self, now: dt.datetime, positions) -> None:
        """回落止盈: 记录维护 + 盘中(与硬止损同窗口)回撤评估。

        记录维护与规则开关解耦: 清仓票删除记录, 持仓票播种/更新买入以来最高,
        避免禁用期间账本失真。规则触发与硬止损同窗口同节奏,
        盘前的跨日回撤检查挂在 _run_opening_exits 的盘前分支。
        """
        schedule = self.config.schedule.hard_stop
        if not (
            schedule.start_at <= now.time()
            and not _after_inclusive_second(now.time(), schedule.last_check_at)
        ):
            return
        if not self._trailing_due(now):
            return
        held_codes = {
            position.code for position in positions if position.available_qty > 0
        }
        for code in tuple(self.store.list_position_highs(self.config.strategy_id)):
            if code not in held_codes:
                self.store.delete_position_high(self.config.strategy_id, code)
        cfg = self.config.trailing_take_profit
        if not cfg.enabled:
            return
        for position in positions:
            if position.available_qty <= 0:
                continue
            if self._event_exists(now.date(), "trailing_exit", position.code):
                continue
            market = self.market_data.latest_strategy_snapshot(position.code)
            market = self._snapshot_for_decision(market, now)
            high = self._position_high_for(position, market)
            decision = decide_trailing_exit(
                position, market, high, cfg, self.config.data_safety, now=now,
            )
            self._handle_decision("trailing_exit", position, market, decision, now)
        self._last_trailing_check = now

    def _run_timed_exit(self, now: dt.datetime, positions, rule_name: str) -> None:
        cfg = getattr(self.config, rule_name)
        schedule = getattr(self.config.schedule, rule_name)
        if not cfg.enabled or not (
            schedule.trigger_at <= now.time() < schedule.retry_until
        ):
            return
        for position in positions:
            if self._event_exists(now.date(), rule_name, position.code):
                continue
            market = self.market_data.latest_strategy_snapshot(position.code)
            market = self._snapshot_for_decision(market, now)
            if rule_name == "morning_exit":
                decision = decide_morning_exit(
                    position, market, cfg.loss_exit, cfg.profit_reduce,
                    cfg.require_not_limit_up, self.config.limit_detection,
                    self.config.data_safety, now=now,
                )
            else:
                decision = decide_afternoon_exit(
                    position, market, cfg.sell_if_not_limit_up,
                    cfg.hold_if_limit_up, self.config.limit_detection,
                    self.config.data_safety, now=now,
                )
            self._handle_decision(rule_name, position, market, decision, now)

    # ---------------------------------------------------------------------------
    # 决策落账与派单
    # ---------------------------------------------------------------------------

    def _handle_decision(self, rule_name, position, market, decision, now) -> None:
        """把规则决策落成策略事件并按需派单；同代码活动卖单未终态时不重复派卖单。"""
        if decision is None:
            existing = self.store.get_strategy_event_optional(
                self.config.strategy_id, now.date(), rule_name, position.code
            )
            if (
                existing is not None
                and existing.status == StrategyEventStatus.BLOCKED_DATA
                and rule_name != "hard_stop"
            ):
                self.store.update_strategy_event(
                    existing.strategy_id,
                    existing.trading_date,
                    existing.rule_name,
                    existing.code,
                    StrategyEventStatus.TERMINAL,
                )
            return
        status = (
            StrategyEventStatus.BLOCKED_DATA
            if decision.action == StrategyAction.BLOCK
            else StrategyEventStatus.TRIGGERED
        )
        signal = None
        signal_id = None
        if decision.action in {StrategyAction.SELL_ALL, StrategyAction.SELL_HALF}:
            active = self._active_sell_by_code.get(position.code)
            if active is not None and not active.done():
                # 不消耗更高优先级事件；活动卖单终态后，下个周期用最新持仓重判。
                return
            signal = _local_sell_signal(
                strategy_id=self.config.strategy_id, trading_date=now.date(),
                rule_name=rule_name, code=position.code, action=decision.action,
                reference_price=market.last_price,
                created_at=now,
                sell_half_insufficient_lot_mode=(
                    self.config.morning_exit.profit_reduce.insufficient_lot_mode
                    if rule_name == "morning_exit"
                    and decision.action == StrategyAction.SELL_HALF
                    else None
                ),
            )
            signal_id = signal.signal_id
        event = StrategyEvent(
            self.config.strategy_id, now.date(), rule_name, position.code,
            decision.action.value, status, signal_id, decision.reason,
            asdict(market), asdict(position),
        )
        if not self.store.upsert_retryable_strategy_event(event):
            return
        if signal is not None:
            self._submit(signal, event)

    def _submit(
        self, signal: TradeSignal, event: StrategyEvent, *, recover: bool = False
    ) -> None:
        """把信号派发到方向线程池并登记活动卖单占位；先落 SUBMITTED 再入池避免状态倒退。"""
        event_key = (event.trading_date, event.rule_name, event.code)
        if signal.action == Action.SELL:
            if not recover and self._new_sell_missed_deadline(
                signal, event, self._clock()
            ):
                return
            active = self._active_sell_by_code.get(signal.code)
            if active is not None and not active.done():
                self.store.update_strategy_event(
                    event.strategy_id, event.trading_date, event.rule_name, event.code,
                    StrategyEventStatus.SUPERSEDED,
                )
                return
            barrier_registered = self.opening_barrier.register(
                signal, opening_sequence=event.rule_name == "opening_exit"
            )
        else:
            barrier_registered = False
        # 先写 SUBMITTED 再把任务交给线程池，避免极快任务的完成回调先写 TERMINAL、
        # 主线程随后又倒退覆盖成 SUBMITTED。
        try:
            if event.status != StrategyEventStatus.SUBMITTED:
                self.store.update_strategy_event(
                    event.strategy_id,
                    event.trading_date,
                    event.rule_name,
                    event.code,
                    StrategyEventStatus.SUBMITTED,
                )
            self._active_event_keys.add(event_key)
            future = self.pools[signal.action].submit(
                self._execute_signal, signal, event, recover
            )
        except Exception:
            self._active_event_keys.discard(event_key)
            self._startup_recovery_complete = False
            if barrier_registered:
                self.opening_barrier.release(signal.signal_id)
            raise
        with self._futures_lock:
            self._futures.add(future)
            if signal.action == Action.SELL:
                self._active_sell_by_code[signal.code] = future
        future.add_done_callback(lambda item: self._finish_future(item, signal, event))

    def _new_sell_missed_deadline(
        self, signal: TradeSignal, event: StrategyEvent, submit_at: dt.datetime
    ) -> bool:
        last_submit_at = (
            self.machine_schedule.order_guard.strategy_sell_last_submit_at
        )
        if (
            submit_at.date() == event.trading_date
            and not _after_inclusive_second(submit_at.time(), last_submit_at)
        ):
            return False
        self.store.update_strategy_event(
            event.strategy_id,
            event.trading_date,
            event.rule_name,
            event.code,
            StrategyEventStatus.MISSED_DEADLINE,
        )
        logger.warning(
            "【策略】⏰ 卖出未报单已越过最终提交时刻 | %s | %s",
            signal.signal_id,
            last_submit_at,
        )
        return True

    def _execute_signal(
        self, signal: TradeSignal, event: StrategyEvent, recover: bool
    ):
        if recover:
            return self.executor.recover(signal)
        if signal.action == Action.SELL and self._new_sell_missed_deadline(
            signal, event, self._clock()
        ):
            return _MISSED_DEADLINE
        return self.executor.execute(signal)

    # ---------------------------------------------------------------------------
    # 执行收口
    # ---------------------------------------------------------------------------

    def _finish_future(self, future, signal, event) -> None:
        """内部信号终态收口：终态转 TERMINAL，排队占位转挂排队 future，其余视为账户异常熔断。"""
        deferred_cleanup = False
        try:
            result = future.result()
            if result is _MISSED_DEADLINE:
                return
            status = getattr(result, "status", None)
            if isinstance(status, ExecutionStatus) and is_terminal_execution_status(status):
                self.store.update_strategy_event(
                    event.strategy_id, event.trading_date, event.rule_name, event.code,
                    StrategyEventStatus.TERMINAL,
                )
            elif status is not None and getattr(status, "value", "") == "filled":
                # 简单测试执行器使用轻量状态对象；生产 ExecutionStatus 走上面的判定。
                self.store.update_strategy_event(
                    event.strategy_id, event.trading_date, event.rule_name, event.code,
                    StrategyEventStatus.TERMINAL,
                )
            elif (
                isinstance(status, ExecutionStatus)
                and status
                in {
                    ExecutionStatus.QUEUED_LIMIT_DOWN,
                    ExecutionStatus.QUEUED_LIMIT_UP,
                }
            ):
                # 排队单移交专用线程的占位结果: 事件保持 SUBMITTED, 活动卖单
                # 跟踪转挂到排队 future, 到排队终态再统一收口 —— 不能在此熔断
                # 策略日, 也不能提前释放同代码活动卖单占位(否则重判会重复派单)。
                deferred_cleanup = True
                self._hand_off_event_to_queue(future, signal, event)
                return
            else:
                self._account_halt_reason = (
                    f"内部信号未进入明确终态: {signal.signal_id}"
                )
                self.store.update_strategy_event(
                    event.strategy_id, event.trading_date, event.rule_name, event.code,
                    StrategyEventStatus.BLOCKED_ACCOUNT_HALT,
                )
                self.store.update_strategy_day(
                    event.strategy_id, event.trading_date, StrategyDayStatus.HALTED,
                    halt_reason=self._account_halt_reason,
                )
        except Exception:
            logger.exception("【策略】❌ 内部信号执行或落账异常 | %s", signal.signal_id)
            self._startup_recovery_complete = False
        finally:
            with self._futures_lock:
                self._futures.discard(future)
            if not deferred_cleanup:
                if signal.action == Action.SELL:
                    self.opening_barrier.release(signal.signal_id)
                with self._futures_lock:
                    if self._active_sell_by_code.get(signal.code) is future:
                        self._active_sell_by_code.pop(signal.code, None)
                    self._active_event_keys.discard(
                        (event.trading_date, event.rule_name, event.code)
                    )

    def _hand_off_event_to_queue(self, worker_future, signal, event) -> None:
        """排队占位结果: 事件与活动卖单跟踪都挂到排队 future, 终态后收口。"""
        queued = self.executor.queue_future_for(signal.signal_id)
        if queued is None or queued.done():
            # 极端竞态: 排队 future 已终态且被遗忘 → 以 SQLite 终态为准收口。
            stored = self.store.get_signal_optional(signal.signal_id)
            if stored is not None and is_terminal_execution_status(stored.status):
                self.store.update_strategy_event(
                    event.strategy_id, event.trading_date, event.rule_name,
                    event.code, StrategyEventStatus.TERMINAL,
                )
            self._release_event_tracking(worker_future, signal, event)
            return
        if signal.action == Action.SELL:
            with self._futures_lock:
                self._active_sell_by_code[signal.code] = queued
        queued.add_done_callback(
            lambda f: self._finish_queued_event(f, signal, event)
        )

    def _finish_queued_event(self, future, signal, event) -> None:
        try:
            result = future.result()
            status = getattr(result, "status", None)
            if (
                isinstance(status, ExecutionStatus)
                and is_terminal_execution_status(status)
            ):
                self.store.update_strategy_event(
                    event.strategy_id, event.trading_date, event.rule_name,
                    event.code, StrategyEventStatus.TERMINAL,
                )
        except Exception:
            logger.exception("【策略】❌ 排队单终态回调异常 | %s", signal.signal_id)
        finally:
            self._release_event_tracking(future, signal, event)

    def _release_event_tracking(self, future, signal, event) -> None:
        if signal.action == Action.SELL:
            self.opening_barrier.release(signal.signal_id)
        with self._futures_lock:
            if self._active_sell_by_code.get(signal.code) is future:
                self._active_sell_by_code.pop(signal.code, None)
            self._active_event_keys.discard(
                (event.trading_date, event.rule_name, event.code)
            )

    # ---------------------------------------------------------------------------
    # 启动恢复
    # ---------------------------------------------------------------------------

    def _recover_outstanding_events(self, now: dt.datetime) -> bool:
        """重启后重放未决事件：卖出先于买入恢复，恢复未全部完成前不做新决策。"""
        events = self.store.list_outstanding_strategy_events(self.config.strategy_id)
        if not events:
            return True
        halted = next(
            (
                event
                for event in events
                if event.status == StrategyEventStatus.BLOCKED_ACCOUNT_HALT
            ),
            None,
        )
        if halted is not None:
            self._account_halt_reason = (
                f"未解决的账户恢复事件: {halted.signal_id}"
            )
            self.store.update_strategy_day(
                self.config.strategy_id,
                now.date(),
                StrategyDayStatus.HALTED,
                halt_reason=self._account_halt_reason,
            )
            return False

        # 任何卖出恢复都先于买入恢复；恢复未全部终态前，本周期不做新决策。
        has_sell = any(event.decision != "fixed_budget" for event in events)
        recoverable = (
            tuple(event for event in events if event.decision != "fixed_budget")
            if has_sell
            else events
        )
        for event in recoverable:
            key = (event.trading_date, event.rule_name, event.code)
            if key in self._active_event_keys:
                continue
            market = event.market_snapshot or {}
            reference_price = float(market.get("last_price") or 0)
            stored_signal = (
                self.store.get_signal_optional(event.signal_id)
                if event.signal_id
                else None
            )
            if event.decision == "fixed_budget":
                budget = float((event.position_snapshot or {}).get("budget_amount") or 0)
                if budget <= 0 or reference_price <= 0:
                    self._account_halt_reason = (
                        f"恢复事件缺少预算或参考价: {event.signal_id}"
                    )
                    self.store.update_strategy_event(
                        event.strategy_id, event.trading_date, event.rule_name, event.code,
                        StrategyEventStatus.BLOCKED_ACCOUNT_HALT,
                    )
                    self.store.update_strategy_day(
                        self.config.strategy_id,
                        now.date(),
                        StrategyDayStatus.HALTED,
                        halt_reason=self._account_halt_reason,
                    )
                    return False
                if stored_signal is None and (
                    event.trading_date != now.date()
                    or now.time() >= self.config.schedule.opening_buy.admit_until
                ):
                    self.store.update_strategy_event(
                        event.strategy_id,
                        event.trading_date,
                        event.rule_name,
                        event.code,
                        StrategyEventStatus.MISSED_DEADLINE,
                    )
                    continue
                if event.rule_name.startswith("opening_buy_topup"):
                    # 补仓事件按补仓信号重建: 用 opening_buy 构造函数会得到
                    # 不同的 signal_id, 触发"恢复 id 不一致"假熔断, 且找不到
                    # 崩溃前已挂 QMT 的旧单。波次序号从规则名后缀重建。
                    try:
                        wave = int(event.rule_name.rsplit("_", 1)[1])
                    except ValueError:
                        wave = 1
                    signal = make_local_topup_signal(
                        strategy_id=event.strategy_id,
                        trading_date=event.trading_date,
                        code=event.code,
                        budget=budget,
                        reference_price=reference_price,
                        created_at=now,
                        wave=wave,
                    )
                else:
                    signal = make_local_buy_signal(
                        strategy_id=event.strategy_id,
                        trading_date=event.trading_date,
                        code=event.code,
                        budget=budget,
                        reference_price=reference_price,
                        created_at=now,
                        preopen_submit=True,
                    )
            elif event.decision in {
                StrategyAction.SELL_ALL.value,
                StrategyAction.SELL_HALF.value,
            }:
                if stored_signal is None and (
                    event.trading_date != now.date()
                    or _after_inclusive_second(
                        now.time(),
                        self.machine_schedule.order_guard.strategy_sell_last_submit_at,
                    )
                ):
                    self.store.update_strategy_event(
                        event.strategy_id,
                        event.trading_date,
                        event.rule_name,
                        event.code,
                        StrategyEventStatus.MISSED_DEADLINE,
                    )
                    continue
                signal = _local_sell_signal(
                    strategy_id=event.strategy_id,
                    trading_date=event.trading_date,
                    rule_name=event.rule_name,
                    code=event.code,
                    action=StrategyAction(event.decision),
                    reference_price=reference_price,
                    created_at=now,
                    sell_half_insufficient_lot_mode=(
                        self.config.morning_exit.profit_reduce.insufficient_lot_mode
                        if event.decision == StrategyAction.SELL_HALF.value
                        else None
                    ),
                )
            else:
                self._account_halt_reason = (
                    f"恢复事件决策类型非法: {event.decision}"
                )
                self.store.update_strategy_event(
                    event.strategy_id,
                    event.trading_date,
                    event.rule_name,
                    event.code,
                    StrategyEventStatus.BLOCKED_ACCOUNT_HALT,
                )
                self.store.update_strategy_day(
                    self.config.strategy_id,
                    now.date(),
                    StrategyDayStatus.HALTED,
                    halt_reason=self._account_halt_reason,
                )
                return False
            if signal.signal_id != event.signal_id:
                self._account_halt_reason = (
                    f"恢复 signal_id 不一致: {event.signal_id}"
                )
                self.store.update_strategy_event(
                    event.strategy_id,
                    event.trading_date,
                    event.rule_name,
                    event.code,
                    StrategyEventStatus.BLOCKED_ACCOUNT_HALT,
                )
                self.store.update_strategy_day(
                    self.config.strategy_id,
                    now.date(),
                    StrategyDayStatus.HALTED,
                    halt_reason=self._account_halt_reason,
                )
                return False
            self._submit(signal, event, recover=stored_signal is not None)
        return False

    def _event_exists(self, trading_date, rule_name, code) -> bool:
        event = self.store.get_strategy_event_optional(
            self.config.strategy_id, trading_date, rule_name, code
        )
        return event is not None and event.status != StrategyEventStatus.BLOCKED_DATA

    def wait_for_idle(self, timeout: float = 5) -> None:
        """等待全部内部 future 收口，超时抛出 TimeoutError（供优雅退出使用）。"""
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            with self._futures_lock:
                pending = tuple(self._futures)
            if not pending:
                return
            for future in pending:
                future.result(timeout=max(0.01, deadline - time.monotonic()))
        raise TimeoutError("local strategy engine still has pending orders")

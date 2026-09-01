"""本地执行账本（SQLite）。

负责持久化信号、委托尝试、日计划、候选计划、策略日与策略事件，
是重启恢复、幂等去重与盘后审计的单一数据来源。每个线程持有一个持久连接，
避免每次操作都 open/close 的开销；配合 WAL 模式，读写并发友好。

本模块约定:
- 信号幂等以 signal_id 主键为准，重复信号返回 False 而不重复下单。
- 委托尝试每笔券商提交一行，与 signals 表通过 signal_id 关联。
"""

from __future__ import annotations

import datetime as dt
import json
import logging
import sqlite3
import threading
from contextlib import contextmanager
from collections.abc import Iterator
from pathlib import Path

from miniqmt_follower.models import (
    DailyPlan,
    ExecutionStatus,
    StoredAttempt,
    StoredSignal,
    TradeSignal,
)
from miniqmt_follower.strategy_models import (
    CandidatePlan,
    CandidatePlanStatus,
    StoredCandidatePlan,
    StrategyDay,
    StrategyDayStatus,
    StrategyEvent,
    StrategyEventStatus,
)

logger = logging.getLogger(__name__)

_MANUAL_RECOVERY_STATUSES = {
    ExecutionStatus.FILLED,
    ExecutionStatus.PARTIALLY_FILLED_TIMEOUT,
    ExecutionStatus.FAILED_TIMEOUT,
    ExecutionStatus.FAILED_BROKER,
}


class PlanPayloadConflict(ValueError):
    """同一 plan_id 被重复用于不同买卖名单。"""


class CandidatePlanConflict(ValueError):
    """候选计划幂等键或同一策略交易日的内容发生冲突。"""


class StrategyEventConflict(ValueError):
    """同一规则事件唯一键被重复用于不同决策。"""


class SQLiteExecutionStore:
    """本地执行账本。

    Redis 负责传输信号, SQLite 负责记录"我到底处理过什么、下过哪些单"。
    这份本地账本是重启恢复、幂等去重和盘后审计的基础。

    使用 thread-local 持久连接: 每个线程第一次访问时打开连接并复用,
    避免每次 SQL 操作都 open/close 的开销（Windows 上尤其明显）。连接启用
    自动提交, 确保每次账本写入立即持久化且不会跨信号持有 SQLite 写锁。
    """

    def __init__(self, path: str | Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._local = threading.local()
        self._connections_lock = threading.Lock()
        self._connections: set[sqlite3.Connection] = set()
        self._init_db()

    # ---------------------------------------------------------------------------
    # 连接管理
    # ---------------------------------------------------------------------------

    @contextmanager
    def _connect(self) -> Iterator[sqlite3.Connection]:
        """创建一个新的临时连接（供测试或特殊用途，不使用线程本地缓存）。"""
        conn = sqlite3.connect(str(self.path), isolation_level=None)
        try:
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA synchronous=NORMAL")
            yield conn
        finally:
            conn.close()

    def _init_db(self) -> None:
        """首次建表（仅 __init__ 调用一次，用临时连接）。"""
        conn = sqlite3.connect(str(self.path), isolation_level=None)
        try:
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS signals (
                    signal_id TEXT PRIMARY KEY,
                    strategy_id TEXT NOT NULL,
                    action TEXT NOT NULL,
                    code TEXT NOT NULL,
                    amount INTEGER NOT NULL,
                    reference_price REAL NOT NULL,
                    created_at TEXT NOT NULL,
                    status TEXT NOT NULL,
                    filled_qty INTEGER NOT NULL DEFAULT 0,
                    raw_json TEXT NOT NULL,
                    accepted_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS order_attempts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    signal_id TEXT NOT NULL,
                    attempt_no INTEGER NOT NULL,
                    broker_order_id TEXT NOT NULL,
                    quantity INTEGER NOT NULL,
                    price REAL NOT NULL,
                    filled_qty INTEGER NOT NULL DEFAULT 0,
                    status TEXT NOT NULL,
                    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY(signal_id) REFERENCES signals(signal_id)
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS plans (
                    plan_id TEXT PRIMARY KEY,
                    strategy_id TEXT NOT NULL,
                    codes_to_sell TEXT NOT NULL,
                    codes_to_buy TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    accepted_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS candidate_plans (
                    plan_id TEXT PRIMARY KEY,
                    strategy_id TEXT NOT NULL,
                    trading_date TEXT NOT NULL,
                    schema_version INTEGER NOT NULL,
                    candidates_json TEXT NOT NULL,
                    strategy_version TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    sent_at_ms INTEGER NOT NULL,
                    mode TEXT NOT NULL,
                    status TEXT NOT NULL,
                    accepted_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            conn.execute(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS uq_candidate_plan_day
                ON candidate_plans(strategy_id, trading_date)
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS strategy_days (
                    strategy_id TEXT NOT NULL,
                    trading_date TEXT NOT NULL,
                    plan_id TEXT,
                    status TEXT NOT NULL,
                    halt_reason TEXT NOT NULL DEFAULT '',
                    started_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY(strategy_id, trading_date)
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS strategy_events (
                    strategy_id TEXT NOT NULL,
                    trading_date TEXT NOT NULL,
                    rule_name TEXT NOT NULL,
                    code TEXT NOT NULL,
                    decision TEXT NOT NULL,
                    status TEXT NOT NULL,
                    signal_id TEXT,
                    reason TEXT NOT NULL DEFAULT '',
                    market_snapshot_json TEXT NOT NULL DEFAULT '{}',
                    position_snapshot_json TEXT NOT NULL DEFAULT '{}',
                    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY(strategy_id, trading_date, rule_name, code)
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS position_highs (
                    strategy_id TEXT NOT NULL,
                    code TEXT NOT NULL,
                    high_price REAL NOT NULL,
                    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY(strategy_id, code)
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS strategy_state (
                    strategy_id TEXT NOT NULL,
                    trading_date TEXT NOT NULL,
                    key TEXT NOT NULL,
                    value TEXT NOT NULL,
                    PRIMARY KEY(strategy_id, trading_date, key)
                )
                """
            )
        finally:
            conn.close()
        logger.debug("💾 SQLite数据库已初始化 | 路径=%s", self.path)

    def _get_conn(self) -> sqlite3.Connection:
        """获取当前线程的持久 SQLite 连接（懒初始化）。"""
        if not hasattr(self._local, "conn") or self._local.conn is None:
            conn = sqlite3.connect(
                str(self.path), isolation_level=None, check_same_thread=False,
            )
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA synchronous=NORMAL")
            self._local.conn = conn
            with self._connections_lock:
                self._connections.add(conn)
        return self._local.conn

    def close(self) -> None:
        """关闭当前线程的数据库连接。"""
        if hasattr(self._local, "conn") and self._local.conn is not None:
            conn = self._local.conn
            with self._connections_lock:
                self._connections.discard(conn)
            conn.close()
            self._local.conn = None

    def close_all(self) -> None:
        """线程池停止后关闭所有工作线程创建的连接。"""
        with self._connections_lock:
            connections = tuple(self._connections)
            self._connections.clear()
        for conn in connections:
            conn.close()
        if hasattr(self._local, "conn"):
            self._local.conn = None

    def __del__(self) -> None:
        """析构时尝试关闭连接（防止 ResourceWarning）。"""
        try:
            self.close_all()
        except Exception:
            pass

    # ---------------------------------------------------------------------------
    # 数据操作（全部复用 thread-local 连接）
    # ---------------------------------------------------------------------------

    def try_accept_signal(self, signal: TradeSignal) -> bool:
        """尝试接收信号。返回 True 表示新信号, False 表示已存在。"""
        payload = json.dumps(
            {
                "signal_id": signal.signal_id,
                "strategy_id": signal.strategy_id,
                "action": signal.action.value,
                "code": signal.code,
                "amount": signal.amount,
                "reference_price": signal.reference_price,
                "created_at": signal.created_at,
                "mode": signal.mode,
                "expire_at": signal.expire_at,
                "quantity_mode": signal.quantity_mode,
                "budget_amount": signal.budget_amount,
            },
            ensure_ascii=False,
        )
        conn = self._get_conn()
        try:
            conn.execute(
                """
                INSERT INTO signals (
                    signal_id, strategy_id, action, code, amount, reference_price,
                    created_at, status, raw_json
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    signal.signal_id,
                    signal.strategy_id,
                    signal.action.value,
                    signal.code,
                    signal.amount,
                    signal.reference_price,
                    signal.created_at,
                    ExecutionStatus.ACCEPTED.value,
                    payload,
                ),
            )
            logger.debug("💾 信号已写入SQLite | signal_id=%s", signal.signal_id)
        except sqlite3.IntegrityError:
            logger.debug("💾 信号已存在(幂等拦截) | signal_id=%s", signal.signal_id)
            return False
        return True

    def try_accept_plan(self, plan: DailyPlan) -> bool:
        """记录日计划已收到（审计用）。返回 False 表示同一天已收到过该 plan。

        注意: 不是执行去重闸门 —— 执行去重由派生信号的 signal_id 幂等保证,
        这样重启后 Redis 重投 plan 时, 未受理的派生信号仍能继续执行。
        """
        conn = self._get_conn()
        try:
            conn.execute(
                """
                INSERT INTO plans (
                    plan_id, strategy_id, codes_to_sell, codes_to_buy, created_at
                )
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    plan.signal_id,
                    plan.strategy_id,
                    json.dumps(list(plan.codes_to_sell), ensure_ascii=False),
                    json.dumps(list(plan.codes_to_buy), ensure_ascii=False),
                    plan.created_at,
                ),
            )
            logger.debug("💾 日计划已写入SQLite | plan_id=%s", plan.signal_id)
        except sqlite3.IntegrityError:
            logger.debug("💾 日计划已存在(审计去重) | plan_id=%s", plan.signal_id)
            existing = conn.execute(
                """
                SELECT strategy_id, codes_to_sell, codes_to_buy
                FROM plans WHERE plan_id = ?
                """,
                (plan.signal_id,),
            ).fetchone()
            expected = (
                plan.strategy_id,
                list(plan.codes_to_sell),
                list(plan.codes_to_buy),
            )
            actual = (
                str(existing["strategy_id"]),
                json.loads(existing["codes_to_sell"]),
                json.loads(existing["codes_to_buy"]),
            )
            if actual != expected:
                raise PlanPayloadConflict(
                    f"plan_id {plan.signal_id!r} 重复但内容不一致"
                )
            return False
        return True

    def accept_candidate_plan(
        self,
        plan: CandidatePlan,
        status: CandidatePlanStatus | None = None,
    ) -> CandidatePlanStatus:
        """原子接收候选计划，并将同 ID/同日冲突持久化后显式报错。"""
        target_status = status or (
            CandidatePlanStatus.READY
            if plan.candidates
            else CandidatePlanStatus.EMPTY
        )
        conn = self._get_conn()
        conn.execute("BEGIN IMMEDIATE")
        conflict_message: str | None = None
        try:
            existing = conn.execute(
                "SELECT * FROM candidate_plans WHERE plan_id = ?", (plan.plan_id,)
            ).fetchone()
            if existing is not None:
                if self._candidate_row_matches(existing, plan):
                    conn.execute("COMMIT")
                    return CandidatePlanStatus(existing["status"])
                conn.execute(
                    """
                    UPDATE candidate_plans
                    SET status = ?, updated_at = CURRENT_TIMESTAMP
                    WHERE plan_id = ?
                    """,
                    (CandidatePlanStatus.CONFLICT.value, plan.plan_id),
                )
                self._upsert_strategy_day_conn(
                    conn,
                    plan.strategy_id,
                    plan.trading_date,
                    StrategyDayStatus.PLAN_CONFLICT,
                    plan.plan_id,
                    "同一 plan_id 内容不一致",
                )
                conflict_message = f"plan_id {plan.plan_id!r} 重复但内容不一致"
            else:
                same_day = conn.execute(
                    """
                    SELECT plan_id FROM candidate_plans
                    WHERE strategy_id = ? AND trading_date = ?
                    """,
                    (plan.strategy_id, plan.trading_date.isoformat()),
                ).fetchone()
                if same_day is not None:
                    conn.execute(
                        """
                        UPDATE candidate_plans
                        SET status = ?, updated_at = CURRENT_TIMESTAMP
                        WHERE plan_id = ?
                        """,
                        (CandidatePlanStatus.CONFLICT.value, same_day["plan_id"]),
                    )
                    self._upsert_strategy_day_conn(
                        conn,
                        plan.strategy_id,
                        plan.trading_date,
                        StrategyDayStatus.PLAN_CONFLICT,
                        str(same_day["plan_id"]),
                        "同一交易日出现多个候选计划 ID",
                    )
                    conflict_message = (
                        f"同一交易日已有计划 {same_day['plan_id']!r}，"
                        f"拒绝新计划 {plan.plan_id!r}"
                    )
                else:
                    conn.execute(
                        """
                        INSERT INTO candidate_plans (
                            plan_id, strategy_id, trading_date, schema_version,
                            candidates_json, strategy_version, created_at,
                            sent_at_ms, mode, status
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            plan.plan_id,
                            plan.strategy_id,
                            plan.trading_date.isoformat(),
                            plan.schema_version,
                            json.dumps(list(plan.candidates), ensure_ascii=False),
                            plan.strategy_version,
                            plan.created_at,
                            plan.sent_at_ms,
                            plan.mode,
                            target_status.value,
                        ),
                    )
                    day_status = (
                        StrategyDayStatus.ACTIVE
                        if target_status in {
                            CandidatePlanStatus.READY,
                            CandidatePlanStatus.EMPTY,
                        }
                        else StrategyDayStatus.NO_PLAN
                    )
                    self._upsert_strategy_day_conn(
                        conn,
                        plan.strategy_id,
                        plan.trading_date,
                        day_status,
                        plan.plan_id,
                        "",
                    )
            conn.execute("COMMIT")
        except Exception:
            if conn.in_transaction:
                conn.execute("ROLLBACK")
            raise
        if conflict_message is not None:
            raise CandidatePlanConflict(conflict_message)
        return target_status

    @staticmethod
    def _candidate_row_matches(row: sqlite3.Row, plan: CandidatePlan) -> bool:
        """判断已落库的候选计划行是否与传入计划内容完全一致。"""
        return (
            str(row["strategy_id"]) == plan.strategy_id
            and str(row["trading_date"]) == plan.trading_date.isoformat()
            and int(row["schema_version"]) == plan.schema_version
            and tuple(json.loads(row["candidates_json"])) == plan.candidates
            and str(row["strategy_version"]) == plan.strategy_version
            and str(row["created_at"]) == plan.created_at
            and int(row["sent_at_ms"]) == plan.sent_at_ms
            and str(row["mode"]) == plan.mode
        )

    def get_candidate_plan(self, plan_id: str) -> StoredCandidatePlan:
        """按 plan_id 读取已落库的候选计划；不存在时抛出 KeyError。"""
        row = self._get_conn().execute(
            "SELECT * FROM candidate_plans WHERE plan_id = ?", (plan_id,)
        ).fetchone()
        if row is None:
            raise KeyError(plan_id)
        plan = CandidatePlan(
            plan_id=str(row["plan_id"]),
            strategy_id=str(row["strategy_id"]),
            trading_date=dt.date.fromisoformat(str(row["trading_date"])),
            candidates=tuple(json.loads(row["candidates_json"])),
            schema_version=int(row["schema_version"]),
            strategy_version=str(row["strategy_version"]),
            created_at=str(row["created_at"]),
            mode=str(row["mode"]),
            sent_at_ms=int(row["sent_at_ms"]),
        )
        return StoredCandidatePlan(
            plan=plan, status=CandidatePlanStatus(row["status"])
        )

    def ensure_strategy_day(
        self,
        strategy_id: str,
        trading_date: dt.date,
        status: StrategyDayStatus = StrategyDayStatus.NO_PLAN,
    ) -> StrategyDay:
        """确保策略日记录存在，缺省按 NO_PLAN 初始化，返回落库后的记录。"""
        conn = self._get_conn()
        conn.execute(
            """
            INSERT OR IGNORE INTO strategy_days (strategy_id, trading_date, status)
            VALUES (?, ?, ?)
            """,
            (strategy_id, trading_date.isoformat(), status.value),
        )
        return self.get_strategy_day(strategy_id, trading_date)

    def update_strategy_day(
        self,
        strategy_id: str,
        trading_date: dt.date,
        status: StrategyDayStatus,
        *,
        plan_id: str | None = None,
        halt_reason: str = "",
    ) -> None:
        """更新策略日的状态、计划 ID 与熔断原因。"""
        conn = self._get_conn()
        self._upsert_strategy_day_conn(
            conn, strategy_id, trading_date, status, plan_id, halt_reason
        )

    @staticmethod
    def _upsert_strategy_day_conn(
        conn: sqlite3.Connection,
        strategy_id: str,
        trading_date: dt.date,
        status: StrategyDayStatus,
        plan_id: str | None,
        halt_reason: str,
    ) -> None:
        """向已开事务的连接写入/更新策略日；计划 ID 仅在传入时覆盖旧值。"""
        conn.execute(
            """
            INSERT INTO strategy_days (
                strategy_id, trading_date, plan_id, status, halt_reason
            ) VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(strategy_id, trading_date) DO UPDATE SET
                plan_id = COALESCE(excluded.plan_id, strategy_days.plan_id),
                status = excluded.status,
                halt_reason = excluded.halt_reason,
                updated_at = CURRENT_TIMESTAMP
            """,
            (
                strategy_id,
                trading_date.isoformat(),
                plan_id,
                status.value,
                halt_reason,
            ),
        )

    def get_strategy_day(
        self, strategy_id: str, trading_date: dt.date
    ) -> StrategyDay:
        """读取策略日记录；不存在时抛出 KeyError。"""
        row = self._get_conn().execute(
            """
            SELECT strategy_id, trading_date, plan_id, status, halt_reason
            FROM strategy_days WHERE strategy_id = ? AND trading_date = ?
            """,
            (strategy_id, trading_date.isoformat()),
        ).fetchone()
        if row is None:
            raise KeyError((strategy_id, trading_date))
        return StrategyDay(
            strategy_id=str(row["strategy_id"]),
            trading_date=dt.date.fromisoformat(str(row["trading_date"])),
            plan_id=str(row["plan_id"]) if row["plan_id"] is not None else None,
            status=StrategyDayStatus(row["status"]),
            halt_reason=str(row["halt_reason"]),
        )

    def create_strategy_event(self, event: StrategyEvent) -> bool:
        """插入单条策略事件，重复键且内容一致时返回 False，内容冲突则抛错。"""
        conn = self._get_conn()
        market_json = json.dumps(
            event.market_snapshot or {}, ensure_ascii=False, sort_keys=True, default=str
        )
        position_json = json.dumps(
            event.position_snapshot or {}, ensure_ascii=False, sort_keys=True, default=str
        )
        try:
            conn.execute(
                """
                INSERT INTO strategy_events (
                    strategy_id, trading_date, rule_name, code, decision,
                    status, signal_id, reason, market_snapshot_json,
                    position_snapshot_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    event.strategy_id,
                    event.trading_date.isoformat(),
                    event.rule_name,
                    event.code,
                    event.decision,
                    event.status.value,
                    event.signal_id,
                    event.reason,
                    market_json,
                    position_json,
                ),
            )
            return True
        except sqlite3.IntegrityError:
            existing = self.get_strategy_event(
                event.strategy_id, event.trading_date, event.rule_name, event.code
            )
            normalized_event = StrategyEvent(
                strategy_id=event.strategy_id,
                trading_date=event.trading_date,
                rule_name=event.rule_name,
                code=event.code,
                decision=event.decision,
                status=event.status,
                signal_id=event.signal_id,
                reason=event.reason,
                market_snapshot=json.loads(market_json),
                position_snapshot=json.loads(position_json),
            )
            if existing != normalized_event:
                raise StrategyEventConflict(
                    f"策略事件已存在但内容不一致: {event.rule_name}/{event.code}"
                )
            return False

    def create_strategy_events(self, events: tuple[StrategyEvent, ...]) -> None:
        """在一个事务里持久化完整决策批次，避免崩溃留下半批兄弟事件。"""
        if not events:
            return
        conn = self._get_conn()
        conn.execute("BEGIN IMMEDIATE")
        try:
            for event in events:
                conn.execute(
                    """
                    INSERT INTO strategy_events (
                        strategy_id, trading_date, rule_name, code, decision,
                        status, signal_id, reason, market_snapshot_json,
                        position_snapshot_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        event.strategy_id,
                        event.trading_date.isoformat(),
                        event.rule_name,
                        event.code,
                        event.decision,
                        event.status.value,
                        event.signal_id,
                        event.reason,
                        json.dumps(
                            event.market_snapshot or {},
                            ensure_ascii=False,
                            sort_keys=True,
                            default=str,
                        ),
                        json.dumps(
                            event.position_snapshot or {},
                            ensure_ascii=False,
                            sort_keys=True,
                            default=str,
                        ),
                    ),
                )
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise

    def upsert_retryable_strategy_event(self, event: StrategyEvent) -> bool:
        """记录数据阻断观察；新鲜决策可覆盖 BLOCKED_DATA 并继续提交。"""
        conn = self._get_conn()
        market_json = json.dumps(
            event.market_snapshot or {}, ensure_ascii=False, sort_keys=True, default=str
        )
        position_json = json.dumps(
            event.position_snapshot or {}, ensure_ascii=False, sort_keys=True, default=str
        )
        conn.execute("BEGIN IMMEDIATE")
        try:
            row = conn.execute(
                """
                SELECT status FROM strategy_events
                WHERE strategy_id = ? AND trading_date = ? AND rule_name = ? AND code = ?
                """,
                (
                    event.strategy_id,
                    event.trading_date.isoformat(),
                    event.rule_name,
                    event.code,
                ),
            ).fetchone()
            if row is None:
                conn.execute(
                    """
                    INSERT INTO strategy_events (
                        strategy_id, trading_date, rule_name, code, decision,
                        status, signal_id, reason, market_snapshot_json,
                        position_snapshot_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        event.strategy_id,
                        event.trading_date.isoformat(),
                        event.rule_name,
                        event.code,
                        event.decision,
                        event.status.value,
                        event.signal_id,
                        event.reason,
                        market_json,
                        position_json,
                    ),
                )
                should_submit = event.status != StrategyEventStatus.BLOCKED_DATA
            elif StrategyEventStatus(row["status"]) == StrategyEventStatus.BLOCKED_DATA:
                conn.execute(
                    """
                    UPDATE strategy_events
                    SET decision = ?, status = ?, signal_id = ?, reason = ?,
                        market_snapshot_json = ?, position_snapshot_json = ?,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE strategy_id = ? AND trading_date = ? AND rule_name = ? AND code = ?
                    """,
                    (
                        event.decision,
                        event.status.value,
                        event.signal_id,
                        event.reason,
                        market_json,
                        position_json,
                        event.strategy_id,
                        event.trading_date.isoformat(),
                        event.rule_name,
                        event.code,
                    ),
                )
                should_submit = event.status != StrategyEventStatus.BLOCKED_DATA
            else:
                should_submit = False
            conn.execute("COMMIT")
            return should_submit
        except Exception:
            conn.execute("ROLLBACK")
            raise

    def update_strategy_event(
        self,
        strategy_id: str,
        trading_date: dt.date,
        rule_name: str,
        code: str,
        status: StrategyEventStatus,
    ) -> None:
        """更新策略事件状态；未命中恰好一行时抛出 KeyError。"""
        cursor = self._get_conn().execute(
            """
            UPDATE strategy_events
            SET status = ?, updated_at = CURRENT_TIMESTAMP
            WHERE strategy_id = ? AND trading_date = ? AND rule_name = ? AND code = ?
            """,
            (status.value, strategy_id, trading_date.isoformat(), rule_name, code),
        )
        if cursor.rowcount != 1:
            raise KeyError((strategy_id, trading_date, rule_name, code))

    def get_strategy_event(
        self,
        strategy_id: str,
        trading_date: dt.date,
        rule_name: str,
        code: str,
    ) -> StrategyEvent:
        """读取单条策略事件；不存在时抛出 KeyError。"""
        row = self._get_conn().execute(
            """
            SELECT * FROM strategy_events
            WHERE strategy_id = ? AND trading_date = ? AND rule_name = ? AND code = ?
            """,
            (strategy_id, trading_date.isoformat(), rule_name, code),
        ).fetchone()
        if row is None:
            raise KeyError((strategy_id, trading_date, rule_name, code))
        return StrategyEvent(
            strategy_id=str(row["strategy_id"]),
            trading_date=dt.date.fromisoformat(str(row["trading_date"])),
            rule_name=str(row["rule_name"]),
            code=str(row["code"]),
            decision=str(row["decision"]),
            status=StrategyEventStatus(row["status"]),
            signal_id=str(row["signal_id"]) if row["signal_id"] is not None else None,
            reason=str(row["reason"]),
            market_snapshot=json.loads(row["market_snapshot_json"]),
            position_snapshot=json.loads(row["position_snapshot_json"]),
        )

    def get_strategy_event_optional(
        self,
        strategy_id: str,
        trading_date: dt.date,
        rule_name: str,
        code: str,
    ) -> StrategyEvent | None:
        """读取策略事件，不存在时返回 None 而非抛错。"""
        try:
            return self.get_strategy_event(strategy_id, trading_date, rule_name, code)
        except KeyError:
            return None

    # ---------------------------------------------------------------------------
    # 买入以来最高价账本 (回落止盈规则)
    # ---------------------------------------------------------------------------

    def get_position_high(self, strategy_id: str, code: str) -> float | None:
        """返回某持仓买入以来的最高价记录，无记录时返回 None。"""
        row = self._get_conn().execute(
            "SELECT high_price FROM position_highs "
            "WHERE strategy_id = ? AND code = ?",
            (strategy_id, code),
        ).fetchone()
        return None if row is None else float(row["high_price"])

    def upsert_position_high(
        self, strategy_id: str, code: str, high_price: float
    ) -> None:
        """写入/更新买入以来最高价（只升不降，旧值更高时保持原值）。"""
        self._get_conn().execute(
            """
            INSERT INTO position_highs (strategy_id, code, high_price, updated_at)
            VALUES (?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(strategy_id, code) DO UPDATE SET
                high_price = MAX(position_highs.high_price, excluded.high_price),
                updated_at = CURRENT_TIMESTAMP
            """,
            (strategy_id, code, high_price),
        )

    def delete_position_high(self, strategy_id: str, code: str) -> None:
        """清仓后删除买入以来最高价记录。"""
        self._get_conn().execute(
            "DELETE FROM position_highs WHERE strategy_id = ? AND code = ?",
            (strategy_id, code),
        )

    def list_position_highs(self, strategy_id: str) -> dict[str, float]:
        """列出某策略全部持仓的买入以来最高价记录。"""
        rows = self._get_conn().execute(
            "SELECT code, high_price FROM position_highs WHERE strategy_id = ?",
            (strategy_id,),
        ).fetchall()
        return {row["code"]: float(row["high_price"]) for row in rows}

    def list_strategy_events(
        self,
        strategy_id: str,
        trading_date: dt.date,
        *,
        status: StrategyEventStatus | None = None,
    ) -> tuple[StrategyEvent, ...]:
        """按策略与交易日列出事件，可按状态过滤。"""
        conn = self._get_conn()
        sql = (
            "SELECT rule_name, code FROM strategy_events "
            "WHERE strategy_id = ? AND trading_date = ?"
        )
        params: list[object] = [strategy_id, trading_date.isoformat()]
        if status is not None:
            sql += " AND status = ?"
            params.append(status.value)
        sql += " ORDER BY created_at, rule_name, code"
        rows = conn.execute(sql, params).fetchall()
        return tuple(
            self.get_strategy_event(strategy_id, trading_date, row["rule_name"], row["code"])
            for row in rows
        )

    def list_outstanding_strategy_events(
        self, strategy_id: str
    ) -> tuple[StrategyEvent, ...]:
        """跨交易日返回必须在新决策前核对的内部事件。"""
        statuses = (
            StrategyEventStatus.TRIGGERED.value,
            StrategyEventStatus.SUBMITTED.value,
            StrategyEventStatus.BLOCKED_ACCOUNT_HALT.value,
        )
        rows = self._get_conn().execute(
            """
            SELECT trading_date, rule_name, code FROM strategy_events
            WHERE strategy_id = ? AND status IN (?, ?, ?)
            ORDER BY trading_date, created_at, rule_name, code
            """,
            (strategy_id, *statuses),
        ).fetchall()
        return tuple(
            self.get_strategy_event(
                strategy_id,
                dt.date.fromisoformat(str(row["trading_date"])),
                str(row["rule_name"]),
                str(row["code"]),
            )
            for row in rows
        )

    def get_strategy_state(
        self, strategy_id: str, trading_date: dt.date, key: str
    ) -> str | None:
        """读取策略跨重启状态(键值); 无记录返回 None。"""
        row = self._get_conn().execute(
            """
            SELECT value FROM strategy_state
            WHERE strategy_id = ? AND trading_date = ? AND key = ?
            """,
            (strategy_id, trading_date.isoformat(), key),
        ).fetchone()
        return None if row is None else str(row["value"])

    def set_strategy_state(
        self, strategy_id: str, trading_date: dt.date, key: str, value: str
    ) -> None:
        """写入策略跨重启状态(键值, upsert)。"""
        self._get_conn().execute(
            """
            INSERT INTO strategy_state (strategy_id, trading_date, key, value)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(strategy_id, trading_date, key)
            DO UPDATE SET value = excluded.value
            """,
            (strategy_id, trading_date.isoformat(), key, value),
        )

    def count_signals_like(self, prefix: str) -> int:
        """统计 signal_id 以给定前缀开头的信号数量(用于补仓波次编号重建)。"""
        row = self._get_conn().execute(
            "SELECT COUNT(*) AS n FROM signals WHERE signal_id LIKE ?",
            (prefix + "%",),
        ).fetchone()
        return int(row["n"]) if row is not None else 0

    def update_signal_status(
        self, signal_id: str, status: ExecutionStatus, *, filled_qty: int | None = None
    ) -> None:
        """更新信号终态或中间状态。"""
        conn = self._get_conn()
        if filled_qty is None:
            conn.execute(
                "UPDATE signals SET status = ?, updated_at = CURRENT_TIMESTAMP WHERE signal_id = ?",
                (status.value, signal_id),
            )
        else:
            conn.execute(
                """
                UPDATE signals
                SET status = ?, filled_qty = ?, updated_at = CURRENT_TIMESTAMP
                WHERE signal_id = ?
                """,
                (status.value, filled_qty, signal_id),
            )

    def record_attempt(
        self,
        signal_id: str,
        attempt_no: int,
        broker_order_id: str,
        quantity: int,
        price: float,
        status: str,
        filled_qty: int = 0,
    ) -> None:
        """记录一次委托尝试。"""
        conn = self._get_conn()
        conn.execute(
            """
            INSERT INTO order_attempts (
                signal_id, attempt_no, broker_order_id, quantity, price, status, filled_qty
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (signal_id, attempt_no, broker_order_id, quantity, price, status, filled_qty),
        )

    def update_attempt(self, broker_order_id: str, status: str, filled_qty: int) -> None:
        """根据券商订单号更新某次委托的状态和成交量。"""
        conn = self._get_conn()
        conn.execute(
            """
            UPDATE order_attempts
            SET status = ?, filled_qty = ?, updated_at = CURRENT_TIMESTAMP
            WHERE broker_order_id = ?
            """,
            (status, filled_qty, broker_order_id),
        )

    def get_signal(self, signal_id: str) -> StoredSignal:
        """读取已接收信号的当前状态。"""
        conn = self._get_conn()
        row = conn.execute(
            "SELECT signal_id, status, filled_qty FROM signals WHERE signal_id = ?",
            (signal_id,),
        ).fetchone()
        if row is None:
            raise KeyError(signal_id)
        return StoredSignal(
            signal_id=row["signal_id"],
            status=ExecutionStatus(row["status"]),
            filled_qty=int(row["filled_qty"]),
        )

    def get_signal_optional(self, signal_id: str) -> StoredSignal | None:
        """读取信号；尚未登记时返回 None，供 Redis 遗留消息恢复分流。"""
        try:
            return self.get_signal(signal_id)
        except KeyError:
            return None

    def list_attempts(self, signal_id: str) -> tuple[StoredAttempt, ...]:
        """按信号列出其全部委托尝试，依 attempt_no 与 id 排序。"""
        conn = self._get_conn()
        rows = conn.execute(
            """
            SELECT attempt_no, broker_order_id, quantity, price, status, filled_qty
            FROM order_attempts
            WHERE signal_id = ?
            ORDER BY attempt_no, id
            """,
            (signal_id,),
        ).fetchall()
        return tuple(
            StoredAttempt(
                attempt_no=int(row["attempt_no"]),
                broker_order_id=str(row["broker_order_id"]),
                quantity=int(row["quantity"]),
                price=float(row["price"]),
                status=str(row["status"]),
                filled_qty=int(row["filled_qty"]),
            )
            for row in rows
        )

    def upsert_recovered_attempt(
        self,
        signal_id: str,
        broker_order_id: str,
        quantity: int,
        price: float,
        status: str,
        filled_qty: int,
    ) -> None:
        """补记崩溃窗口内 QMT 已受理、但本机尚未来得及记录的委托。"""
        conn = self._get_conn()
        row = conn.execute(
            "SELECT id FROM order_attempts WHERE broker_order_id = ?",
            (broker_order_id,),
        ).fetchone()
        if row is not None:
            conn.execute(
                """
                UPDATE order_attempts
                SET quantity = ?, price = ?, status = ?, filled_qty = ?,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
                """,
                (quantity, price, status, filled_qty, row["id"]),
            )
            return
        next_attempt = conn.execute(
            "SELECT COALESCE(MAX(attempt_no), 0) + 1 FROM order_attempts WHERE signal_id = ?",
            (signal_id,),
        ).fetchone()[0]
        self.record_attempt(
            signal_id,
            int(next_attempt),
            broker_order_id,
            quantity,
            price,
            status,
            filled_qty,
        )

    def resolve_recovery(
        self,
        signal_id: str,
        status: ExecutionStatus,
        *,
        filled_qty: int,
    ) -> None:
        """人工在 QMT 完成对账后，把不确定信号收口到明确终态。"""
        current = self.get_signal(signal_id)
        if current.status != ExecutionStatus.RECOVERY_REQUIRED:
            raise ValueError(
                f"只能处理 recovery_required 信号，当前为 {current.status.value}"
            )
        if status not in _MANUAL_RECOVERY_STATUSES:
            allowed = ", ".join(sorted(item.value for item in _MANUAL_RECOVERY_STATUSES))
            raise ValueError(f"人工对账终态只能为: {allowed}")
        if int(filled_qty) < 0:
            raise ValueError("filled_qty 不能小于0")
        self.update_signal_status(
            signal_id, status, filled_qty=int(filled_qty),
        )

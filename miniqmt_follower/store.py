"""本地执行账本（SQLite）。

每个线程持有一个持久连接，避免每次操作都 open/close 的开销。
配合 WAL 模式，读写并发友好。
"""

from __future__ import annotations

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

logger = logging.getLogger(__name__)

_MANUAL_RECOVERY_STATUSES = {
    ExecutionStatus.FILLED,
    ExecutionStatus.PARTIALLY_FILLED_TIMEOUT,
    ExecutionStatus.FAILED_TIMEOUT,
    ExecutionStatus.FAILED_BROKER,
}


class PlanPayloadConflict(ValueError):
    """同一 plan_id 被重复用于不同买卖名单。"""


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

    # ------------------------------------------------------------------
    # 连接管理
    # ------------------------------------------------------------------

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

    # ------------------------------------------------------------------
    # 数据操作（全部复用 thread-local 连接）
    # ------------------------------------------------------------------

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

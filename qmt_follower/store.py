"""本地执行账本（SQLite）。

每个线程持有一个持久连接，避免每次操作都 open/close 的开销。
配合 WAL 模式，读写并发友好。
"""

from __future__ import annotations

import json
import logging
import sqlite3
import threading
from pathlib import Path

from qmt_follower.models import ExecutionStatus, StoredSignal, TradeSignal

logger = logging.getLogger(__name__)


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
        self._init_db()

    # ------------------------------------------------------------------
    # 连接管理
    # ------------------------------------------------------------------

    def _connect(self) -> sqlite3.Connection:
        """创建一个新的临时连接（供测试或特殊用途，不使用线程本地缓存）。"""
        conn = sqlite3.connect(str(self.path), isolation_level=None)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA synchronous=NORMAL")
        return conn

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
        finally:
            conn.close()
        logger.debug("💾 SQLite数据库已初始化 | 路径=%s", self.path)

    def _get_conn(self) -> sqlite3.Connection:
        """获取当前线程的持久 SQLite 连接（懒初始化）。"""
        if not hasattr(self._local, "conn") or self._local.conn is None:
            conn = sqlite3.connect(str(self.path), isolation_level=None)
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA synchronous=NORMAL")
            self._local.conn = conn
        return self._local.conn

    def close(self) -> None:
        """关闭当前线程的数据库连接。"""
        if hasattr(self._local, "conn") and self._local.conn is not None:
            self._local.conn.close()
            self._local.conn = None

    def __del__(self) -> None:
        """析构时尝试关闭连接（防止 ResourceWarning）。"""
        try:
            self.close()
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

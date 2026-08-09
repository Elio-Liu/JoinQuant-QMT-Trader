"""人工核对 QMT 后，安全收口 recovery_required 信号。"""

from __future__ import annotations

import argparse
from collections.abc import Sequence

from miniqmt_follower.models import ExecutionStatus
from miniqmt_follower.process_lock import SingleInstanceLock
from miniqmt_follower.store import SQLiteExecutionStore


_STATUS_CHOICES = (
    ExecutionStatus.FILLED.value,
    ExecutionStatus.PARTIALLY_FILLED_TIMEOUT.value,
    ExecutionStatus.FAILED_TIMEOUT.value,
    ExecutionStatus.FAILED_BROKER.value,
)


def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="停止交易程序并在 QMT 委托列表对账后，收口一条恢复待办。"
    )
    parser.add_argument("--state-db", required=True, help="该账号的 SQLite 账本路径")
    parser.add_argument("--signal-id", required=True)
    parser.add_argument("--status", required=True, choices=_STATUS_CHOICES)
    parser.add_argument("--filled-qty", required=True, type=int)
    parser.add_argument(
        "--confirm-qmt-reconciled",
        action="store_true",
        help="确认已在 QMT 委托/成交列表核对该 signal_id",
    )
    args = parser.parse_args(argv)
    if not args.confirm_qmt_reconciled:
        parser.error("必须先核对 QMT，然后加 --confirm-qmt-reconciled")
    if args.filled_qty < 0:
        parser.error("--filled-qty 不能小于0")
    return args


def main(argv: Sequence[str] | None = None) -> int:
    args = _parse_args(argv)
    lock = SingleInstanceLock(str(args.state_db) + ".lock")
    lock.acquire()
    store = SQLiteExecutionStore(args.state_db)
    try:
        store.resolve_recovery(
            args.signal_id,
            ExecutionStatus(args.status),
            filled_qty=args.filled_qty,
        )
    finally:
        store.close_all()
        lock.release()
    print(
        f"已收口信号 {args.signal_id}: "
        f"status={args.status}, filled_qty={args.filled_qty}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

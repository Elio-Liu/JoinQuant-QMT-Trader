#!/usr/bin/env python3
"""只读复盘: 统计 state_db 的信号终态与委托尝试分布, 支撑分机调参。

用法: python tools/analyze_attempts.py --db data/gj.db
"""
import argparse
import sqlite3


def _tables(db_path: str):
    """以只读方式打开账本，统计信号终态、委托尝试、重挂与报单时刻分布。"""
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    status = conn.execute(
        "SELECT action, status, COUNT(*) n FROM signals "
        "GROUP BY action, status ORDER BY action, n DESC"
    ).fetchall()
    attempts = conn.execute(
        "SELECT attempt_no, COUNT(*) n FROM order_attempts "
        "GROUP BY attempt_no ORDER BY attempt_no"
    ).fetchall()
    relist = conn.execute(
        "SELECT COUNT(*) n FROM (SELECT signal_id FROM order_attempts "
        "GROUP BY signal_id HAVING MAX(attempt_no) >= 2)"
    ).fetchone()["n"]
    hourly = conn.execute(
        "SELECT substr(created_at, 12, 2) hh, COUNT(*) n FROM order_attempts "
        "GROUP BY hh ORDER BY hh"
    ).fetchall()
    conn.close()
    return status, attempts, relist, hourly


def main() -> None:
    """解析参数后打印信号终态、委托尝试、重挂指标与报单时刻分布。"""
    parser = argparse.ArgumentParser(description="复盘执行账本")
    parser.add_argument("--db", default="data/miniqmt_follower.db")
    args = parser.parse_args()
    status, attempts, relist, hourly = _tables(args.db)
    print("== 信号终态分布 (action × status) ==")
    for row in status:
        print(f"  {row['action']:<4} {row['status']:<28} {row['n']:>6}")
    total_attempts = sum(r["n"] for r in attempts)
    print("\n== 委托尝试分布 (attempt_no) ==")
    for row in attempts:
        print(f"  第{row['attempt_no']}次挂单: {row['n']:>6} "
              f"({row['n'] / total_attempts * 100:.1f}%)")
    print(f"\n== 重挂指标 ==")
    print(f"  挂过 ≥2 次单的信号数: {relist}")
    print(f"  重挂尝试占比(第2次及以后 / 全部): "
          f"{sum(r['n'] for r in attempts if r['attempt_no'] >= 2) / total_attempts * 100:.1f}%")
    print("\n== 报单时刻分布 (按小时) ==")
    for row in hourly:
        print(f"  {row['hh']}:00 档: {row['n']:>6}")


if __name__ == "__main__":
    main()

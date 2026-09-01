#!/usr/bin/env python3
"""统计撤单确认耗时分布: 慢撤单券商开盘实测 ~16-18s, 是"撤单重挂能否成立"的命门。

用法: python tools/analyze_cancel_latency.py logs/gj/miniqmt_follower.log
"""
import argparse
import re
from datetime import datetime

_TS = r"\[(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3})\]"
_REQ = re.compile(_TS + r".*🔙 撤单请求已提交 \| QMT单号=(\d+)")
_CONF = re.compile(
    _TS + r".*🔙 撤单终态已确认 \| broker单号=(\d+) 状态=\S+ 成交=(\d+)"
)


def main() -> None:
    """解析参数后统计撤单确认耗时分布、撤单期间捡回的成交与未确认笔数。"""
    parser = argparse.ArgumentParser(description="统计撤单确认耗时分布")
    parser.add_argument("logfile")
    args = parser.parse_args()
    pending: dict[str, datetime] = {}
    latencies: list[float] = []
    fills: list[int] = []
    with open(args.logfile, encoding="utf-8") as fh:
        for line in fh:
            m = _REQ.search(line)
            if m:
                pending[m.group(2)] = datetime.strptime(
                    m.group(1), "%Y-%m-%d %H:%M:%S.%f"
                )
                continue
            m = _CONF.search(line)
            if m and m.group(2) in pending:
                start = pending.pop(m.group(2))
                end = datetime.strptime(m.group(1), "%Y-%m-%d %H:%M:%S.%f")
                latencies.append((end - start).total_seconds())
                fills.append(int(m.group(3)))
    latencies.sort()
    print(f"样本数: {len(latencies)}")
    if latencies:
        print(
            f"确认耗时: 最小 {latencies[0]:.1f}s / "
            f"中位 {latencies[len(latencies) // 2]:.1f}s / "
            f"P90 {latencies[int(len(latencies) * 0.9)]:.1f}s / "
            f"最大 {latencies[-1]:.1f}s"
        )
        print(f"撤单期间捡回成交的样本数: {sum(1 for q in fills if q > 0)} / "
              f"总成交量: {sum(fills)}股")
    if pending:
        print(f"仍未确认的撤单: {len(pending)} 笔")


if __name__ == "__main__":
    main()

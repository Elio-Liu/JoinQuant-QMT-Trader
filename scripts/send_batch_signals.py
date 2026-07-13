"""向 Redis Stream 快速发送一批模拟盘交易信号，测试 Windows/QMT 批量执行能力。

使用前先确认 Windows 端连接的是 miniQMT 模拟账号、同一 Redis 消费组没有实盘执行端
在线，并核对 SIGNALS 中的参考价仍在实时行情附近。执行端默认会拒绝与参考价偏离
超过 2% 的订单。

运行方式:
    python scripts/send_batch_signals.py

脚本会先校验并打印完整批次，只有输入动态生成的确认口令后才会真正发送。
"""

from __future__ import annotations

import datetime as dt
import sys
import time
import uuid
from collections import defaultdict
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.redis_target_config import load_redis_target

# ---------------------------------------------------------------------------
# 手动配置区
# ---------------------------------------------------------------------------

TARGET = "remote_prod"
STRATEGY_ID = "manual_stress"

# 相邻两条 Redis 信号之间的等待秒数。0=无额外等待；0.10=间隔 100ms。
SEND_INTERVAL_SEC = 0.10

# 卖出安全上限。修改或新增卖出标的时，必须同步填写实际可卖数量。
MAX_SELL_QUANTITIES = {
    "517110.XSHG": 13700,
}

# 按日志设计的 10 笔压力测试：先发 5 笔卖出，再发 5 笔买入。
# reference_price 必须在运行前根据模拟盘实时行情重新核对。
SIGNALS = [
    {"code": "517110.XSHG", "action": "sell", "amount": 1000, "reference_price": 0.741},
    {"code": "517110.XSHG", "action": "sell", "amount": 1000, "reference_price": 0.741},
    {"code": "517110.XSHG", "action": "sell", "amount": 1000, "reference_price": 0.741},
    {"code": "517110.XSHG", "action": "sell", "amount": 1000, "reference_price": 0.741},
    {"code": "517110.XSHG", "action": "sell", "amount": 1000, "reference_price": 0.741},
    {"code": "159309.XSHE", "action": "buy", "amount": 100, "reference_price": 1.274},
    {"code": "159309.XSHE", "action": "buy", "amount": 100, "reference_price": 1.274},
    {"code": "159309.XSHE", "action": "buy", "amount": 100, "reference_price": 1.274},
    {"code": "159309.XSHE", "action": "buy", "amount": 100, "reference_price": 1.274},
    {"code": "159309.XSHE", "action": "buy", "amount": 100, "reference_price": 1.274},
]


def validate_batch(signals, max_sell_quantities, interval_sec: float) -> None:
    """发送前校验批次，避免超持仓卖出或构造无效委托。"""
    if not signals:
        raise ValueError("SIGNALS 不能为空")
    if interval_sec < 0:
        raise ValueError("SEND_INTERVAL_SEC 不能小于 0")

    sell_totals = defaultdict(int)
    for index, signal in enumerate(signals, start=1):
        try:
            code = str(signal["code"])
            action = str(signal["action"]).lower()
            amount = signal["amount"]
            reference_price = float(signal["reference_price"])
        except (KeyError, TypeError, ValueError) as exc:
            raise ValueError(f"第{index}笔信号字段不完整或类型错误: {signal}") from exc

        if action not in {"buy", "sell"}:
            raise ValueError(f"第{index}笔 action 必须是 buy 或 sell: {action}")
        if not isinstance(amount, int) or isinstance(amount, bool) or amount <= 0:
            raise ValueError(f"第{index}笔 amount 必须是正整数: {amount}")
        if amount % 100 != 0:
            raise ValueError(f"第{index}笔 amount 必须是 100 的整数倍: {amount}")
        if reference_price <= 0:
            raise ValueError(f"第{index}笔 reference_price 必须大于 0: {reference_price}")
        if not code.endswith((".XSHG", ".XSHE")):
            raise ValueError(f"第{index}笔必须使用聚宽格式代码: {code}")

        if action == "sell":
            if code not in max_sell_quantities:
                raise ValueError(f"卖出标的 {code} 未配置可卖数量上限")
            sell_totals[code] += amount

    for code, total in sell_totals.items():
        limit = int(max_sell_quantities[code])
        if total > limit:
            raise ValueError(f"{code} 批次卖出合计 {total} 股，超过可卖上限 {limit} 股")


def build_batch_id() -> str:
    timestamp = dt.datetime.now().strftime("%Y%m%d%H%M%S")
    return f"{timestamp}-{uuid.uuid4().hex[:6]}"


def build_payloads(signals, strategy_id: str, batch_id: str) -> list[dict]:
    """按配置顺序构造 payload；发送时间在实际 XADD 前写入。"""
    payloads = []
    for sequence, signal in enumerate(signals, start=1):
        code = str(signal["code"])
        action = str(signal["action"]).lower()
        amount = int(signal["amount"])
        code_flat = code.replace(".", "")
        signal_id = (
            f"stress-{strategy_id}-{batch_id}-{sequence:02d}-"
            f"{code_flat}-{action}-{amount}"
        )
        payloads.append(
            {
                "signal_id": signal_id,
                "strategy_id": strategy_id,
                "mode": "live",
                "action": action,
                "code": code,
                "amount": amount,
                "reference_price": float(signal["reference_price"]),
            }
        )
    return payloads


def publish_batch(
    stream,
    payloads,
    interval_sec: float,
    *,
    now_fn=dt.datetime.now,
    sleep_fn=time.sleep,
) -> list[dict]:
    """顺序写入 Redis；只在相邻消息之间等待，不在最后一笔后额外休眠。"""
    if interval_sec < 0:
        raise ValueError("interval_sec 不能小于 0")

    published = []
    for index, payload in enumerate(payloads):
        now = now_fn()
        outgoing = payload.copy()
        outgoing["created_at"] = now.strftime("%Y-%m-%d %H:%M:%S")
        outgoing["sent_at_ms"] = int(now.timestamp() * 1000)
        message_id = stream.publish_signal(outgoing)
        published.append(
            {
                "signal_id": outgoing["signal_id"],
                "message_id": message_id,
            }
        )
        if index < len(payloads) - 1 and interval_sec > 0:
            sleep_fn(interval_sec)
    return published


def main() -> None:
    from qmt_follower.config import RedisConfig
    from qmt_follower.redis_stream import RedisStreamClient

    validate_batch(SIGNALS, MAX_SELL_QUANTITIES, SEND_INTERVAL_SEC)
    batch_id = build_batch_id()
    payloads = build_payloads(SIGNALS, STRATEGY_ID, batch_id)
    target = load_redis_target(TARGET)

    print(f"目标 Redis: {TARGET} ({target['host']}:{target['port']}, stream={target['stream']})")
    print(
        f"批次: {batch_id} | 信号数={len(payloads)} | 间隔={SEND_INTERVAL_SEC:.3f}秒 | "
        f"预计发送耗时={(len(payloads) - 1) * SEND_INTERVAL_SEC:.3f}秒"
    )
    for index, payload in enumerate(payloads, start=1):
        print(
            f"  {index:02d}. {payload['action']} {payload['code']} "
            f"{payload['amount']}股 @{payload['reference_price']:.3f}"
        )

    print("\n注意: 请先确认同一 Redis 消费组只有模拟盘执行端在线。")
    print("请核对 517110 和 159309 的实时价格仍在参考价 2% 范围内。")
    confirmation = f"SEND {batch_id}"
    if input(f"确认模拟盘批量发送请输入 {confirmation}: ").strip() != confirmation:
        print("已取消，未发送任何信号。")
        return

    redis_config = RedisConfig(
        host=target["host"],
        port=target["port"],
        password=target["password"],
        stream=target["stream"],
        group="qmt_executors",
        consumer="manual-stress-script",
    )
    stream = RedisStreamClient(redis_config)
    started = time.monotonic()
    published = publish_batch(stream, payloads, SEND_INTERVAL_SEC)
    elapsed = time.monotonic() - started

    for index, item in enumerate(published, start=1):
        print(
            f"  {index:02d}. 已发送 | signal_id={item['signal_id']} "
            f"redis_message_id={item['message_id']}"
        )
    print(f"批次发送完成 | 数量={len(published)} Redis发送耗时={elapsed:.3f}秒")
    print("请到 Windows 日志核对收到信号、首笔委托耗时、尝试次数、终态和端到端耗时。")


if __name__ == "__main__":
    main()

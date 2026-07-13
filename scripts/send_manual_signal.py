"""手动向 Redis Stream 发送一条交易信号。

Redis 目标统一从同目录的 ``redis_targets.yaml`` 读取。该文件包含本机
真实连接信息并已被 Git 忽略，脚本和控制台都不输出密码。

修改下方手动信号参数后运行 ``python scripts/send_manual_signal.py``。发送前必须
输入 ``yes`` 确认；请仅连接模拟盘或在人工盯盘的小额环境中使用。
"""

from __future__ import annotations

import datetime as dt
import sys
import uuid
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.redis_target_config import load_redis_target

# ---------------------------------------------------------------------------
# 手动配置区
# ---------------------------------------------------------------------------

TARGET = "remote_prod"
CODE = "589700.XSHG"
ACTION = "buy"
AMOUNT = 100
REFERENCE_PRICE = 2.445
STRATEGY_ID = "manual"
SIGNAL_ID = None


def build_signal_id(strategy_id: str, code: str, action: str, amount: int) -> str:
    timestamp = dt.datetime.now().strftime("%Y%m%d%H%M%S")
    code_flat = code.replace(".", "")
    suffix = uuid.uuid4().hex[:6]
    return f"manual-{strategy_id}-{timestamp}-{code_flat}-{action}-{amount}-{suffix}"


def main() -> None:
    from qmt_follower.config import RedisConfig
    from qmt_follower.redis_stream import RedisStreamClient

    target = load_redis_target(TARGET)
    redis_config = RedisConfig(
        host=str(target["host"]),
        port=int(target["port"]),
        password=target["password"],
        stream=str(target["stream"]),
        group="qmt_executors",
        consumer="manual-script",
    )
    signal_id = SIGNAL_ID or build_signal_id(STRATEGY_ID, CODE, ACTION, AMOUNT)
    now = dt.datetime.now()
    payload = {
        "signal_id": signal_id,
        "strategy_id": STRATEGY_ID,
        "mode": "live",
        "action": ACTION,
        "code": CODE,
        "amount": AMOUNT,
        "reference_price": REFERENCE_PRICE,
        "created_at": now.strftime("%Y-%m-%d %H:%M:%S"),
        "sent_at_ms": int(now.timestamp() * 1000),
    }

    print(
        f"目标 Redis: {TARGET} "
        f"({target['host']}:{target['port']}, stream={target['stream']})"
    )
    print(f"信号: {ACTION} {CODE} {AMOUNT}股 @ {REFERENCE_PRICE} (signal_id={signal_id})")
    if input("确认发送? 输入 yes 继续: ").strip().lower() != "yes":
        print("已取消。")
        return

    stream = RedisStreamClient(redis_config)
    message_id = stream.publish_signal(payload)
    print(f"已发送 | signal_id={signal_id} redis_message_id={message_id}")
    print("请到 Windows 端日志确认执行结果。")


if __name__ == "__main__":
    main()

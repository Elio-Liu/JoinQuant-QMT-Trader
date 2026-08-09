#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""信号发送测试脚本 —— 模拟聚宽端，往 Redis Stream 写测试信号。

用途：在接实盘之前验证"聚宽 → Redis → 两台交易机"这条链路是通的，
重点是确认**两台机器都能收到每一条信号**（而不是被消费组瓜分掉）。

刻意做成零依赖单文件：只需要 redis-py，不 import 本项目任何模块，
所以可以直接拷到任何一台机器上跑，不需要装项目、也不挑 Python 版本（3.6+）。

用法：
    # 先修改下方 REDIS_HOST / REDIS_PORT，再运行

    # 发一整轮（预订阅 + 日计划 + 卖半仓 + 清仓），最常用
    python send_test_signal.py --password YOUR_REDIS_PASSWORD

    # 只发某一类
    python send_test_signal.py --password YOUR_REDIS_PASSWORD --kind plan
    python send_test_signal.py --password YOUR_REDIS_PASSWORD --kind sell_all

    # 发真实协议之外的畸形消息，验证交易机不会被打崩
    python send_test_signal.py --password YOUR_REDIS_PASSWORD --kind malformed

    # 查看 Stream 和消费组现状（诊断"为什么只有一台收到"）
    python send_test_signal.py --password YOUR_REDIS_PASSWORD --kind inspect

注意：默认 strategy_id 用 connectivity_test，如果交易机白名单不包含它，
测试信号会被挡掉并直接 ACK。这仍能验证收信链路和白名单。改成其他
strategy_id 会进入二次确认；继续前务必先把 trading.enabled 设为 false，
否则可能在真实账户上下单。
"""

from __future__ import print_function

import argparse
import datetime as dt
import json
import sys
import time
import uuid

try:
    import redis
except ImportError:
    sys.exit("需要先安装 redis-py:  pip install redis")


# GitHub 公开版只保留占位值，请勿提交真实 IP 或密码。
# 部署时只需把 REDIS_HOST 改成 Redis 服务器地址。
REDIS_HOST = "YOUR_REDIS_SERVER_IP"
REDIS_PORT = 6380
REDIS_PASSWORD = None
REDIS_STREAM = "tidal_quant_signals"
TEST_STRATEGY_ID = "connectivity_test"


def _now_ms():
    return int(time.time() * 1000)


def _now_text():
    return dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _stamp():
    """同一次运行里所有消息共用的批次戳，便于在两台机器的输出里比对。"""
    return dt.datetime.now().strftime("%Y%m%d%H%M%S")


def build_messages(kind, strategy_id, codes, batch):
    """按类型构造要发送的 payload 列表。返回 [(说明, payload_dict), ...]"""
    sell_code = codes[0]
    buy_codes = codes[1:] if len(codes) > 1 else codes

    watchlist = {
        "action": "subscribe",
        "codes": codes,
        "strategy_id": strategy_id,
        "mode": "live",
        "sent_at_ms": _now_ms(),
    }
    plan = {
        "signal_id": "%s-%s-plan" % (strategy_id, batch),
        "strategy_id": strategy_id,
        "action": "plan",
        "codes_to_sell": [sell_code],
        "codes_to_buy": list(buy_codes),
        "created_at": _now_text(),
        "mode": "live",
        "sent_at_ms": _now_ms(),
    }
    sell_half = {
        "signal_id": "%s-%s-%s-sell_half" % (strategy_id, batch, sell_code.split(".")[0]),
        "strategy_id": strategy_id,
        "action": "sell_half",
        "code": sell_code,
        "reference_price": 10.0,
        "created_at": _now_text(),
        "mode": "live",
        "sent_at_ms": _now_ms(),
    }
    sell_all = {
        "signal_id": "%s-%s-%s-sell_all" % (strategy_id, batch, sell_code.split(".")[0]),
        "strategy_id": strategy_id,
        "action": "sell_all",
        "code": sell_code,
        "reference_price": 10.0,
        "created_at": _now_text(),
        "mode": "live",
        "sent_at_ms": _now_ms(),
    }
    # 旧协议：带精确股数的买单
    exact_buy = {
        "signal_id": "%s-%s-%s-buy-exact" % (strategy_id, batch, buy_codes[0].split(".")[0]),
        "strategy_id": strategy_id,
        "action": "buy",
        "code": buy_codes[0],
        "amount": 100,
        "reference_price": 10.0,
        "created_at": _now_text(),
        "mode": "live",
        "nonce": uuid.uuid4().hex[:8],
        "sent_at_ms": _now_ms(),
    }
    # 故意缺 code 字段：验证交易机把它记日志 + ACK 跳过，而不是崩掉
    malformed = {
        "signal_id": "%s-%s-malformed" % (strategy_id, batch),
        "strategy_id": strategy_id,
        "action": "buy",
        "amount": 100,
        "reference_price": 1.0,
    }

    table = {
        "watchlist": [("预订阅 subscribe", watchlist)],
        "plan": [("日计划 plan", plan)],
        "sell_half": [("卖半仓 sell_half", sell_half)],
        "sell_all": [("清仓 sell_all", sell_all)],
        "buy": [("旧协议精确买单 buy", exact_buy)],
        "malformed": [("畸形消息（缺 code 字段）", malformed)],
    }
    if kind == "all":
        return (
            table["watchlist"] + table["plan"] + table["sell_half"] + table["sell_all"]
        )
    return table[kind]


def inspect(client, stream):
    print("=" * 66)
    print("Stream 与消费组现状")
    print("=" * 66)
    try:
        length = client.xlen(stream)
    except Exception as exc:
        print("  ❌ 读不到 stream %s: %s" % (stream, exc))
        return 1
    print("  stream=%s  消息总数=%s" % (stream, length))

    try:
        groups = client.xinfo_groups(stream)
    except Exception as exc:
        print("  ❌ 读不到消费组: %s" % exc)
        return 1

    if not groups:
        print("  ⚠️ 还没有任何消费组 —— 交易机没连上过，或连的不是这个 stream")
        return 1

    print("  消费组数量=%d" % len(groups))
    for g in groups:
        print("    - %-24s consumers=%-3s pending=%-4s last-delivered=%s"
              % (g.get("name"), g.get("consumers"), g.get("pending"),
                 g.get("last-delivered-id")))
        try:
            for c in client.xinfo_consumers(stream, g.get("name")):
                print("        └─ consumer=%-14s pending=%-4s idle=%sms"
                      % (c.get("name"), c.get("pending"), c.get("idle")))
        except Exception:
            pass

    if len(groups) < 2:
        print()
        print("  ⚠️ 只有 %d 个消费组。两台交易机必须用**不同的 group**，" % len(groups))
        print("     否则 Redis 会把消息在两台之间瓜分，每台只收到一部分，")
        print("     而且日志里不会有任何异常。检查两份 config 的 redis.group。")
    return 0


def main():
    parser = argparse.ArgumentParser(
        description="往 Redis Stream 发测试信号，验证跟单链路",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--host", default=REDIS_HOST,
        help="Redis 服务器地址；默认读取脚本顶部 REDIS_HOST",
    )
    parser.add_argument("--port", type=int, default=REDIS_PORT)
    parser.add_argument("--password", default=REDIS_PASSWORD)
    parser.add_argument("--stream", default=REDIS_STREAM)
    parser.add_argument(
        "--kind", default="all",
        choices=["all", "watchlist", "plan", "sell_half", "sell_all", "buy",
                 "malformed", "inspect"],
        help="发送哪一类消息；inspect 只查看现状不发送（默认 all）",
    )
    parser.add_argument(
        "--strategy-id", default=TEST_STRATEGY_ID,
        help="默认 connectivity_test（通常会被实盘白名单挡掉）。"
             "改成其他值会进入二次确认。",
    )
    parser.add_argument(
        "--codes", default="000001.XSHE,600000.XSHG,000002.XSHE",
        help="测试用股票代码，逗号分隔（第一个用作卖出标的）",
    )
    parser.add_argument("--interval", type=float, default=0.5,
                        help="多条消息之间的间隔秒数")
    args = parser.parse_args()

    if not args.host or args.host == "YOUR_REDIS_SERVER_IP":
        print("❌ 请先在脚本顶部填写 REDIS_HOST，或运行时传入 --host。")
        return 2

    client = redis.Redis(
        host=args.host, port=args.port, password=args.password,
        decode_responses=True, socket_connect_timeout=5, socket_timeout=5,
    )

    try:
        client.ping()
    except Exception as exc:
        print("❌ 连不上 Redis %s:%s —— %s" % (args.host, args.port, exc))
        print("   依次检查：端口、口令、Windows 防火墙规则、云安全组。")
        return 2

    try:
        version = client.info("server").get("redis_version", "?")
    except Exception:
        version = "?"
    print("✅ 已连接 %s:%s  (Redis %s)" % (args.host, args.port, version))

    if version != "?" and int(str(version).split(".")[0]) < 5:
        print()
        print("🚨 Redis 版本低于 5.0，**没有 Stream 功能**，整套跟单跑不起来。")
        print("   XADD / XREADGROUP 都是 5.0 才有的命令。请升级。")
        return 2

    if args.kind == "inspect":
        return inspect(client, args.stream)

    if args.strategy_id != TEST_STRATEGY_ID:
        print()
        print("⚠️ 当前 strategy_id 不是默认测试值，信号可能进入执行引擎。")
        print("   请确认两台交易机的 trading.enabled 都是 false，否则会真实下单。")
        try:
            if input("   确认继续？输入 yes 回车： ").strip().lower() != "yes":
                print("已取消。")
                return 0
        except (EOFError, KeyboardInterrupt):
            print("\n已取消。")
            return 0

    batch = _stamp()
    codes = [c.strip() for c in args.codes.split(",") if c.strip()]
    messages = build_messages(args.kind, args.strategy_id, codes, batch)

    print()
    print("=" * 66)
    print("批次 %s | strategy_id=%s | 共 %d 条" % (batch, args.strategy_id, len(messages)))
    print("=" * 66)

    for label, payload in messages:
        body = json.dumps(payload, ensure_ascii=False)
        try:
            message_id = client.xadd(
                args.stream, {"payload": body}, maxlen=10000, approximate=True,
            )
        except Exception as exc:
            print("  ❌ %-22s 发送失败: %s" % (label, exc))
            if "unknown command" in str(exc).lower():
                print("     → Redis 版本不支持 Stream，需要 5.0 以上。")
            return 2
        print("  📤 %-22s id=%s" % (label, message_id))
        print("     %s" % body)
        if args.interval > 0 and label != messages[-1][0]:
            time.sleep(args.interval)

    print()
    print("发完了。现在到**两台**交易机上看 recv_test_signal.py 的输出，")
    print("确认它们**各自都收到了全部 %d 条**（批次 %s）。" % (len(messages), batch))
    print("如果只有一台收到、或两台各收到一部分 —— 说明 redis.group 配重了。")
    print()
    print("查看消费组现状：  python send_test_signal.py --host %s --port %s --kind inspect"
          % (args.host, args.port))
    return 0


if __name__ == "__main__":
    sys.exit(main())

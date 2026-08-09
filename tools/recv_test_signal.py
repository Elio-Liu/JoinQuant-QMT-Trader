#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""信号接收与解析测试脚本 —— 在每台交易机上跑，验证能否正常收信。

它复刻执行端真实的消费方式（XREADGROUP + 消费组 + ACK）和解析规则，但
**绝不下单、不碰 QMT**，纯粹把收到的东西打印出来。

刻意做成零依赖单文件：只需要 redis-py，不 import 本项目任何模块，
所以可以直接拷到一台还没装好项目的交易机上跑（Python 3.6+ 即可），
用来把"网络/端口/口令/消费组"这层问题和"项目本身"的问题分开排查。

用法（两台机器的 group 必须不同，且都不能与交易机正在用的 group 相同！）：

    # 先修改下方 REDIS_HOST / REDIS_PORT，再在交易机 A 运行
    python recv_test_signal.py --password YOUR_REDIS_PASSWORD \
        --group connectivity_machine_a_test --consumer machine-a-test

    # 交易机 B（group 和 consumer 都要与 A 不同）
    python recv_test_signal.py --password YOUR_REDIS_PASSWORD \
        --group connectivity_machine_b_test --consumer machine-b-test

用交易机的真实 group 会把消息从它嘴里抢走并 ACK 掉，交易机永远收不到，
而且它的日志里没有任何异常。脚本对不像测试组的组名会二次确认。

两台同时跑着，再从任意一台执行 send_test_signal.py，
**两边都应该看到完整的同一批消息**。只有一边看到、或各看到一部分，
就说明 group 配重了 —— 消费组是"组内分发"不是"广播"，而且这个状态存在
Redis 服务端，跟机器是不是物理独立无关。

Ctrl+C 退出时会打印本次统计。
"""

from __future__ import print_function

import argparse
import datetime as dt
import json
import signal as _signal
import sys
import time

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


_running = True


def _stop(signum, frame):
    global _running
    _running = False


def _ts():
    return dt.datetime.now().strftime("%H:%M:%S.%f")[:-3]


def parse_message(fields):
    """复刻执行端 redis_stream._parse_message 的路由与容错。

    关键点：任何解析失败都返回一个"被拒绝"的结果，**不抛异常**。
    真实执行端里这个函数驱动着整个消费循环，一条畸形消息如果抛出去，
    当天的跟单就直接收工了 —— Stream 是多策略共享的，别的策略换个 schema
    就可能产生本执行端不认识的消息。
    """
    try:
        raw_payload = fields.get("payload")
        raw = json.loads(raw_payload) if raw_payload is not None else dict(fields)
        if not isinstance(raw, dict):
            return "rejected", "payload 不是 JSON 对象", None

        action = str(raw.get("action", "")).lower()

        if action == "subscribe":
            codes = raw.get("codes", [])
            return "watchlist", "预订阅 %d 只" % len(codes), raw

        if action == "plan":
            # DailyPlan.from_dict 会强制要求这两个字段
            _ = raw["signal_id"], raw["strategy_id"]
            return ("plan",
                    "日计划 清仓%d只 待买%d只" % (len(raw.get("codes_to_sell", [])),
                                                len(raw.get("codes_to_buy", []))),
                    raw)

        # 以下复刻 TradeSignal.from_dict 的必填校验与协议映射
        reference_price = raw.get("reference_price", raw.get("price"))
        if reference_price is None:
            raise ValueError("signal requires reference_price or price")
        _ = raw["signal_id"], raw["strategy_id"], raw["code"]

        if action in ("sell_half", "sell_all"):
            direction, mode = "sell", action
        elif action == "buy" and raw.get("amount") is None:
            direction, mode = "buy", "auto_buy"
        elif action in ("buy", "sell"):
            if raw.get("amount") is None:
                raise ValueError("signal requires amount for exact quantity")
            direction, mode = action, "exact"
        else:
            raise ValueError("unknown action: %s" % action)

        qty = raw.get("amount")
        qty_text = "%s股" % qty if mode == "exact" else "(数量由执行端按账户计算)"
        return "signal", "%s %s %s %s" % (direction, raw["code"], mode, qty_text), raw
    except Exception as exc:
        return "rejected", "%s: %s" % (type(exc).__name__, exc), None


_TEST_GROUP_HINTS = ("test", "probe", "dryrun", "dry_run", "测", "试")


def _looks_like_a_test_group(group):
    lowered = str(group).lower()
    return any(hint in lowered for hint in _TEST_GROUP_HINTS)


def main():
    parser = argparse.ArgumentParser(description="消费 Redis Stream 测试信号，只打印不下单")
    parser.add_argument(
        "--host", default=REDIS_HOST,
        help="Redis 服务器地址；默认读取脚本顶部 REDIS_HOST",
    )
    parser.add_argument("--port", type=int, default=REDIS_PORT)
    parser.add_argument("--password", default=REDIS_PASSWORD)
    parser.add_argument("--stream", default=REDIS_STREAM)
    parser.add_argument("--group", required=True,
                        help="消费组名。两台交易机必须不同！")
    parser.add_argument("--consumer", required=True, help="本机消费者名")
    parser.add_argument("--block-ms", type=int, default=1000)
    parser.add_argument("--from-start", action="store_true",
                        help="新建消费组时从 Stream 最早位置开始读（默认从最新，"
                             "与执行端一致）。只在补看历史消息时用。")
    parser.add_argument("--no-ack", action="store_true",
                        help="不 ACK，消息留在 pending 里。用来观察 pending 行为。")
    parser.add_argument("--show-raw", action="store_true", help="打印完整 payload")
    parser.add_argument("--yes", action="store_true",
                        help="跳过消费组确认（非交互式运行时用）")
    args = parser.parse_args()

    if not args.host or args.host == "YOUR_REDIS_SERVER_IP":
        print("❌ 请先在脚本顶部填写 REDIS_HOST，或运行时传入 --host。")
        return 2

    if not _looks_like_a_test_group(args.group) and not args.yes:
        print()
        print("⚠️ group=%s 看着不像测试专用消费组。" % args.group)
        print("   消费组状态在 Redis 服务端：如果它和某台交易机的 group 相同，")
        print("   Redis 会把消息在你和那台机器之间瓜分，本脚本还会把抢到的那部分")
        print("   ACK 掉 —— 交易机将永远收不到那些信号，而它的日志里没有任何异常。")
        print("   测试请另起一个组名，例如 %s_test。" % args.group)
        try:
            if input("   确认继续？输入 yes 回车： ").strip().lower() != "yes":
                print("已取消。")
                return 0
        except (EOFError, KeyboardInterrupt):
            print("\n已取消。")
            return 0

    _signal.signal(_signal.SIGINT, _stop)
    _signal.signal(_signal.SIGTERM, _stop)

    client = redis.Redis(
        host=args.host, port=args.port, password=args.password,
        decode_responses=True,
        socket_connect_timeout=5,
        # 必须大于 block_ms，否则阻塞读每次都被自己的超时掐断
        socket_timeout=args.block_ms / 1000.0 + 5,
        socket_keepalive=True, health_check_interval=30, retry_on_timeout=True,
    )

    try:
        client.ping()
        version = client.info("server").get("redis_version", "?")
    except Exception as exc:
        print("❌ 连不上 Redis %s:%s —— %s" % (args.host, args.port, exc))
        print("   依次检查：端口、口令、Windows 防火墙规则、云安全组。")
        return 2

    if version != "?" and int(str(version).split(".")[0]) < 5:
        print("🚨 Redis %s 没有 Stream 功能（需要 5.0+），整套跟单跑不起来。" % version)
        return 2

    start_id = "0" if args.from_start else "$"
    try:
        client.xgroup_create(args.stream, args.group, id=start_id, mkstream=True)
        print("📡 消费组已创建 | group=%s | 起点=%s" % (args.group, start_id))
    except Exception as exc:
        if "BUSYGROUP" not in str(exc):
            print("❌ 消费组创建失败: %s" % exc)
            return 2
        print("📡 消费组已存在 | group=%s（游标从上次位置继续）" % args.group)

    print("=" * 72)
    print("Redis %s @ %s:%s | stream=%s" % (version, args.host, args.port, args.stream))
    print("group=%s | consumer=%s | ACK=%s"
          % (args.group, args.consumer, "否" if args.no_ack else "是"))
    print("=" * 72)
    print("等待信号中… (Ctrl+C 退出)")
    print()

    stats = {"signal": 0, "plan": 0, "watchlist": 0, "rejected": 0}
    started = time.time()

    while _running:
        try:
            response = client.xreadgroup(
                args.group, args.consumer, {args.stream: ">"},
                count=10, block=args.block_ms,
            )
        except Exception as exc:
            print("[%s] ⚠️ 读取异常，2 秒后重试: %s" % (_ts(), exc))
            time.sleep(2)
            continue

        if not response:
            continue

        for _stream_name, messages in response:
            for message_id, fields in messages:
                kind, summary, raw = parse_message(fields)
                stats[kind] = stats.get(kind, 0) + 1

                icon = {"signal": "📈", "plan": "📋",
                        "watchlist": "📡", "rejected": "⛔"}[kind]
                strategy = (raw or {}).get("strategy_id", "-")
                print("[%s] %s %-9s | %s" % (_ts(), icon, kind, summary))
                print("            id=%s | strategy=%s" % (message_id, strategy))

                if kind == "rejected":
                    print("            ↑ 这条消息解析不了。真实执行端会记日志后直接 ACK，")
                    print("              绝不让它卡住消费组或打崩消费循环。")

                sent_at_ms = (raw or {}).get("sent_at_ms")
                if sent_at_ms:
                    lag = time.time() * 1000 - float(sent_at_ms)
                    print("            传输延迟 %.0fms（依赖两端 NTP 同步，仅供量级参考）" % lag)

                if args.show_raw:
                    print("            payload=%s" % fields.get("payload"))

                if not args.no_ack:
                    try:
                        client.xack(args.stream, args.group, message_id)
                    except Exception as exc:
                        print("            ⚠️ ACK 失败: %s" % exc)
                print()

    total = sum(stats.values())
    print()
    print("=" * 72)
    print("本次统计 | 运行 %.0f 秒 | 共收到 %d 条" % (time.time() - started, total))
    print("  交易信号 %d | 日计划 %d | 预订阅 %d | 无法解析 %d"
          % (stats["signal"], stats["plan"], stats["watchlist"], stats["rejected"]))
    print("  group=%s consumer=%s" % (args.group, args.consumer))
    print("=" * 72)
    if total == 0:
        print()
        print("一条都没收到，按顺序排查：")
        print("  1. 发送端真的发出去了吗？在发送端跑 --kind inspect 看 stream 长度。")
        print("  2. 消费组是不是刚建的？新组从 $ 开始，收不到建组之前的消息 ——")
        print("     先起接收端、再发信号。或者加 --from-start 补看历史。")
        print("  3. stream 名两端一致吗？")
    return 0


if __name__ == "__main__":
    sys.exit(main())

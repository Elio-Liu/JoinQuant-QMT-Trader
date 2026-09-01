# -*- coding: utf-8 -*-
"""聚宽侧 Redis 信号发送函数 —— 复制进聚宽策略即可使用。

本文件是「聚宽 → Redis Stream → Windows QMT 执行端」链路的聚宽侧发送端。
不需要 import：直接把本文件完整内容粘贴到聚宽策略代码中（或只粘贴用到的
函数），按下方「部署前配置」修改后即可。

────────────────────────────────────────────────────────────────────────
一、部署前配置
────────────────────────────────────────────────────────────────────────
1. 修改 SIGNAL_REDIS_CONFIG：填真实的 Redis 地址和密码；stream 必须与
   Windows 端 config.yaml 的 redis.stream 完全一致。
2. SIGNAL_STRATEGY_ID：本策略的策略标识。Windows 端
   redis.allowed_strategy_ids 白名单非空时，只执行名单内的 strategy_id。
3. 不要把带真实 Redis 地址/密码的部署副本提交回仓库。

────────────────────────────────────────────────────────────────────────
二、在聚宽策略里的典型用法
────────────────────────────────────────────────────────────────────────
选股完成后推送股票池（只订阅行情、不产生订单）：

    publish_watchlist_to_redis(context, selected_codes)

QMT 本地策略引擎模式只推送当日有序候选结果：

    publish_candidate_plan_to_redis(context, selected_codes, "local-engine-v1")

需要清仓时，对清单逐只发 sell_all 意图信号：

    def auction_stop_loss(context):
        for stock in context.get_open_clear_codes():   # 示例，按实际逻辑取
            publish_sell_all_to_redis(context, stock, get_current_data()[stock].last_price)

发送日计划（清仓清单 + 待买清单，执行端先卖后按真实资金买入）：

    publish_daily_plan_to_redis(context, codes_to_sell, codes_to_buy)

精确买卖（旧协议，五参数签名固定，数量由策略指定）：

    publish_trade_signal_to_redis(context, "buy", "510300.XSHG", 1000, 3.850)
    publish_trade_signal_to_redis(context, "sell", "159915.XSHE", 500, 1.235)

盘中意图信号（数量由执行端按真实持仓/资金计算）：

    publish_sell_half_to_redis(context, "000001.XSHE", 10.5)   # 卖出半仓
    publish_sell_all_to_redis(context, "000002.XSHE", 9.8)     # 清仓

────────────────────────────────────────────────────────────────────────
三、重要语义
────────────────────────────────────────────────────────────────────────
- 回测、研究、历史补跑不会写 Redis：按 context.current_dt 与系统时间差判断，
  时间差超过 SIGNAL_MAX_LIVE_LAG_SECONDS（默认 600 秒）视为非实时。
- XADD 成功只表示 Redis 已收到信号，不代表 Windows 端已经下单或成交。
- signal_id 是执行端幂等键：同一秒、同一策略、同一代码、同一方向、同一数量
  会生成相同 id，重复投递会被执行端去重（同一秒两笔独立订单需自行改 id）。
- 信号过期由执行端配置 execution.signal_expire_seconds 决定：执行端按
  sent_at_ms（发送时刻毫秒时间戳）+ 配置秒数判断，到点仍未开始执行就
  直接终态并 ACK，不下单。发送端只附带发送时刻，不写死过期时间。
- execute_at 已废弃：执行端收到未过期信号后立即执行，不再等待预约时间。
- reference_price 只作审计/日志参考，执行端始终按实时行情重新定价。
- QMT 本地策略引擎的专用账户只接受 candidate_plan；同策略旧的普通交易和
  plan 消息会被交易端 ACK 拒绝。不要在同一策略里同时发送两套协议。
"""

import datetime as dt
import json
import re
import time
import uuid


# ---------------------------------------------------------------------------
# 配置常量
# ---------------------------------------------------------------------------
SIGNAL_REDIS_CONFIG = {
    "host": "YOUR_REDIS_HOST",   # 部署前替换；生产地址不要提交到仓库
    "port": 6379,
    "password": None,            # 无密码可留 None；有密码填字符串
    "stream": "jq_qmt_signals",  # 必须与 Windows 端 config.yaml 一致
    "maxlen": 10000,
    "socket_connect_timeout": 1,
    "socket_timeout": 2,         # 读写超时: 半死连接最多挂 2 秒, 不会卡死整个策略
}
SIGNAL_STRATEGY_ID = "YOUR_STRATEGY_ID"  # 与 Windows 端白名单 allowed_strategy_ids 对应
SIGNAL_MAX_LIVE_LAG_SECONDS = 600    # context 时间与系统时间差超过它视为回测
# 发送失败重试: signal_id 不变, 重复消息由执行端幂等去重, 重试不会重复下单。
SIGNAL_XADD_MAX_RETRIES = 3
SIGNAL_XADD_RETRY_DELAY_SEC = 0.5


# ---------------------------------------------------------------------------
# 内部工具函数
# ---------------------------------------------------------------------------
def _log(message):
    """聚宽平台提供全局 log 对象；本机直接运行/测试时回退到 print。"""
    try:
        log.info(message)
    except NameError:
        print(message)


def _signal_mode(context):
    """区分实盘/回测：context 时间明显早于系统时间时视为回测。"""
    context_time = context.current_dt
    if getattr(context_time, "tzinfo", None) is not None:
        context_time = context_time.replace(tzinfo=None)
    time_diff = (dt.datetime.now() - context_time).total_seconds()
    return ("backtest" if time_diff > SIGNAL_MAX_LIVE_LAG_SECONDS else "live"), context_time


def _cached_redis_client():
    """按 SIGNAL_REDIS_CONFIG 缓存 Redis 连接，所有发送函数共用一条。"""
    import redis

    holder = publish_trade_signal_to_redis
    config_key = (
        SIGNAL_REDIS_CONFIG["host"],
        SIGNAL_REDIS_CONFIG["port"],
        SIGNAL_REDIS_CONFIG["password"],
        SIGNAL_REDIS_CONFIG["stream"],
    )
    if (
        not hasattr(holder, "_redis_client")
        or getattr(holder, "_redis_config_key", None) != config_key
    ):
        holder._redis_client = redis.Redis(
            host=SIGNAL_REDIS_CONFIG["host"],
            port=SIGNAL_REDIS_CONFIG["port"],
            password=SIGNAL_REDIS_CONFIG["password"],
            decode_responses=True,
            socket_connect_timeout=SIGNAL_REDIS_CONFIG["socket_connect_timeout"],
            socket_timeout=SIGNAL_REDIS_CONFIG["socket_timeout"],
        )
        holder._redis_config_key = config_key
    return holder._redis_client


def _xadd(payload):
    """写入 Stream; 失败自动重试, 全部失败才抛给上层记日志。"""
    client = _cached_redis_client()
    last_error = None
    for attempt in range(1, SIGNAL_XADD_MAX_RETRIES + 1):
        try:
            return client.xadd(
                SIGNAL_REDIS_CONFIG["stream"],
                {"payload": json.dumps(payload, ensure_ascii=False)},
                maxlen=SIGNAL_REDIS_CONFIG["maxlen"],
                approximate=True,
            )
        except Exception as exc:
            last_error = exc
            _log("[信号] Redis写入失败 第{}次重试: {}".format(attempt, exc))
            if attempt < SIGNAL_XADD_MAX_RETRIES:
                time.sleep(SIGNAL_XADD_RETRY_DELAY_SEC)
    raise last_error


# ---------------------------------------------------------------------------
# 公开发送函数
# ---------------------------------------------------------------------------
def publish_trade_signal_to_redis(context, action, code, amount, price):
    """精确买卖信号：五参数签名固定，旧策略调用点无需改动。

    action 为 "buy"/"sell"，amount 为目标股数（正数，执行端仍会按最新资金/持仓
    缩量），price 仅作审计参考、执行端按实时行情重新定价；非实时信号只返回
    sent=False 不写 Redis。返回 dict 含 sent/mode/signal/redis_message_id。
    """
    try:
        action = str(action).lower()
        if action not in ("buy", "sell"):
            raise ValueError("action must be 'buy' or 'sell'")
        amount = int(amount)
        if amount <= 0:
            raise ValueError("amount must be positive")

        mode, context_time = _signal_mode(context)
        safe_code = re.sub(r"[^0-9A-Za-z]", "", str(code))
        now = dt.datetime.now()
        signal = {
            "signal_id": "{}-{}-{}-{}-{}".format(
                SIGNAL_STRATEGY_ID,
                context_time.strftime("%Y%m%d%H%M%S"),
                safe_code,
                action,
                amount,
            ),
            "strategy_id": SIGNAL_STRATEGY_ID,
            "mode": mode,
            "action": action,
            "code": str(code),
            "amount": amount,
            "reference_price": float(price),
            "created_at": context_time.strftime("%Y-%m-%d %H:%M:%S"),
            "nonce": uuid.uuid4().hex[:8],
            "sent_at_ms": int(now.timestamp() * 1000),
        }
        if mode != "live":
            _log(
                "[信号] 跳过非实时信号: {} {} {}股 @ {}".format(
                    action, code, amount, price
                )
            )
            return {
                "sent": False,
                "mode": mode,
                "signal": signal,
                "redis_message_id": None,
            }

        redis_message_id = _xadd(signal)
        _log(
            "[信号] 已写入Redis Stream: id={} {} {} {}股 @ {}".format(
                redis_message_id, action, code, amount, price
            )
        )
        return {
            "sent": True,
            "mode": mode,
            "signal": signal,
            "redis_message_id": redis_message_id,
        }
    except Exception as exc:
        _log("[信号] Redis Stream发送失败: {}".format(exc))
        return {
            "sent": False,
            "mode": None,
            "signal": None,
            "redis_message_id": None,
            "error": str(exc),
        }


def publish_watchlist_to_redis(context, codes):
    """盘前预订阅：把当日股票池推给执行端只订阅行情，不触发交易。"""
    try:
        codes = [str(item) for item in codes if item]
        if not codes:
            return {"sent": False, "mode": None, "redis_message_id": None}

        mode, _ = _signal_mode(context)
        if mode != "live":
            return {"sent": False, "mode": mode, "redis_message_id": None}

        payload = {
            "action": "subscribe",
            "codes": codes,
            "strategy_id": SIGNAL_STRATEGY_ID,
            "mode": mode,
            "sent_at_ms": int(dt.datetime.now().timestamp() * 1000),
        }
        redis_message_id = _xadd(payload)
        _log(
            "[信号] 预订阅列表已推送: {}只 {}".format(
                len(codes), ",".join(codes)
            )
        )
        return {"sent": True, "mode": mode, "redis_message_id": redis_message_id}
    except Exception as exc:
        _log("[信号] 预订阅列表推送失败: {}".format(exc))
        return {
            "sent": False,
            "mode": None,
            "redis_message_id": None,
            "error": str(exc),
        }


def publish_candidate_plan_to_redis(context, candidates, strategy_version=""):
    """推送 QMT 本地策略引擎所需的唯一日输入：有序候选股票列表。

    空列表也会发送，明确表示当天不买。资金、止损、卖出时点与订单数量均由
    Windows 交易机的私有 config.strategy.yaml 决定，不进入 Redis 消息。
    """
    try:
        candidates = [str(code) for code in (candidates or []) if code]
        if len(set(candidates)) != len(candidates):
            raise ValueError("candidates 包含重复代码")
        code_pattern = re.compile(r"^\d{6}\.(XSHG|XSHE)$")
        for code in candidates:
            if not code_pattern.match(code):
                raise ValueError("非法聚宽证券代码: {}".format(code))

        mode, context_time = _signal_mode(context)
        if mode != "live":
            return {"sent": False, "mode": mode, "redis_message_id": None}

        now = dt.datetime.now()
        payload = {
            "action": "candidate_plan",
            "schema_version": 1,
            "plan_id": "{}-{}-candidates".format(
                SIGNAL_STRATEGY_ID, context_time.strftime("%Y%m%d")
            ),
            "strategy_id": SIGNAL_STRATEGY_ID,
            "mode": mode,
            "trading_date": context_time.strftime("%Y-%m-%d"),
            "candidates": candidates,
            "strategy_version": str(strategy_version or ""),
            "created_at": context_time.strftime("%Y-%m-%d %H:%M:%S"),
            "sent_at_ms": int(now.timestamp() * 1000),
        }
        redis_message_id = _xadd(payload)
        _log(
            "[信号] 候选计划已推送: {}只 {}".format(
                len(candidates), ",".join(candidates)
            )
        )
        return {
            "sent": True,
            "mode": mode,
            "candidate_plan": payload,
            "redis_message_id": redis_message_id,
        }
    except Exception as exc:
        _log("[信号] 候选计划推送失败: {}".format(exc))
        return {
            "sent": False,
            "mode": None,
            "candidate_plan": None,
            "redis_message_id": None,
            "error": str(exc),
        }


def publish_daily_plan_to_redis(context, codes_to_sell, codes_to_buy):
    """日计划意图信号：开盘清仓清单 + 今日待买清单。

    执行端在计划时刻先对 codes_to_sell 全部清仓（sell_all），再按真实可用
    资金对 codes_to_buy 等分买入（auto_buy），已持仓代码自动跳过。
    signal_id 按日期生成，同一日重复发送会被执行端幂等处理。

    调用时机由使用者自己的策略决定。
    """
    try:
        codes_to_sell = [str(code) for code in codes_to_sell or [] if code]
        codes_to_buy = [str(code) for code in codes_to_buy or [] if code]
        if not codes_to_sell and not codes_to_buy:
            return {"sent": False, "mode": None, "redis_message_id": None}

        mode, context_time = _signal_mode(context)
        payload = {
            "signal_id": "{}-{}-plan".format(
                SIGNAL_STRATEGY_ID, context_time.strftime("%Y%m%d")
            ),
            "strategy_id": SIGNAL_STRATEGY_ID,
            "mode": mode,
            "action": "plan",
            "codes_to_sell": codes_to_sell,
            "codes_to_buy": codes_to_buy,
            "created_at": context_time.strftime("%Y-%m-%d %H:%M:%S"),
            "sent_at_ms": int(dt.datetime.now().timestamp() * 1000),
        }
        if mode != "live":
            return {"sent": False, "mode": mode, "redis_message_id": None}

        redis_message_id = _xadd(payload)
        _log(
            "[信号] 日计划已推送: 清仓{}只 待买{}只".format(
                len(codes_to_sell), len(codes_to_buy)
            )
        )
        return {"sent": True, "mode": mode, "redis_message_id": redis_message_id}
    except Exception as exc:
        _log("[信号] 日计划推送失败: {}".format(exc))
        return {
            "sent": False,
            "mode": None,
            "redis_message_id": None,
            "error": str(exc),
        }


def publish_sell_half_to_redis(context, code, price):
    """卖出半仓意图信号：数量由执行端按真实可卖持仓计算，策略不用算。"""
    return _publish_intent(context, "sell_half", code, price)


def publish_sell_all_to_redis(context, code, price):
    """清仓意图信号：数量由执行端按真实可卖持仓计算，策略不用算。"""
    return _publish_intent(context, "sell_all", code, price)


def _publish_intent(context, action, code, price):
    """sell_half/sell_all 共用的意图信号构造：signal_id 不含数量，数量由执行端按真实持仓解析。"""
    try:
        mode, context_time = _signal_mode(context)
        safe_code = re.sub(r"[^0-9A-Za-z]", "", str(code))
        payload = {
            "signal_id": "{}-{}-{}-{}".format(
                SIGNAL_STRATEGY_ID,
                context_time.strftime("%Y%m%d%H%M%S"),
                safe_code,
                action,
            ),
            "strategy_id": SIGNAL_STRATEGY_ID,
            "mode": mode,
            "action": action,
            "code": str(code),
            "reference_price": float(price),
            "created_at": context_time.strftime("%Y-%m-%d %H:%M:%S"),
            "sent_at_ms": int(dt.datetime.now().timestamp() * 1000),
        }
        if mode != "live":
            return {"sent": False, "mode": mode, "redis_message_id": None}

        redis_message_id = _xadd(payload)
        _log("[信号] {}已推送: {} @ {}".format(action, code, price))
        return {"sent": True, "mode": mode, "redis_message_id": redis_message_id}
    except Exception as exc:
        _log("[信号] {}推送失败: {}".format(action, exc))
        return {
            "sent": False,
            "mode": None,
            "redis_message_id": None,
            "error": str(exc),
        }

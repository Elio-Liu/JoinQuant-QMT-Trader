import datetime as dt
import json
import re
import uuid


def publish_trade_signal_to_redis(context, action, code, amount, price):
    """将交易信号写入 Redis Stream，供 Windows miniQMT 执行端消费。

    保持原始五参数签名，方便直接嵌入聚宽策略。XADD 成功只表示 Redis 已收到
    信号，不代表 Windows 端已经下单或成交。
    """
    # 部署到聚宽前替换占位符。不要把生产地址或密码提交到仓库。
    redis_config = {
        "host": "YOUR_REDIS_HOST",
        "port": 6379,
        "password": None,
        "stream": "tidal_quant_signals",
        "maxlen": 10000,
        "socket_connect_timeout": 1,
    }
    strategy_id = "hunter"
    expire_seconds = 20
    max_live_lag_seconds = 600

    try:
        action = str(action).lower()
        if action not in ("buy", "sell"):
            raise ValueError("action must be 'buy' or 'sell'")

        amount = int(amount)
        if amount <= 0:
            raise ValueError("amount must be positive")

        context_time = context.current_dt
        current_time = dt.datetime.now()
        if getattr(context_time, "tzinfo", None) is not None:
            context_time = context_time.replace(tzinfo=None)

        time_diff = (current_time - context_time).total_seconds()
        mode = "backtest" if time_diff > max_live_lag_seconds else "live"
        safe_code = re.sub(r"[^0-9A-Za-z]", "", str(code))
        signal_id = "{}-{}-{}-{}-{}".format(
            strategy_id,
            context_time.strftime("%Y%m%d%H%M%S"),
            safe_code,
            action,
            amount,
        )
        signal = {
            "signal_id": signal_id,
            "strategy_id": strategy_id,
            "mode": mode,
            "action": action,
            "code": str(code),
            "amount": amount,
            "reference_price": float(price),
            "created_at": context_time.strftime("%Y-%m-%d %H:%M:%S"),
            "expire_at": (current_time + dt.timedelta(seconds=expire_seconds)).strftime(
                "%Y-%m-%d %H:%M:%S"
            ),
            "nonce": uuid.uuid4().hex[:8],
            "sent_at_ms": int(dt.datetime.now().timestamp() * 1000),
        }

        if mode != "live":
            try:
                log.info("[信号] 跳过非实时信号: {} {} {}股 @ {}".format(action, code, amount, price))
            except NameError:
                print("[信号] 跳过非实时信号: {} {} {}股 @ {}".format(action, code, amount, price))
            return {"sent": False, "mode": mode, "signal": signal, "redis_message_id": None}

        client = _cached_redis_client(redis_config)
        redis_message_id = client.xadd(
            redis_config["stream"],
            {"payload": json.dumps(signal, ensure_ascii=False)},
            maxlen=redis_config["maxlen"],
            approximate=True,
        )
        try:
            log.info(
                "[信号] 已写入Redis Stream: id={} {} {} {}股 @ {}".format(
                    redis_message_id, action, code, amount, price
                )
            )
        except NameError:
            print(
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
        try:
            log.error("[信号] Redis Stream发送失败: {}".format(exc))
        except NameError:
            print("[信号] Redis Stream发送失败: {}".format(exc))
        return {
            "sent": False,
            "mode": None,
            "signal": None,
            "redis_message_id": None,
            "error": str(exc),
        }


def publish_watchlist_to_redis(context, codes):
    """把当日股票池推送给 Windows 端预订阅行情，不触发交易。"""
    # 必须与 publish_trade_signal_to_redis 的部署配置保持一致。
    redis_config = {
        "host": "YOUR_REDIS_HOST",
        "port": 6379,
        "password": None,
        "stream": "tidal_quant_signals",
        "maxlen": 10000,
        "socket_connect_timeout": 1,
    }
    strategy_id = "hunter"
    max_live_lag_seconds = 600

    try:
        codes = [str(item) for item in codes if item]
        if not codes:
            return {"sent": False, "mode": None, "redis_message_id": None}

        context_time = context.current_dt
        if getattr(context_time, "tzinfo", None) is not None:
            context_time = context_time.replace(tzinfo=None)
        time_diff = (dt.datetime.now() - context_time).total_seconds()
        mode = "backtest" if time_diff > max_live_lag_seconds else "live"
        if mode != "live":
            return {"sent": False, "mode": mode, "redis_message_id": None}

        payload = {
            "action": "subscribe",
            "codes": codes,
            "strategy_id": strategy_id,
            "mode": mode,
            "sent_at_ms": int(dt.datetime.now().timestamp() * 1000),
        }
        client = _cached_redis_client(redis_config)
        redis_message_id = client.xadd(
            redis_config["stream"],
            {"payload": json.dumps(payload, ensure_ascii=False)},
            maxlen=redis_config["maxlen"],
            approximate=True,
        )
        try:
            log.info("[信号] 预订阅列表已推送: {}只 {}".format(len(codes), ",".join(codes)))
        except NameError:
            print("[信号] 预订阅列表已推送: {}只 {}".format(len(codes), ",".join(codes)))
        return {"sent": True, "mode": mode, "redis_message_id": redis_message_id}
    except Exception as exc:
        try:
            log.error("[信号] 预订阅列表推送失败: {}".format(exc))
        except NameError:
            print("[信号] 预订阅列表推送失败: {}".format(exc))
        return {
            "sent": False,
            "mode": None,
            "redis_message_id": None,
            "error": str(exc),
        }


def _cached_redis_client(redis_config):
    """按配置缓存 Redis 连接，交易信号和预订阅共用一条连接。"""
    import redis

    holder = publish_trade_signal_to_redis
    config_key = (
        redis_config["host"],
        redis_config["port"],
        redis_config["password"],
        redis_config["stream"],
    )
    if (
        not hasattr(holder, "_redis_client")
        or getattr(holder, "_redis_config_key", None) != config_key
    ):
        holder._redis_client = redis.Redis(
            host=redis_config["host"],
            port=redis_config["port"],
            password=redis_config["password"],
            decode_responses=True,
            socket_connect_timeout=redis_config["socket_connect_timeout"],
        )
        holder._redis_config_key = config_key
    return holder._redis_client

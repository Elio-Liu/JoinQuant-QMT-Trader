import datetime as dt
import json
import re
import uuid


def publish_trade_signal_to_redis(context, action, code, amount, price):
    """
    将买卖交易信号通过 Redis Stream 发布, 用于 Windows 端 miniQMT 实盘跟单。

    这个函数刻意保持原始五参数签名, 方便直接替换旧的 publish 版本。
    XADD 成功只代表 Redis 已经收到信号, 不代表 Windows 端已下单或成交。

    Args:
        context: 聚宽上下文对象
        action: 交易动作 ("buy" 或 "sell")
        code: 股票代码
        amount: 交易数量
        price: 策略参考价格, Windows 端会用 miniQMT 最新行情重新按滑点定价

    Returns:
        dict: 发送结果。原策略可忽略返回值。
    """
    # Redis 配置: 只需要在这里改, 不需要改策略里的函数调用位置。
    # 实盘建议不要把 Redis 暴露在公网 6379 端口, 至少使用安全组白名单或 VPN。
    redis_config = {
        "host": "47.102.126.24",
        "port": 6379,
        "password": None,
        "stream": "tidal_quant_signals",
        "maxlen": 10000,
        "socket_connect_timeout": 1,
    }
    # strategy_id 会进入 signal_id, Windows 端用它区分不同策略来源。
    strategy_id = "hunter"
    # expire_at 是给执行端/监控端看的过期时间, 当前聚宽侧只负责写入信号。
    expire_seconds = 20
    # 聚宽回测或补跑时 context.current_dt 会明显早于系统时间, 这里避免旧信号进入实盘。
    max_live_lag_seconds = 600

    try:
        # 输入先标准化, 避免 "BUY"/"Sell" 这类大小写差异影响执行端解析。
        action = str(action).lower()
        if action not in ("buy", "sell"):
            raise ValueError("action must be 'buy' or 'sell'")

        amount = int(amount)
        if amount <= 0:
            raise ValueError("amount must be positive")

        context_time = context.current_dt
        current_time = dt.datetime.now()
        # 聚宽时间通常是 naive datetime；若外部测试传入带时区时间, 先去掉时区避免相减报错。
        if getattr(context_time, "tzinfo", None) is not None:
            context_time = context_time.replace(tzinfo=None)

        time_diff = (current_time - context_time).total_seconds()
        mode = "backtest" if time_diff > max_live_lag_seconds else "live"

        # signal_id 是实盘幂等关键字段: 同一个 signal_id 在 Windows 端只会执行一次。
        # 默认由策略、聚宽时间、股票、方向、数量组成, 适合常规“一次触发一笔订单”的策略。
        safe_code = re.sub(r"[^0-9A-Za-z]", "", str(code))
        signal_id = "{}-{}-{}-{}-{}".format(
            strategy_id,
            context_time.strftime("%Y%m%d%H%M%S"),
            safe_code,
            action,
            amount,
        )
        # reference_price 是策略参考价, 不是最终委托价。
        # Windows 端会用 miniQMT/xtquant 最新行情按配置滑点重新计算委托价。
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
            # nonce 只用于审计排查, 不参与幂等。幂等只看 signal_id。
            "nonce": uuid.uuid4().hex[:8],
        }

        # 非 live 信号直接丢弃, 防止回测、研究环境、补跑脚本误触发实盘。
        if mode != "live":
            try:
                log.info("[信号] 跳过非实时信号: {} {} {}股 @ {}".format(action, code, amount, price))
            except NameError:
                print("[信号] 跳过非实时信号: {} {} {}股 @ {}".format(action, code, amount, price))
            return {"sent": False, "mode": mode, "signal": signal, "redis_message_id": None}

        import redis

        # 开盘抢单时连接建立是明显延迟来源, 因此把 Redis 客户端缓存在函数属性里。
        # 这样仍然只暴露一个函数, 但同一轮策略运行中后续信号会复用同一条连接。
        redis_config_key = (
            redis_config["host"],
            redis_config["port"],
            redis_config["password"],
            redis_config["stream"],
        )
        if (
            not hasattr(publish_trade_signal_to_redis, "_redis_client")
            or getattr(publish_trade_signal_to_redis, "_redis_config_key", None) != redis_config_key
        ):
            publish_trade_signal_to_redis._redis_client = redis.Redis(
                host=redis_config["host"],
                port=redis_config["port"],
                password=redis_config["password"],
                decode_responses=True,
                socket_connect_timeout=redis_config["socket_connect_timeout"],
            )
            publish_trade_signal_to_redis._redis_config_key = redis_config_key
        r = publish_trade_signal_to_redis._redis_client
        # 使用 Redis Stream 而不是 publish:
        # - Stream 会保存消息, Windows 程序短暂离线后可继续消费
        # - 消费端可以 XACK, 方便追踪是否处理完成
        # - maxlen 使用近似裁剪, 防止 Redis 长期积累过多历史信号
        redis_message_id = r.xadd(
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

    except Exception as e:
        # 聚宽侧发送失败只记录错误, 不在这里重试, 避免阻塞策略主流程。
        try:
            log.error("[信号] Redis Stream发送失败: {}".format(e))
        except NameError:
            print("[信号] Redis Stream发送失败: {}".format(e))
        return {"sent": False, "mode": None, "signal": None, "redis_message_id": None, "error": str(e)}

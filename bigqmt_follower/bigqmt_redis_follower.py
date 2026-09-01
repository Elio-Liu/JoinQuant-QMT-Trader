# -*- coding: utf-8 -*-
"""大 QMT 内置策略：消费 Redis Stream 并执行股票交易信号。

与 miniQMT 版 miniqmt_follower 协议对齐：解析 trade/watchlist/plan 消息，按买卖
方向 FIFO 执行真实委托，靠 signal_id 幂等去重并只在信号终态后 ACK。竞价排队
报价(统一 quote_band_pct 包络 + 价格笼子贴边回退)、涨跌停锁盘排队、开盘卖单
屏障、开盘首挂耐心、撤单确认 60s 熔断等规则均与 miniQMT 版同义。

本模块约定:
- 注释与日志为简体中文、emoji 风格；日志字符串一字不改。
- 委托经 BigQmtGateway 收口大 QMT 全局函数，运行时状态由 BigQmtRuntime 维护。
"""

from __future__ import print_function

import json
import hashlib
import datetime
import re
import threading
import time
import uuid
from collections import deque
from decimal import Decimal, ROUND_HALF_UP

try:
    import queue as queue_module
except ImportError:  # pragma: no cover - 兼容少量仍使用 Python 2 的旧 QMT 环境
    import Queue as queue_module


# ---------------------------------------------------------------------------
# 配置与时段常量
# ---------------------------------------------------------------------------
CONFIG = {
    "trading_enabled": False,
    "account_id": "",
    "account_type": "stock",
    "strategy_name": "bigqmt_redis_follower",
    "redis_host": "",
    "redis_port": 6379,
    "redis_password": "",
    "redis_stream": "jq_qmt_signals",
    "redis_group": "bigqmt_executors",
    "redis_block_ms": 500,
    "redis_reconnect_sec": 3.0,
    "pricing_mode": "book",
    # 统一挂单包络(与 miniQMT 版 quote_band_pct 同义): 竞价排队与滑点回退共用,
    # 买卖单都以最新价×(1±该比例)挂出, 最大化一次挂单成交率; 0 关闭。
    "quote_band_pct": 0.015,
    "book_tick_offset": 2,
    "order_timeout_sec": 3.0,
    # 开盘首挂耐心(与 miniQMT 版 opening_order_timeout_sec 同义): 开盘窗口
    # 60s 内的买单首笔按该秒数等待成交; 盘前挂单的买单为 距开盘+该秒数。
    "opening_order_timeout_sec": 2.0,
    "order_visibility_timeout_sec": 3.0,
    # 撤单请求发出后等待终态回报的时长, 从"发出撤单那一刻"独立计时,
    # 不与 max_total_duration_sec 共用预算 —— 超过它才熔断。
    # 与 miniQMT 版一致(2026-08-19 事件后放宽): 开盘撮合期撤单回执实测可超 30s。
    "cancel_confirm_timeout_sec": 60.0,
    "max_attempts": 3,
    "max_total_duration_sec": 15.0,
    # 涨跌停锁盘处理, 与 miniQMT 版同义:
    #   limit_down_sell_mode: ""=旧开关推导, skip=直接跳过, queue=挂跌停价排队至截止
    #   limit_up_buy_mode:    ""=旧开关推导, skip=直接跳过, queue=挂涨停价排队至截止
    "limit_down_sell_mode": "",
    "skip_sell_when_limit_down": False,
    "limit_up_buy_mode": "",
    "skip_buy_when_limit_up": False,
    "queue_sell_deadline": "14:56:30",
    "queue_buy_deadline": "14:56:30",
    # 与 miniQMT 版同义: 策略白名单(空=不过滤) 与 意图信号单票买入上限。
    "allowed_strategy_ids": [],
    "max_single_position_pct": 0.2,
    # sell_half 半仓取整不足一手(<200股)时的处理: sell_all=全卖(默认) / skip=跳过不卖。
    "sell_half_insufficient_lot_mode": "sell_all",
    # 信号过期秒数: 按 sent_at_ms(发送时刻毫秒) + 该值判断; 0=不过期。
    # 旧协议 expire_at 绝对时间字段仍优先兼容。
    "signal_expire_seconds": 600,
}

# 竞价排队时段: 9:15~9:30 提交的委托都要等 9:30 连续竞价才可能成交。
_AUCTION_START = datetime.time(9, 15)
_MARKET_OPEN = datetime.time(9, 30)
_PREOPEN_SELL_START = datetime.time(9, 25)
# 盘前卖单首笔: 开盘后只给 0.5s 回报宽限再进入撤单流程 (与 miniQMT 一致)。
_OPENING_SELL_RECONCILE_GRACE_SEC = 0.5
# 开盘首挂窗口(与 miniQMT 版 opening_aggressive_window_sec 同义): 连续竞价开始
# 后的前 60s 内, 买单首笔用 opening_order_timeout_sec 作为耐心等待成交。
_OPENING_WINDOW_SEC = 60.0


def _in_opening_window():
    """当前是否处于开盘首挂窗口(连续竞价开始后 60s)。测试可替换本模块级函数。"""
    now = datetime.datetime.now()
    market_open = now.replace(hour=9, minute=30, second=0, microsecond=0)
    return 0 <= (now - market_open).total_seconds() <= _OPENING_WINDOW_SEC


# ---------------------------------------------------------------------------
# 通用工具函数
# ---------------------------------------------------------------------------
def _in_call_auction():
    """当前是否处于 9:15~9:30 竞价排队时段。测试可替换本模块级函数。"""
    return _AUCTION_START <= datetime.datetime.now().time() < _MARKET_OPEN


def _in_preopen_window():
    """当前是否处于 9:25~9:30 盘前窗口。测试可替换本模块级函数。"""
    return _PREOPEN_SELL_START <= datetime.datetime.now().time() < _MARKET_OPEN


def is_preopen_sell(signal):
    """按信号创建时间判定是否为 9:25~9:30 的盘前卖单 (与 miniQMT opening 同规则)。"""
    if signal.get("action") != "sell":
        return False
    try:
        created = datetime.datetime.strptime(signal["created_at"], "%Y-%m-%d %H:%M:%S")
    except (TypeError, ValueError):
        return False
    return _PREOPEN_SELL_START <= created.time() < _MARKET_OPEN


def effective_limit_down_sell_mode(config):
    """归一化跌停卖出模式; 缺省时由旧开关 skip_sell_when_limit_down 推导。"""
    mode = config.get("limit_down_sell_mode") or ""
    if mode:
        return mode
    return "skip" if config.get("skip_sell_when_limit_down") else "none"


def effective_limit_up_buy_mode(config):
    """归一化涨停买入模式; 缺省时由旧开关 skip_buy_when_limit_up 推导。"""
    mode = config.get("limit_up_buy_mode") or ""
    if mode:
        return mode
    return "skip" if config.get("skip_buy_when_limit_up") else "none"


def _is_confirmed_limit_up(tick, high_limit, code):
    """仅在涨停价可用且最新价/买一已触及涨停时确认锁盘 (与 miniQMT 同规则)。"""
    if high_limit is None or high_limit <= 0:
        return False
    if _first_positive(tick.get("askPrice")) is not None:
        return False
    tolerance = tick_size_for(code) + 1e-9
    last = float(tick.get("lastPrice") or 0)
    bid1 = _first_positive(tick.get("bidPrice"))
    return any(
        price is not None and abs(price - high_limit) <= tolerance
        for price in (last, bid1)
    )


def parse_stream_message(message_id, fields):
    """解析 Redis Stream entry：trade / watchlist / plan 消息。

    与 miniQMT 版协议对齐: plan / sell_half / sell_all / 不带 amount 的 buy
    (auto_buy) 都是意图型消息, 数量由执行端按真实账户计算。
    """
    raw = json.loads(fields["payload"]) if "payload" in fields else dict(fields)
    action = str(raw.get("action", "")).lower()
    if action == "subscribe":
        codes = raw.get("codes", [])
        if not isinstance(codes, (list, tuple)):
            raise ValueError("watchlist codes must be a list")
        return {
            "kind": "watchlist",
            "message_id": str(message_id),
            "strategy_id": str(raw.get("strategy_id", "")),
            "codes": [str(code) for code in codes],
        }

    required = ("signal_id", "strategy_id", "action")
    missing = [name for name in required if raw.get(name) in (None, "")]
    if missing:
        raise ValueError("trade signal missing fields: %s" % ", ".join(missing))

    if action == "plan":
        codes_to_sell = [str(code) for code in raw.get("codes_to_sell", []) or []]
        codes_to_buy = [str(code) for code in raw.get("codes_to_buy", []) or []]
        if not codes_to_sell and not codes_to_buy:
            raise ValueError("plan requires codes_to_sell or codes_to_buy")
        plan = {
            "signal_id": str(raw["signal_id"]),
            "strategy_id": str(raw["strategy_id"]),
            "mode": str(raw.get("mode", "live")).lower(),
            "codes_to_sell": codes_to_sell,
            "codes_to_buy": codes_to_buy,
            "created_at": str(raw.get("created_at", "")),
            "sent_at_ms": raw.get("sent_at_ms"),
        }
        return {"kind": "plan", "message_id": str(message_id), "plan": plan}

    if raw.get("code") in (None, ""):
        raise ValueError("trade signal missing code")

    quantity_mode = str(raw.get("quantity_mode") or "").lower()
    if quantity_mode not in ("", "exact", "auto_buy", "sell_half", "sell_all"):
        raise ValueError("invalid quantity_mode: %s" % quantity_mode)
    if action in ("sell_half", "sell_all"):
        trade_action = "sell"
        quantity_mode = action
    elif action == "buy" and raw.get("amount") is None:
        trade_action = "buy"
        quantity_mode = "auto_buy"
    elif action in ("buy", "sell"):
        trade_action = action
        if not quantity_mode:
            quantity_mode = "exact"
    else:
        raise ValueError("unsupported trade action: %s" % action)

    reference_price = raw.get("reference_price", raw.get("price"))
    if reference_price in (None, ""):
        raise ValueError("trade signal missing reference_price")

    if quantity_mode == "exact":
        amount = int(raw["amount"])
        if amount <= 0:
            raise ValueError("trade signal amount must be positive")
    else:
        amount = int(raw["amount"]) if raw.get("amount") is not None else 0

    expire_at = raw.get("expire_at")
    signal = {
        "signal_id": str(raw["signal_id"]),
        "strategy_id": str(raw["strategy_id"]),
        "mode": str(raw.get("mode", "live")).lower(),
        "action": trade_action,
        "code": str(raw["code"]),
        "amount": amount,
        "reference_price": float(reference_price),
        "created_at": str(raw.get("created_at", raw.get("timestamp", ""))),
        "sent_at_ms": raw.get("sent_at_ms"),
        "expire_at": str(expire_at) if expire_at else None,
        "quantity_mode": quantity_mode,
    }
    budget_raw = raw.get("budget_group_size")
    if budget_raw is not None:
        signal["budget_group_size"] = int(budget_raw)
    return {"kind": "trade", "message_id": str(message_id), "signal": signal}


def signal_expiration_status(signal, now_epoch, expire_seconds=0):
    """返回 None / "EXPIRED" / "FAILED_RISK"。

    旧协议 expire_at(绝对时间字符串)优先兼容；新机制按
    sent_at_ms + expire_seconds 计算截止时间。sent_at_ms 缺失或
    expire_seconds<=0 时不做过期判断。
    """
    expire_at = signal.get("expire_at")
    if expire_at:
        try:
            deadline = datetime.datetime.strptime(expire_at, "%Y-%m-%d %H:%M:%S")
        except (TypeError, ValueError):
            return "FAILED_RISK"
        if now_epoch > time.mktime(deadline.timetuple()):
            return "EXPIRED"
        return None
    seconds = int(expire_seconds or 0)
    sent_at_ms = signal.get("sent_at_ms")
    if seconds > 0 and sent_at_ms is not None:
        if now_epoch * 1000 > int(sent_at_ms) + seconds * 1000:
            return "EXPIRED"
    return None


def jq_code_to_qmt_code(code):
    """聚宽代码转换为大 QMT 代码。"""
    if code.endswith(".XSHG"):
        return code[:-5] + ".SH"
    if code.endswith(".XSHE"):
        return code[:-5] + ".SZ"
    return code


def _display_stock_label(code, stock_name=None):
    """日志展示用证券标识：中文名(六码)，取不到名称时回退六码。"""
    short_code = str(code).split(".", 1)[0]
    return "%s(%s)" % (stock_name, short_code) if stock_name else short_code


def _gateway_instrument_label(gateway, code):
    """优先用网关的中文名格式化接口，取不到再回退名称解析。"""
    formatter = getattr(gateway, "instrument_label", None)
    if callable(formatter):
        return formatter(code)
    name_resolver = getattr(gateway, "instrument_name", None)
    name = name_resolver(code) if callable(name_resolver) else None
    return _display_stock_label(code, name)


def _normalize_qmt_instrument(instrument_id, exchange_id=""):
    """把"代码 + 交易所"补成带点的完整合约号；已带点则原样返回。"""
    code = str(instrument_id or "")
    if "." in code:
        return code
    exchange = str(exchange_id or "").upper()
    return code + "." + exchange if code and exchange else code


def tick_size_for(code):
    """沪深基金/ETF 使用 0.001，其余股票使用 0.01。"""
    qmt_code = jq_code_to_qmt_code(code)
    digits = qmt_code.split(".", 1)[0]
    market = qmt_code.split(".", 1)[1] if "." in qmt_code else ""
    if (market == "SH" and digits.startswith("5")) or (
        market == "SZ" and digits.startswith("1")
    ):
        return 0.001
    return 0.01


def _is_a_share(code):
    """只识别沪深 A 股，避免把股票价格笼子套到 ETF、基金或债券。"""
    security_code = code.split(".", 1)[0]
    suffix = code.rsplit(".", 1)[-1] if "." in code else ""
    if suffix in ("XSHG", "SH"):
        return security_code.startswith("6")
    if suffix in ("XSHE", "SZ"):
        return security_code.startswith(("0", "3"))
    return False


def _positive_price_or_none(value):
    """静态信息里的涨跌停价字段转 float; 0/负数/缺失/非法都返回 None。"""
    try:
        price = float(value)
    except (TypeError, ValueError):
        return None
    return price if price > 0 else None


def _first_positive(values):
    if not values:
        return None
    try:
        value = float(values[0])
    except (TypeError, ValueError, IndexError):
        return None
    return value if value > 0 else None


def _round_to_tick_half_up(value, tick):
    """按交易所常用的四舍五入取到合法 tick，避开 Python 银行家舍入。"""
    value_decimal = Decimal(str(round(value, 12)))
    tick_decimal = Decimal(str(tick))
    tick_units = (value_decimal / tick_decimal).quantize(
        Decimal("1"), rounding=ROUND_HALF_UP,
    )
    return float(tick_units * tick_decimal)


def _resolve_sell_all(available_position):
    """清仓数量 = 真实可卖持仓（负值按 0），与 miniQMT sizing 一致。"""
    return max(int(available_position), 0)


def _resolve_sell_half(available_position, insufficient_lot_mode="sell_all"):
    """卖一半: 向下取整到整手；不足一手按模式处理（与 miniQMT sizing 一致）。"""
    position = max(int(available_position), 0)
    half = position // 2 // 100 * 100
    if half >= 100:
        return half
    if insufficient_lot_mode == "skip":
        return 0
    return position


def _resolve_auto_buy(
    available_cash, total_assets, buy_count, price, max_single_position_pct
):
    """自动买入: 等分可用资金并受单票上限约束, 向下取整到 100 股。"""
    if buy_count <= 0 or price <= 0:
        return 0
    per_stock_budget = min(
        available_cash / buy_count,
        total_assets * max_single_position_pct,
    )
    return int(per_stock_budget / price / 100) * 100


def _auction_queue_price(action, last_price, config, tick_size, limits):
    """9:15~9:30 的排队报价; 不适用时返回 None 由常规模式定价。

    与 miniQMT 版 pricing._auction_queue_price 同一套规则。为什么不挂涨跌停价:
    策略 9:27/9:29 发的信号早已错过 9:25 的开盘集合竞价, 这些委托排队等 9:30
    连续竞价开撮, 成交价按对手方挂单价逐档确定。挂跌停价卖, 能成交的部分确实
    按买一价成交, 但吃不完的剩余会留在跌停价当卖一, 被后续买单以跌停价扫走。
    所以只做有界的百分比激进报价, 并强制夹进 [跌停价, 涨停价];
    取不到涨跌停价则整个回退常规定价 —— 宁可排位靠后, 不能裸奔。
    """
    pct = float(config.get("quote_band_pct", 0) or 0)
    if pct <= 0:
        return None
    if not _in_call_auction():
        return None
    high_limit, low_limit = limits if limits else (None, None)
    if high_limit is None or low_limit is None:
        return None

    raw_price = last_price * (1.0 + pct) if action == "buy" else last_price * (1.0 - pct)
    price = round(raw_price / tick_size) * tick_size
    return min(max(price, float(low_limit)), float(high_limit))


def calculate_order_price(signal, tick, config, limits=None):
    """按竞价排队 / 盘口 / 滑点定价。limits 是 (涨停价, 跌停价), 缺省则不走竞价报价。

    刻意不做"行情价偏离 reference_price 就拒单"的拦截 (与 miniQMT 版一致):
    本执行端是跟单器, 被拦下的委托会让实盘持仓与聚宽模拟盘永久分叉, 而且没有
    补单机制。价格风险由实时盘口定价、竞价报价的涨跌停夹取, 以及 ±10% 涨跌停带
    兜底; 择时与选股风控属于策略端职责。
    """
    action = signal["action"]
    last_price = float(tick.get("lastPrice") or 0)
    if last_price <= 0:
        raise ValueError("latest price is unavailable")
    pricing_mode = str(config.get("pricing_mode", "book")).lower()
    tick_size = tick_size_for(signal["code"])

    auction_price = _auction_queue_price(action, last_price, config, tick_size, limits)
    if auction_price is not None:
        order_price = auction_price
    else:
        pricing_base = None

        if pricing_mode == "book":
            if action == "buy":
                pricing_base = _first_positive(tick.get("askPrice"))
            else:
                pricing_base = _first_positive(tick.get("bidPrice"))

        if pricing_base is None:
            pricing_base = last_price
            if action == "buy":
                raw_price = pricing_base * (1.0 + float(config["quote_band_pct"]))
            else:
                raw_price = pricing_base * (1.0 - float(config["quote_band_pct"]))
        else:
            offset = int(config.get("book_tick_offset", 0)) * tick_size
            raw_price = pricing_base + offset if action == "buy" else pricing_base - offset
        order_price = raw_price

    # 沪深 A 股夹进动态价格笼子: 基准±2% 与基准±10tick 取较宽者, 再夹涨跌停。
    # 与 miniQMT 版 pricing._apply_stock_dynamic_price_cage 同一套规则, 防止
    # 开盘高波动时委托价超出交易所有效申报范围被拒单。
    if _is_a_share(signal["code"]):
        order_price = _apply_stock_dynamic_price_cage(
            action, tick, order_price, tick_size, limits
        )
        return _round_to_tick_half_up(order_price, tick_size)
    return round(round(order_price / tick_size) * tick_size, 3)


def _apply_stock_dynamic_price_cage(action, tick, candidate_price, tick_size, limits):
    """把沪深 A 股候选委托价夹进动态价格笼子和当日涨跌停范围。"""
    if action == "buy":
        reference = (
            _first_positive(tick.get("askPrice"))
            or _first_positive(tick.get("bidPrice"))
            or float(tick.get("lastPrice") or 0)
        )
        percent_boundary = _round_to_tick_half_up(reference * 1.02, tick_size)
        dynamic_boundary = max(percent_boundary, reference + 10 * tick_size)
        price = min(candidate_price, dynamic_boundary)
        if price == dynamic_boundary:
            # 贴住笼顶回退 1 tick: 102% 边界经 tick 取整可能越界被交易所拒单。
            price = max(price - tick_size, 0.0)
    else:
        reference = (
            _first_positive(tick.get("bidPrice"))
            or _first_positive(tick.get("askPrice"))
            or float(tick.get("lastPrice") or 0)
        )
        percent_boundary = _round_to_tick_half_up(reference * 0.98, tick_size)
        dynamic_boundary = min(percent_boundary, reference - 10 * tick_size)
        price = max(candidate_price, dynamic_boundary)
        if price == dynamic_boundary:
            # 贴住笼底抬升 1 tick, 同理避免取整越界被拒单。
            price = price + tick_size
    if limits:
        high_limit, low_limit = limits
        if high_limit is not None:
            price = min(price, float(high_limit))
        if low_limit is not None:
            price = max(price, float(low_limit))
    return price


# ---------------------------------------------------------------------------
# Redis 消费线程
# ---------------------------------------------------------------------------
class RedisStreamWorker(object):
    """仅负责 Redis I/O 的后台线程，不调用任何大 QMT API。"""

    def __init__(self, config, inbound_queue, ack_queue, redis_factory=None):
        self.config = config
        self.inbound_queue = inbound_queue
        self.ack_queue = ack_queue
        self.redis_factory = redis_factory
        self.client = None
        self.consumer_name = "bigqmt-%s" % uuid.uuid4().hex[:10]
        self._stop_event = threading.Event()
        self._thread = None

    def connect(self):
        """建立 Redis 连接；无 factory 时懒加载 redis 包并缺省使用 redis.Redis。"""
        factory = self.redis_factory
        if factory is None:
            try:
                import redis
            except ImportError:
                raise RuntimeError("大 QMT Python 环境缺少 redis 包")
            factory = redis.Redis
        self.client = factory(
            host=self.config["redis_host"],
            port=int(self.config.get("redis_port", 6379)),
            password=self.config.get("redis_password") or None,
            decode_responses=True,
            socket_connect_timeout=1,
        )

    def ensure_group(self):
        """确保消费组已创建；已存在(BUSYGROUP)视为正常，其余异常上抛。"""
        try:
            self.client.xgroup_create(
                self.config["redis_stream"],
                self.config["redis_group"],
                id="$",
                mkstream=True,
            )
            _log("INFO", "Redis消费组已从最新位置创建")
        except Exception as exc:
            if "BUSYGROUP" not in str(exc):
                raise

    def process_ack_queue(self):
        """把 ack 队列里的消息 id 全部 ACK；失败时放回队列并上抛触发重连。"""
        while True:
            try:
                message_id = self.ack_queue.get_nowait()
            except queue_module.Empty:
                return
            try:
                self.client.xack(
                    self.config["redis_stream"], self.config["redis_group"], message_id
                )
            except Exception:
                self.ack_queue.put(message_id)
                raise

    def read_once(self):
        """读取一批流消息并解析入队；格式错误的消息记日志后直接 ACK 丢弃。"""
        response = self.client.xreadgroup(
            self.config["redis_group"],
            self.consumer_name,
            {self.config["redis_stream"]: ">"},
            count=10,
            block=int(self.config.get("redis_block_ms", 500)),
        )
        for _stream, messages in response or []:
            for message_id, fields in messages:
                try:
                    message = parse_stream_message(message_id, fields)
                except Exception as exc:
                    _log("ERROR", "Redis消息格式错误，已丢弃并确认: %s" % exc)
                    self.ack_queue.put(str(message_id))
                    continue
                self.inbound_queue.put(message)

    def run(self):
        """后台主循环：连接、建组后循环 ACK 与读取，断线按重连间隔重试。"""
        while not self._stop_event.is_set():
            try:
                self.connect()
                self.ensure_group()
                _log(
                    "INFO",
                    "Redis监听已启动 stream=%s group=%s consumer=%s"
                    % (
                        self.config["redis_stream"],
                        self.config["redis_group"],
                        self.consumer_name,
                    ),
                )
                while not self._stop_event.is_set():
                    self.process_ack_queue()
                    self.read_once()
            except Exception as exc:
                if self._stop_event.is_set():
                    break
                _log("ERROR", "Redis连接或消费失败: %s" % exc)
                self._stop_event.wait(float(self.config.get("redis_reconnect_sec", 3.0)))

    def start(self):
        """以守护线程启动后台循环；已在运行时跳过。"""
        if self._thread is not None and self._thread.is_alive():
            return
        self._thread = threading.Thread(target=self.run, name="bigqmt-redis")
        self._thread.daemon = True
        self._thread.start()

    def stop(self):
        """置停止事件，让后台循环退出。"""
        self._stop_event.set()


def _log(level, message):
    print("[%s][%s] %s" % (time.strftime("%Y-%m-%d %H:%M:%S"), level, message))


def classify_order_status(status):
    """把大 QMT 委托状态映射为状态机类别；未知值保守视为在途。"""
    value = int(status)
    if value in (53, 54):
        return "CANCELED"
    if value == 56:
        return "FILLED"
    if value == 57:
        return "REJECTED"
    return "OPEN"


# 拒单原因分类关键字 —— 与 miniQMT 版 adapters/qmt.py classify_qmt_rejection 同一套。
_HARD_STOP_REJECTION_KEYWORDS = (
    "停牌",
    "账户异常",
    "账户状态异常",
    "无交易权限",
    "交易权限",
    "权限不足",
    "未开通",
    "股东账户",
    "股东代码",
    "证券账户未指定",
    "禁止买入",
    "禁止卖出",
    "禁止交易",
    "禁买",
    "禁卖",
    "不允许交易",
    "未登录",
)
_PRICE_REJECTION_KEYWORDS = (
    "委托价",
    "订单价格",
    "价格超出",
    "报价不正确",
    "价格笼子",
    "涨跌停范围",
)
_RESOURCE_REJECTION_KEYWORDS = (
    "资金不足",
    "可用资金",
    "可卖数量",
    "持仓不足",
    "委托数量",
    "数量不正确",
    "最大可委托",
)
_TRANSIENT_REJECTION_KEYWORDS = (
    "繁忙",
    "柜台忙",
    "稍后重试",
    "频率过高",
    "流控",
)


def classify_qmt_rejection(reason):
    """把大 QMT/柜台废单文案归一化；未识别的已确认废单默认允许重试。"""
    normalized = str(reason or "").strip().lower()
    for keyword in _HARD_STOP_REJECTION_KEYWORDS:
        if keyword in normalized:
            return "HARD_STOP"
    for keyword in _PRICE_REJECTION_KEYWORDS:
        if keyword in normalized:
            return "PRICE"
    for keyword in _RESOURCE_REJECTION_KEYWORDS:
        if keyword in normalized:
            return "RESOURCE"
    for keyword in _TRANSIENT_REJECTION_KEYWORDS:
        if keyword in normalized:
            return "TRANSIENT"
    return "UNKNOWN"


def _order_rejection_reason(order_info):
    """从大 QMT 委托对象读取拒单原因; 字段名不统一, 兼容常见几种。"""
    for attr in ("m_strErrorMsg", "m_strMessage", "m_strCancelInfo"):
        value = getattr(order_info, attr, "")
        if value:
            return str(value)
    return ""


# ---------------------------------------------------------------------------
# 大 QMT 网关适配
# ---------------------------------------------------------------------------
class BigQmtGateway(object):
    """把大 QMT 全局交易函数收口为可注入、可测试的接口。"""

    def __init__(self, context, config):
        self.context = context
        self.config = config
        self._limit_cache = {}
        self._limit_cache_day = ""

    def latest_tick(self, code):
        """取某只股票的最新行情快照；取不到时抛 RuntimeError。"""
        qmt_code = jq_code_to_qmt_code(code)
        ticks = self.context.get_full_tick([qmt_code])
        tick = ticks.get(qmt_code)
        if not tick:
            raise RuntimeError("无法取得行情: %s" % self.instrument_label(code))
        return tick

    def instrument_label(self, code):
        """取证券的中文名(六码)用于日志；取不到名称时回退六码。"""
        qmt_code = jq_code_to_qmt_code(code)
        getter = getattr(self.context, "get_instrument_detail", None)
        if getter is None:
            return _display_stock_label(qmt_code)
        try:
            detail = getter(qmt_code) or {}
        except Exception:
            return _display_stock_label(qmt_code)
        name = detail.get("InstrumentName") or detail.get("instrument_name")
        return _display_stock_label(qmt_code, str(name).strip() if name else None)

    def limit_prices(self, code):
        """取当日 (涨停价, 跌停价), 按代码+交易日缓存; 取不到返回 (None, None)。

        tick 里没有涨跌停价, 只能从静态合约信息取。取不到不是错误 —— 竞价排队
        报价会因此回退常规定价, 宁可排位靠后也不用错价挂单。
        """
        qmt_code = jq_code_to_qmt_code(code)
        today = datetime.date.today().isoformat()
        if today != self._limit_cache_day:
            self._limit_cache = {}
            self._limit_cache_day = today
        cached = self._limit_cache.get(qmt_code)
        if cached is not None:
            return cached

        getter = getattr(self.context, "get_instrument_detail", None)
        if getter is None:
            return (None, None)
        try:
            detail = getter(qmt_code)
        except Exception as exc:
            _log("WARN", "涨跌停价查询失败 %s: %s" % (_display_stock_label(qmt_code), exc))
            return (None, None)
        if not detail:
            return (None, None)
        prices = (
            _positive_price_or_none(detail.get("UpStopPrice")),
            _positive_price_or_none(detail.get("DownStopPrice")),
        )
        self._limit_cache[qmt_code] = prices
        return prices

    def available_cash(self):
        """查询账户可用资金；取不到时抛 RuntimeError 让上层不下单。"""
        accounts = get_trade_detail_data(
            self.config["account_id"], self.config["account_type"], "account"
        )
        if not accounts:
            raise RuntimeError("无法取得账户资金")
        value = getattr(accounts[0], "m_dAvailable", None)
        if value is None:
            raise RuntimeError("账户对象缺少 m_dAvailable")
        return float(value)

    def available_position(self, code):
        """查询某只股票的可卖持仓数量；未持仓返回 0。"""
        qmt_code = jq_code_to_qmt_code(code)
        positions = get_trade_detail_data(
            self.config["account_id"], self.config["account_type"], "position"
        )
        for position in positions or []:
            position_code = _normalize_qmt_instrument(
                getattr(position, "m_strInstrumentID", ""),
                getattr(position, "m_strExchangeID", ""),
            )
            if position_code != qmt_code:
                continue
            value = getattr(position, "m_nCanUseVolume", None)
            if value is None:
                raise RuntimeError("持仓对象缺少 m_nCanUseVolume")
            return int(value)
        return 0

    def total_position(self, code):
        """查询某只股票的总持仓(含当日不可卖部分), 用于日计划"已有持仓不补仓"。"""
        qmt_code = jq_code_to_qmt_code(code)
        positions = get_trade_detail_data(
            self.config["account_id"], self.config["account_type"], "position"
        )
        for position in positions or []:
            position_code = _normalize_qmt_instrument(
                getattr(position, "m_strInstrumentID", ""),
                getattr(position, "m_strExchangeID", ""),
            )
            if position_code != qmt_code:
                continue
            value = getattr(position, "m_nVolume", None)
            if value is None:
                raise RuntimeError("持仓对象缺少 m_nVolume")
            return int(value)
        return 0

    def query_total_assets(self):
        """查询账户总资产；取不到时抛 RuntimeError。"""
        accounts = get_trade_detail_data(
            self.config["account_id"], self.config["account_type"], "account"
        )
        if not accounts:
            raise RuntimeError("无法取得账户总资产")
        value = getattr(accounts[0], "m_dTotalAssets", None)
        if value is None:
            raise RuntimeError("账户对象缺少 m_dTotalAssets")
        return float(value)

    def list_orders(self):
        """取本策略当日的委托列表；老接口不认 strategy_name 时回退全量查询。"""
        try:
            return get_trade_detail_data(
                self.config["account_id"],
                self.config["account_type"],
                "order",
                self.config["strategy_name"],
            ) or []
        except TypeError:
            return get_trade_detail_data(
                self.config["account_id"], self.config["account_type"], "order"
            ) or []

    def submit(self, signal, quantity, price, remark):
        """按方向调用 passorder 报单，备注 remark 用于委托回报归属。"""
        op_type = 23 if signal["action"] == "buy" else 24
        passorder(
            op_type,
            1101,
            self.config["account_id"],
            jq_code_to_qmt_code(signal["code"]),
            11,
            price,
            quantity,
            self.config["strategy_name"],
            1,
            remark,
            self.context,
        )

    def can_cancel(self, order_id):
        """判断某委托当前是否可撤。"""
        return bool(
            can_cancel_order(
                order_id, self.config["account_id"], self.config["account_type"]
            )
        )

    def cancel(self, order_id):
        """请求撤销指定委托，返回是否受理。"""
        return bool(
            cancel(
                order_id,
                self.config["account_id"],
                self.config["account_type"],
                self.context,
            )
        )


# ---------------------------------------------------------------------------
# 运行时执行状态机
# ---------------------------------------------------------------------------
class BigQmtRuntime(object):
    """单次大 QMT 运行期内按买卖方向分别 FIFO 的内存执行状态机。"""

    ACTION_ORDER = ("sell", "buy")

    def __init__(self, config, worker, gateway_factory=None, clock=None):
        self.config = config
        self.worker = worker
        self.gateway_factory = gateway_factory or BigQmtGateway
        self.clock = clock or time.time
        self.gateway = None
        self.pending_by_action = dict((action, deque()) for action in self.ACTION_ORDER)
        self.active_by_action = dict((action, None) for action in self.ACTION_ORDER)
        self.seen_signal_ids = set()
        self.seen_plan_ids = set()
        # plan 消息只在其全部派生信号终态后 ACK (与 miniQMT 组合 Future 语义一致)。
        self._plan_children_remaining = {}
        self.universe = set()
        self.halted = False
        self.halt_reason = ""
        # 开盘屏障: 尚未完成的盘前卖单数量, 买单必须等它们全部终态后再提交。
        self._preopen_sell_count = 0

    @property
    def pending_count(self):
        """两个方向待执行消息的总数。"""
        return sum(len(items) for items in self.pending_by_action.values())

    @property
    def active(self):
        """兼容单通道运行时检查；双通道同时活动时优先返回卖单。"""
        for action in self.ACTION_ORDER:
            active = self.active_by_action[action]
            if active is not None:
                return active
        return None

    def on_timer(self, context):
        """定时器驱动的主循环：拉取入站消息、启动新信号并推进活动订单。"""
        if self.gateway is None:
            self.gateway = self.gateway_factory(context, self.config)
        self._drain_inbound(context)
        if self.halted:
            # 与 miniQMT 一致: 熔断后的后续信号以 FAILED_BROKER 落终态并 ACK,
            # 不让 Redis pending 无限堆积; 恢复时凭 QMT 委托与日志人工对账。
            self._flush_pending_halted("sell")
            self._flush_pending_halted("buy")
            return
        for action in self.ACTION_ORDER:
            if self.active_by_action[action] is None and self.pending_by_action[action]:
                # 开盘屏障: 9:25~9:30 的买单排队等待开盘, 且必须等盘前卖单
                # 全部完成后才提交 —— 卖出回款计入可用资金, 与 miniQMT 一致。
                if action == "buy" and (
                    self._preopen_sell_count > 0 or _in_preopen_window()
                ):
                    continue
                self._start_next(action)
                if self.halted:
                    return
        if any(self.active_by_action.values()):
            self._refresh_active_orders()
            if not self.halted:
                for action in self.ACTION_ORDER:
                    if self.active_by_action[action] is not None:
                        self._advance_active(action)

    def _allowed_strategy(self, strategy_id):
        """白名单过滤；白名单为空时放行所有策略。"""
        allowed = self.config.get("allowed_strategy_ids") or []
        return not allowed or str(strategy_id) in allowed

    def _drain_inbound(self, context):
        """排空入站队列：按 watchlist/plan/trade 分流并做白名单与幂等过滤。"""
        while True:
            try:
                message = self.worker.inbound_queue.get_nowait()
            except queue_module.Empty:
                return
            if message["kind"] == "watchlist":
                if not self._allowed_strategy(message.get("strategy_id", "")):
                    _log("WARNING", "预订阅忽略 | 策略 %s 不在白名单" % message.get("strategy_id", ""))
                    self.worker.ack_queue.put(message["message_id"])
                    continue
                self.universe.update(jq_code_to_qmt_code(code) for code in message["codes"])
                context.set_universe(sorted(self.universe))
                self.worker.ack_queue.put(message["message_id"])
                continue
            if message["kind"] == "plan":
                plan = message["plan"]
                if plan.get("mode") != "live":
                    _log("INFO", "日计划非实时模式 | ACK | %s" % plan["signal_id"])
                    self.worker.ack_queue.put(message["message_id"])
                    continue
                if not self._allowed_strategy(plan["strategy_id"]):
                    _log("WARNING", "日计划忽略 | 策略 %s 不在白名单" % plan["strategy_id"])
                    self.worker.ack_queue.put(message["message_id"])
                    continue
                try:
                    self._expand_plan(message)
                except Exception as exc:
                    _log(
                        "ERROR",
                        "日计划展开失败 %s: %s | ACK" % (plan["signal_id"], exc),
                    )
                    self.worker.ack_queue.put(message["message_id"])
                continue
            signal = message["signal"]
            if signal.get("mode") != "live":
                self.worker.ack_queue.put(message["message_id"])
                continue
            if not self._allowed_strategy(signal["strategy_id"]):
                _log("WARNING", "信号忽略 | 策略 %s 不在白名单 | ACK" % signal["strategy_id"])
                self.worker.ack_queue.put(message["message_id"])
                continue
            signal_id = signal["signal_id"]
            if signal_id in self.seen_signal_ids:
                self.worker.ack_queue.put(message["message_id"])
                continue
            self.seen_signal_ids.add(signal_id)
            self.pending_by_action[signal["action"]].append(message)

    def _expand_plan(self, message):
        """把日计划展开为派生信号; 全部派生信号终态后才 ACK plan (与 miniQMT 一致)。"""
        plan = message["plan"]
        plan_id = plan["signal_id"]
        if plan_id in self.seen_plan_ids:
            self.worker.ack_queue.put(message["message_id"])
            return
        self.seen_plan_ids.add(plan_id)

        derived = []
        for code in plan["codes_to_sell"]:
            derived.append(self._derived_signal(message, plan, code, "sell_all", None))
        buy_codes = [
            code for code in plan["codes_to_buy"]
            if self.gateway.total_position(jq_code_to_qmt_code(code)) <= 0
        ]
        for code in buy_codes:
            derived.append(self._derived_signal(message, plan, code, "auto_buy", len(buy_codes)))

        enqueued = 0
        for child in derived:
            if child["signal"]["signal_id"] in self.seen_signal_ids:
                continue
            self.seen_signal_ids.add(child["signal"]["signal_id"])
            child["parent_plan_id"] = message["message_id"]
            self.pending_by_action[child["signal"]["action"]].append(child)
            enqueued += 1
        if enqueued == 0:
            self.worker.ack_queue.put(message["message_id"])
            return
        self._plan_children_remaining[message["message_id"]] = enqueued
        _log("INFO", "日计划已展开 | %s | 派生信号 %s 个" % (plan_id, enqueued))

    @staticmethod
    def _derived_signal(message, plan, code, quantity_mode, budget_group_size):
        """日计划派生信号: signal_id 与 miniQMT PlanExecutor 同规则, 保证重放幂等。"""
        safe_code = re.sub(r"[^0-9A-Za-z]", "", str(code))
        trade_action = "sell" if quantity_mode == "sell_all" else "buy"
        return {
            "kind": "trade",
            "message_id": message["message_id"],
            "signal": {
                "signal_id": "%s-%s-%s" % (plan["signal_id"], trade_action, safe_code),
                "strategy_id": plan["strategy_id"],
                "mode": plan["mode"],
                "action": trade_action,
                "code": str(code),
                "amount": 0,
                "reference_price": 0.0,
                "created_at": plan["created_at"],
                "sent_at_ms": plan["sent_at_ms"],
                "expire_at": None,
                "quantity_mode": quantity_mode,
                "budget_group_size": budget_group_size,
            },
        }

    def _start_next(self, action):
        """从指定方向取出下一条信号，解析意图数量并提交首笔委托。"""
        message = self.pending_by_action[action].popleft()
        signal = message["signal"]

        # 意图型信号: 先按真实账户解析数量, 与 miniQMT 执行顺序一致。
        if signal.get("quantity_mode", "exact") != "exact":
            try:
                quantity_mode = signal["quantity_mode"]
                resolved = self._resolve_intent_quantity(signal)
            except Exception as exc:
                _log("ERROR", "意图数量解析失败 %s: %s" % (signal["signal_id"], exc))
                self._ack_message(message["message_id"], message.get("parent_plan_id"))
                return
            if resolved <= 0:
                if signal["action"] == "sell":
                    if (
                        signal.get("quantity_mode") == "sell_half"
                        and self.gateway.available_position(
                            jq_code_to_qmt_code(signal["code"])
                        )
                        > 0
                    ):
                        status = "SKIPPED_SMALL_POSITION"
                    else:
                        status = "SKIPPED_NO_POSITION"
                else:
                    status = "FAILED_RISK"
                _log(
                    "INFO",
                    "信号终态 %s status=%s %s数量=0 未下单"
                    % (signal["signal_id"], status, quantity_mode),
                )
                self._ack_message(message["message_id"], message.get("parent_plan_id"))
                return
            signal["amount"] = resolved
            signal["quantity_mode"] = "exact"
            _log("INFO", "实盘计算数量 %s=%s股" % (quantity_mode, resolved))

        expiration_status = signal_expiration_status(
            signal,
            self.clock(),
            self.config.get("signal_expire_seconds", 600),
        )
        if expiration_status is not None:
            level = "ERROR" if expiration_status == "FAILED_RISK" else "WARNING"
            _log(
                level,
                "信号终态 %s status=%s expire_at=%r 未下单"
                % (
                    signal["signal_id"],
                    expiration_status,
                    signal.get("expire_at"),
                ),
            )
            self._ack_message(message["message_id"], message.get("parent_plan_id"))
            return
        active = {
            "message": message,
            "signal": signal,
            "code_label": _gateway_instrument_label(self.gateway, signal["code"]),
            "requested_qty": int(signal["amount"]),
            "target_qty": None,
            "filled_qty": 0,
            "attempt": 0,
            "attempt_filled": 0,
            "state": "QUEUED",
            "started_at": self.clock(),
            "order_id": "",
            "remark": "",
            "order_status": None,
            "cancel_requested_at": None,
            "deal_ids": set(),
            "barrier_released": False,
        }
        active["is_preopen_sell"] = is_preopen_sell(signal)
        if active["is_preopen_sell"]:
            self._preopen_sell_count += 1
        active["total_deadline"] = self._deadline_after_auction(
            active["started_at"], float(self.config["max_total_duration_sec"])
        )
        self.active_by_action[action] = active
        self._submit_attempt(action)

    def _resolve_intent_quantity(self, signal):
        """意图型信号 → 具体股数（sell_all/sell_half 查持仓, auto_buy 查资金+行情）。"""
        qmt_code = jq_code_to_qmt_code(signal["code"])
        quantity_mode = signal["quantity_mode"]
        if quantity_mode == "sell_all":
            return _resolve_sell_all(self.gateway.available_position(qmt_code))
        if quantity_mode == "sell_half":
            return _resolve_sell_half(
                self.gateway.available_position(qmt_code),
                self.config.get("sell_half_insufficient_lot_mode", "sell_all"),
            )
        if quantity_mode == "auto_buy":
            tick = self.gateway.latest_tick(signal["code"])
            return _resolve_auto_buy(
                self.gateway.available_cash(),
                self.gateway.query_total_assets(),
                int(signal.get("budget_group_size") or 1),
                float(tick.get("lastPrice") or 0),
                float(self.config.get("max_single_position_pct", 0.2)),
            )
        raise ValueError("invalid quantity_mode: %s" % quantity_mode)

    def _submit_attempt(self, action):
        """按最新行情与资源提交一次委托；首次缩量后冻结执行目标数量。"""
        active = self.active_by_action[action]
        signal = active["signal"]
        target_qty = active.get("target_qty")
        remaining = (
            target_qty if target_qty is not None else active["requested_qty"]
        ) - active["filled_qty"]
        if remaining <= 0:
            self._finish_active(action, "FILLED")
            return
        try:
            tick = self.gateway.latest_tick(signal["code"])
            limits = self.gateway.limit_prices(signal["code"])
            high_limit, low_limit = limits if limits else (None, None)
            bid1 = _first_positive(tick.get("bidPrice"))
            ask1 = _first_positive(tick.get("askPrice"))

            # ---------------------------------------------------------------------------
            # 涨跌停锁盘处理（与 miniQMT 版一致）
            # ---------------------------------------------------------------------------
            # 跌停无买盘 → 卖单挂跌停价排队等开板, 或按配置跳过;
            # 涨停确认锁盘 → 买单挂涨停价排队, 或按配置跳过。
            queue_mode = None
            queue_deadline = None
            if signal["action"] == "sell" and bid1 is None:
                mode = effective_limit_down_sell_mode(self.config)
                if mode == "queue":
                    if low_limit is None or low_limit <= 0:
                        self._finish_active(action, "SKIPPED_LIMIT_DOWN")
                        return
                    queue_deadline = self._queue_deadline_epoch(
                        self.config["queue_sell_deadline"]
                    )
                    if self.clock() >= queue_deadline:
                        self._finish_active(action, "SKIPPED_LIMIT_DOWN")
                        return
                    price = low_limit
                    queue_mode = "QUEUE_LIMIT_DOWN"
                elif mode == "skip":
                    self._finish_active(action, "SKIPPED_LIMIT_DOWN")
                    return
            elif (
                signal["action"] == "buy"
                and ask1 is None
                and _is_confirmed_limit_up(tick, high_limit, signal["code"])
            ):
                mode = effective_limit_up_buy_mode(self.config)
                if mode == "queue":
                    if high_limit is None or high_limit <= 0:
                        self._finish_active(action, "SKIPPED_LIMIT_UP")
                        return
                    queue_deadline = self._queue_deadline_epoch(
                        self.config["queue_buy_deadline"]
                    )
                    if self.clock() >= queue_deadline:
                        self._finish_active(action, "SKIPPED_LIMIT_UP")
                        return
                    price = high_limit
                    queue_mode = "QUEUE_LIMIT_UP"
                elif mode == "skip":
                    self._finish_active(action, "SKIPPED_LIMIT_UP")
                    return

            if queue_mode is None:
                price = calculate_order_price(signal, tick, self.config, limits)
            if signal["action"] == "buy":
                affordable = int(self.gateway.available_cash() // price)
                available = (affordable // 100) * 100
            else:
                available = int(
                    self.gateway.available_position(jq_code_to_qmt_code(signal["code"]))
                )
            quantity = min(remaining, available)
            if signal["action"] == "buy":
                quantity = (quantity // 100) * 100
            if quantity <= 0:
                if signal["action"] == "sell" and active["filled_qty"] == 0:
                    self._finish_active(action, "SKIPPED_NO_POSITION")
                else:
                    self._finish_active(action, "PARTIALLY_FILLED_TIMEOUT" if active["filled_qty"] else "FAILED_RISK")
                return
        except Exception as exc:
            _log("ERROR", "行情或资源查询失败，信号不下单: %s" % exc)
            self._finish_active(action, "FAILED_BROKER")
            return

        # 首次按真实资源缩量后冻结执行目标, 与 miniQMT target_qty 语义一致。
        if active.get("target_qty") is None:
            active["target_qty"] = quantity
        active["attempt"] += 1
        digest = hashlib.sha1(signal["signal_id"].encode("utf-8")).hexdigest()[:12]
        remark = "BQR-%s-%02d" % (digest, active["attempt"])
        active.update(
            {
                "remark": remark,
                "order_id": "",
                "order_status": None,
                "order_class": None,
                "attempt_qty": quantity,
                "attempt_filled": 0,
                "deal_filled_hint": 0,
                "deal_ids": set(),
                "price": price,
                "submitted_at": self.clock(),
                "state": "WAITING_ORDER_ID",
            "cancel_requested_at": None,
            "attempt_accounted": False,
            "queue_mode": queue_mode,
            "queue_deadline": queue_deadline,
        }
        )
        if (
            active["attempt"] == 1
            and active.get("is_preopen_sell")
            and active.get("queue_mode") is None
        ):
            # 盘前卖单首笔: 开盘后只给 0.5s 回报宽限再撤, 与 miniQMT 一致。
            active["order_deadline"] = self._deadline_after_auction(
                active["submitted_at"], 0
            ) + _OPENING_SELL_RECONCILE_GRACE_SEC
        elif (
            active["attempt"] == 1
            and signal["action"] == "buy"
            and active.get("queue_mode") is None
            and float(self.config.get("opening_order_timeout_sec", 0) or 0) > 0
        ):
            # 开盘首挂耐心(与 miniQMT 一致): 盘前挂单的买单=距开盘+耐心秒数;
            # 开盘窗口内的买单=耐心秒数。开盘撮合回执可能迟到 1~2 秒,
            # 0.5s 就撤会向已成交的单发多余撤单。
            patience = float(self.config["opening_order_timeout_sec"])
            if _in_call_auction():
                active["order_deadline"] = self._deadline_after_auction(
                    active["submitted_at"], patience
                )
            elif _in_opening_window():
                active["order_deadline"] = active["submitted_at"] + patience
            else:
                active["order_deadline"] = self._order_deadline(
                    active["submitted_at"]
                )
        else:
            active["order_deadline"] = self._order_deadline(active["submitted_at"])
        active["visibility_deadline"] = self._deadline_after_auction(
            active["submitted_at"],
            float(self.config["order_visibility_timeout_sec"]),
        )
        try:
            self.gateway.submit(signal, quantity, price, remark)
        except Exception as exc:
            self._halt("passorder 返回异常，委托状态不确定: %s" % exc)
            return
        _log(
            "INFO",
            "下单 %s %s %s股@%.3f 第%s次"
            % (
                signal["action"],
                active["code_label"],
                quantity,
                price,
                active["attempt"],
            ),
        )

    def on_order(self, order_info):
        """接收大 QMT 委托主推；实际状态推进留给下一次 timer。"""
        self._apply_order(order_info)

    def on_deal(self, deal_info):
        """成交主推只做去重后的成交提示，最终累计仍以委托对象为准。"""
        remark = str(getattr(deal_info, "m_strRemark", ""))
        active = self._active_for_remark(remark)
        if active is None:
            return
        trade_id = str(getattr(deal_info, "m_strTradeID", ""))
        if not trade_id or trade_id in active["deal_ids"]:
            return
        active["deal_ids"].add(trade_id)
        volume = int(getattr(deal_info, "m_nVolume", 0) or 0)
        active["deal_filled_hint"] = active.get("deal_filled_hint", 0) + volume

    def _refresh_active_orders(self):
        """拉取活动委托并按备注匹配更新对应活动单；查询失败则熔断。"""
        try:
            orders = self.gateway.list_orders()
        except Exception as exc:
            self._halt("查询活动委托失败，状态不确定: %s" % exc)
            return
        for order in orders:
            self._apply_order(order)

    def _apply_order(self, order_info):
        """把委托回报映射到备注匹配的活动单，记录单号、状态与本次成交。"""
        remark = str(getattr(order_info, "m_strRemark", ""))
        active = self._active_for_remark(remark)
        if active is None:
            return
        order_id = str(getattr(order_info, "m_strOrderSysID", "") or "")
        status = int(getattr(order_info, "m_nOrderStatus", 255))
        traded = int(getattr(order_info, "m_nVolumeTraded", 0) or 0)
        if traded < 0:
            traded = 0
        if order_id:
            active["order_id"] = order_id
        active["order_status"] = status
        order_class = classify_order_status(status)
        active["order_class"] = order_class
        if order_class == "REJECTED":
            active["rejection_kind"] = classify_qmt_rejection(
                _order_rejection_reason(order_info)
            )
        active["attempt_filled"] = max(active.get("attempt_filled", 0), traded)

    def _active_for_remark(self, remark):
        """按备注在两个方向的活动单里找匹配；空备注返回 None。"""
        if not remark:
            return None
        for action in self.ACTION_ORDER:
            active = self.active_by_action[action]
            if active is not None and remark == active.get("remark"):
                return active
        return None

    def _advance_active(self, action):
        """推进指定方向活动单的状态机：等待单号/排队/在途/撤单确认。"""
        active = self.active_by_action[action]
        now = self.clock()
        order_class = active.get("order_class")

        if active["state"] == "WAITING_ORDER_ID":
            if active.get("order_id"):
                if active.get("queue_mode"):
                    active["state"] = active["queue_mode"]
                    if (
                        active["queue_mode"] == "QUEUE_LIMIT_DOWN"
                        and active.get("is_preopen_sell")
                        and not active.get("barrier_released")
                    ):
                        # 跌停卖单已确认排队: 立即放行买单, 与 miniQMT 开盘屏障一致。
                        active["barrier_released"] = True
                        self._preopen_sell_count = max(0, self._preopen_sell_count - 1)
                elif order_class == "OPEN":
                    active["state"] = "WORKING"
                else:
                    self._handle_terminal_attempt(action, order_class)
                    return
            elif now >= active["visibility_deadline"]:
                self._halt("passorder 后未找到对应委托，无法确认是否已提交")
                return

        if active["state"] in ("QUEUE_LIMIT_DOWN", "QUEUE_LIMIT_UP"):
            # 涨跌停排队单: 不按普通 order_timeout 撤单重挂 (重挂丢队列位置),
            # 只轮询到终态或截止时刻撤一次单收尾, 与 miniQMT 版排队逻辑一致。
            if order_class in ("CANCELED", "FILLED", "REJECTED"):
                self._handle_terminal_attempt(action, order_class)
                return
            if now >= active.get("queue_deadline", 0):
                order_id = active.get("order_id")
                if not order_id:
                    self._halt("排队委托超时但缺少委托号")
                    return
                try:
                    if not self.gateway.can_cancel(order_id):
                        self._halt("排队委托不可撤，终态无法确认: %s" % order_id)
                        return
                    if not self.gateway.cancel(order_id):
                        self._halt("排队撤单请求未受理，终态无法确认: %s" % order_id)
                        return
                except Exception as exc:
                    self._halt("排队撤单调用失败，终态无法确认: %s" % exc)
                    return
                active["state"] = "CANCEL_REQUESTED"
                active["cancel_requested_at"] = now
                _log("INFO", "排队截止已请求撤单 order_id=%s" % order_id)
                return

        if active["state"] == "WORKING":
            if order_class in ("CANCELED", "FILLED", "REJECTED"):
                self._handle_terminal_attempt(action, order_class)
                return
            total_deadline = active["total_deadline"]
            if now >= active["order_deadline"] or now >= total_deadline:
                order_id = active.get("order_id")
                if not order_id:
                    self._halt("委托超时但缺少委托号")
                    return
                try:
                    if not self.gateway.can_cancel(order_id):
                        self._halt("在途委托不可撤，终态无法确认: %s" % order_id)
                        return
                    if not self.gateway.cancel(order_id):
                        self._halt("撤单请求未受理，终态无法确认: %s" % order_id)
                        return
                except Exception as exc:
                    self._halt("撤单调用失败，终态无法确认: %s" % exc)
                    return
                active["state"] = "CANCEL_REQUESTED"
                active["cancel_requested_at"] = now
                _log("INFO", "已请求撤单 order_id=%s" % order_id)
                return

        if active["state"] == "CANCEL_REQUESTED":
            if order_class in ("CANCELED", "FILLED", "REJECTED"):
                self._handle_terminal_attempt(action, order_class)
                return
            if now - active["cancel_requested_at"] >= float(
                self.config["cancel_confirm_timeout_sec"]
            ):
                self._halt("撤单终态等待超时: %s" % active.get("order_id"))

    def _handle_terminal_attempt(self, action, order_class):
        """按终态累计成交并决定补挂剩余、终止或熔断后的收尾。"""
        active = self.active_by_action[action]
        if active.get("attempt_accounted"):
            return
        active["attempt_filled"] = max(
            active.get("attempt_filled", 0), active.get("deal_filled_hint", 0)
        )
        if order_class == "FILLED":
            active["attempt_filled"] = max(
                active.get("attempt_filled", 0), active.get("attempt_qty", 0)
            )
        active["filled_qty"] += min(
            active.get("attempt_filled", 0), active.get("attempt_qty", 0)
        )
        active["attempt_accounted"] = True
        target_qty = active.get("target_qty")
        remaining = max(
            (target_qty if target_qty is not None else active["requested_qty"])
            - active["filled_qty"],
            0,
        )
        _log(
            "INFO",
            "委托终态 status=%s 本次成交=%s 累计=%s 剩余=%s"
            % (order_class, active.get("attempt_filled", 0), active["filled_qty"], remaining),
        )
        if remaining == 0:
            self._finish_active(action, "FILLED")
            return
        # 排队单不重挂 (重挂丢队列位置); 涨停排队被废单时,
        # 非硬拒单刷新重排 (与 miniQMT 主循环一致), 跌停排队被拒直接终态。
        if active.get("state") in ("QUEUE_LIMIT_DOWN", "QUEUE_LIMIT_UP"):
            if (
                active["state"] == "QUEUE_LIMIT_UP"
                and order_class == "REJECTED"
                and active.get("rejection_kind") != "HARD_STOP"
                and active["attempt"] < int(self.config["max_attempts"])
                and self.clock() < active["total_deadline"]
            ):
                active["state"] = "RETRYING"
                self._submit_attempt(action)
                return
            queue_expired_status = (
                "LIMIT_UP_QUEUE_EXPIRED"
                if active["state"] == "QUEUE_LIMIT_UP"
                else "LIMIT_DOWN_QUEUE_EXPIRED"
            )
            self._finish_active(
                action,
                "PARTIALLY_FILLED_TIMEOUT" if active["filled_qty"] else (
                    "FAILED_BROKER" if order_class == "REJECTED" else queue_expired_status
                ),
            )
            return
        # 硬拒单(停牌/权限/账户异常等永久性原因)立即终止, 不浪费重试窗口。
        if order_class == "REJECTED" and active.get("rejection_kind") == "HARD_STOP":
            self._finish_active(
                action,
                "PARTIALLY_FILLED_TIMEOUT" if active["filled_qty"] else "FAILED_BROKER",
            )
            return
        if active["attempt"] >= int(self.config["max_attempts"]):
            self._finish_active(
                action,
                "PARTIALLY_FILLED_TIMEOUT" if active["filled_qty"] else "FAILED_BROKER",
            )
            return
        if self.clock() >= active["total_deadline"]:
            self._finish_active(
                action,
                "PARTIALLY_FILLED_TIMEOUT" if active["filled_qty"] else "FAILED_TIMEOUT",
            )
            return
        active["state"] = "RETRYING"
        self._submit_attempt(action)

    def _order_deadline(self, submitted_at):
        """普通委托的撤单截止时刻；竞价时段顺延到开盘后计时。"""
        return self._deadline_after_auction(
            submitted_at, float(self.config["order_timeout_sec"])
        )

    def _queue_deadline_epoch(self, hhmmss):
        """把 queue_*_deadline (HH:MM:SS) 转成当天 epoch, 供排队单截止撤单。"""
        hour, minute, second = (int(part) for part in hhmmss.split(":"))
        moment = datetime.datetime.fromtimestamp(self.clock()).replace(
            hour=hour, minute=minute, second=second, microsecond=0
        )
        return time.mktime(moment.timetuple())

    def _deadline_after_auction(self, started_at, duration):
        """竞价时段提交的委托把计时起点顺延到 9:30 开盘，避免开盘前被误撤。"""
        normal_deadline = started_at + duration
        moment = datetime.datetime.fromtimestamp(started_at)
        auction_start = moment.replace(hour=9, minute=15, second=0, microsecond=0)
        market_open = moment.replace(hour=9, minute=30, second=0, microsecond=0)
        if auction_start <= moment < market_open:
            return max(
                normal_deadline,
                time.mktime(market_open.timetuple())
                + duration,
            )
        return normal_deadline

    def _finish_active(self, action, status):
        """把活动单落终态、ACK 消息并释放开盘屏障计数。"""
        active = self.active_by_action[action]
        if active is None:
            return
        message = active["message"]
        _log(
            "INFO",
            "信号终态 %s | %s status=%s"
            % (active["code_label"], active["signal"]["signal_id"], status),
        )
        self._ack_message(message["message_id"], message.get("parent_plan_id"))
        if active.get("is_preopen_sell") and not active.get("barrier_released"):
            self._preopen_sell_count = max(0, self._preopen_sell_count - 1)
        self.active_by_action[action] = None

    def _ack_message(self, message_id, parent_plan_id=None):
        """ACK 单条消息; 日计划派生信号全部终态后才 ACK 父 plan (与 miniQMT 一致)。"""
        if parent_plan_id:
            remaining = self._plan_children_remaining.get(parent_plan_id, 0)
            if remaining <= 1:
                self._plan_children_remaining.pop(parent_plan_id, None)
                self.worker.ack_queue.put(parent_plan_id)
            else:
                self._plan_children_remaining[parent_plan_id] = remaining - 1
        else:
            self.worker.ack_queue.put(message_id)

    def _flush_pending_halted(self, action):
        """熔断后把该方向剩余待执行信号以 FAILED_BROKER 落终态并 ACK。"""
        while self.pending_by_action[action]:
            message = self.pending_by_action[action].popleft()
            _log(
                "ERROR",
                "信号终态 %s status=FAILED_BROKER 熔断未执行"
                % message["signal"]["signal_id"],
            )
            self._ack_message(message["message_id"], message.get("parent_plan_id"))

    def _halt(self, reason):
        """置熔断标志并把活动单以 FAILED_BROKER 收尾，后续信号统一终态。"""
        self.halted = True
        self.halt_reason = reason
        _log("ERROR", "交易执行端熔断: %s" % reason)
        # 与 miniQMT 一致: 状态不明的活动单以 FAILED_BROKER 落终态并 ACK,
        # 后续信号由 on_timer 的熔断分支统一收尾。
        for action in self.ACTION_ORDER:
            if self.active_by_action[action] is not None:
                self._finish_active(action, "FAILED_BROKER")


# ---------------------------------------------------------------------------
# 策略入口与回调
# ---------------------------------------------------------------------------
_RUNTIME = None


def validate_config(config):
    """校验 CONFIG 必填项与取值域，不合法时抛 RuntimeError 阻止启动。"""
    if not config.get("trading_enabled"):
        raise RuntimeError("CONFIG.trading_enabled 必须显式设为 True")
    if not str(config.get("account_id", "")).strip():
        raise RuntimeError("CONFIG.account_id 不能为空")
    if str(config.get("account_type", "stock")).lower() != "stock":
        raise RuntimeError("当前版本只支持 account_type=stock 的普通账户")
    if not str(config.get("redis_host", "")).strip():
        raise RuntimeError("CONFIG.redis_host 不能为空")
    if str(config.get("pricing_mode", "")).lower() not in ("book", "slippage"):
        raise RuntimeError("CONFIG.pricing_mode 必须是 book 或 slippage")
    for name in (
        "order_timeout_sec",
        "order_visibility_timeout_sec",
        "cancel_confirm_timeout_sec",
        "max_total_duration_sec",
    ):
        if float(config.get(name, 0)) <= 0:
            raise RuntimeError("CONFIG.%s 必须大于 0" % name)
    if int(config.get("max_attempts", 0)) <= 0:
        raise RuntimeError("CONFIG.max_attempts 必须大于 0")
    # 0 = 关闭竞价排队报价; 负数无意义, 上限防手滑写成 20 而不是 0.20。
    auction_pct = float(config.get("auction_aggressive_pct", 0) or 0)
    if auction_pct < 0 or auction_pct > 0.1:
        raise RuntimeError("CONFIG.auction_aggressive_pct 必须在 0~0.1 之间 (0=关闭)")
    for name in ("limit_down_sell_mode", "limit_up_buy_mode"):
        if str(config.get(name, "")) not in ("", "skip", "queue", "none"):
            raise RuntimeError("CONFIG.%s 必须是空、skip、queue 或 none" % name)
    for name in ("queue_sell_deadline", "queue_buy_deadline"):
        value = str(config.get(name, ""))
        parts = value.split(":")
        if len(parts) != 3:
            raise RuntimeError("CONFIG.%s 必须是 HH:MM:SS" % name)
        try:
            [int(part) for part in parts]
        except ValueError:
            raise RuntimeError("CONFIG.%s 必须是 HH:MM:SS" % name)
    allowed = config.get("allowed_strategy_ids") or []
    if not isinstance(allowed, (list, tuple)):
        raise RuntimeError("CONFIG.allowed_strategy_ids 必须是列表")
    pct = float(config.get("max_single_position_pct", 0.2))
    if not (0.0 < pct <= 1.0):
        raise RuntimeError("CONFIG.max_single_position_pct 必须位于 (0,1]")
    half_mode = str(
        config.get("sell_half_insufficient_lot_mode", "sell_all") or "sell_all"
    ).strip().lower()
    if half_mode not in ("sell_all", "skip"):
        raise RuntimeError("CONFIG.sell_half_insufficient_lot_mode 必须是 sell_all 或 skip")
    if int(config.get("signal_expire_seconds", 600)) < 0:
        raise RuntimeError("CONFIG.signal_expire_seconds 不能为负数")


def init(ContextInfo):
    """大 QMT 策略初始化入口。"""
    global _RUNTIME
    validate_config(CONFIG)
    ContextInfo.set_account(CONFIG["account_id"])
    inbound_queue = queue_module.Queue()
    ack_queue = queue_module.Queue()
    worker = RedisStreamWorker(CONFIG, inbound_queue, ack_queue)
    runtime = BigQmtRuntime(CONFIG, worker)
    ContextInfo.run_time(
        "qmt_timer", "500nMilliSecond", "2000-01-01 00:00:00", "SH"
    )
    worker.start()
    _RUNTIME = runtime
    _log("INFO", "大QMT Redis执行端已启动，账户=%s" % _masked_account(CONFIG["account_id"]))


def handlebar(ContextInfo):
    """行情回调不推进订单；执行端统一由 500ms 定时器驱动。"""
    return None


def qmt_timer(ContextInfo):
    """500ms 定时器回调，驱动运行时状态机推进。"""
    if _RUNTIME is not None:
        _RUNTIME.on_timer(ContextInfo)


def order_callback(ContextInfo, orderInfo):
    """大 QMT 委托主推回调，转发给运行时记录订单状态。"""
    if _RUNTIME is not None:
        _RUNTIME.on_order(orderInfo)


def deal_callback(ContextInfo, dealInfo):
    """大 QMT 成交主推回调，转发给运行时做去重成交提示。"""
    if _RUNTIME is not None:
        _RUNTIME.on_deal(dealInfo)


def stop(ContextInfo):
    """停止 Redis 后台线程并清空运行时句柄。"""
    global _RUNTIME
    if _RUNTIME is not None:
        _RUNTIME.worker.stop()
        _RUNTIME = None
        _log("INFO", "大QMT Redis执行端已停止")


def _masked_account(account_id):
    value = str(account_id)
    if len(value) <= 4:
        return "****"
    return "****" + value[-4:]

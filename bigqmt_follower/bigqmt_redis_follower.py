# -*- coding: utf-8 -*-
"""大 QMT 内置策略：消费 Redis Stream 并执行股票交易信号。"""

from __future__ import print_function

import json
import hashlib
import datetime
import threading
import time
import uuid
from collections import deque
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR

try:
    import queue as queue_module
except ImportError:  # pragma: no cover - 兼容少量仍使用 Python 2 的旧 QMT 环境
    import Queue as queue_module


CONFIG = {
    "trading_enabled": False,
    "account_id": "",
    "account_type": "stock",
    "strategy_name": "bigqmt_redis_follower",
    "redis_host": "",
    "redis_port": 6379,
    "redis_password": "",
    "redis_stream": "tidal_quant_signals",
    "redis_group": "bigqmt_executors",
    "redis_block_ms": 500,
    "redis_reconnect_sec": 3.0,
    "pricing_mode": "book",
    "buy_slippage_pct": 0.003,
    "sell_slippage_pct": 0.003,
    "book_tick_offset": 2,
    "max_deviation_from_signal_price_pct": 0.02,
    "order_timeout_sec": 3.0,
    "order_visibility_timeout_sec": 3.0,
    "cancel_confirm_timeout_sec": 3.0,
    "max_attempts": 3,
    "max_total_duration_sec": 15.0,
}


class PriceDeviationError(ValueError):
    """实时定价相对聚宽参考价偏离过大。"""


def parse_stream_message(message_id, fields):
    """解析现有 Redis Stream entry，返回 trade 或 watchlist 消息。"""
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

    required = ("signal_id", "strategy_id", "action", "code", "amount")
    missing = [name for name in required if raw.get(name) in (None, "")]
    if missing:
        raise ValueError("trade signal missing fields: %s" % ", ".join(missing))
    if action not in ("buy", "sell"):
        raise ValueError("unsupported trade action: %s" % action)
    reference_price = raw.get("reference_price", raw.get("price"))
    if reference_price in (None, ""):
        raise ValueError("trade signal missing reference_price")
    amount = int(raw["amount"])
    if amount <= 0:
        raise ValueError("trade signal amount must be positive")
    signal = {
        "signal_id": str(raw["signal_id"]),
        "strategy_id": str(raw["strategy_id"]),
        "mode": str(raw.get("mode", "live")).lower(),
        "action": action,
        "code": str(raw["code"]),
        "amount": amount,
        "reference_price": float(reference_price),
        "created_at": str(raw.get("created_at", raw.get("timestamp", ""))),
        "sent_at_ms": raw.get("sent_at_ms"),
    }
    return {"kind": "trade", "message_id": str(message_id), "signal": signal}


def jq_code_to_qmt_code(code):
    """聚宽代码转换为大 QMT 代码。"""
    if code.endswith(".XSHG"):
        return code[:-5] + ".SH"
    if code.endswith(".XSHE"):
        return code[:-5] + ".SZ"
    return code


def _normalize_qmt_instrument(instrument_id, exchange_id=""):
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


def _first_positive(values):
    if not values:
        return None
    try:
        value = float(values[0])
    except (TypeError, ValueError, IndexError):
        return None
    return value if value > 0 else None


def _round_to_tick(value, tick, action):
    value_decimal = Decimal(str(value))
    tick_decimal = Decimal(str(tick))
    rounding = ROUND_CEILING if action == "buy" else ROUND_FLOOR
    ticks = (value_decimal / tick_decimal).to_integral_value(rounding=rounding)
    return float(ticks * tick_decimal)


def calculate_order_price(signal, tick, config):
    """按盘口或滑点定价，并执行参考价偏离保护。"""
    action = signal["action"]
    last_price = float(tick.get("lastPrice") or 0)
    if last_price <= 0:
        raise ValueError("latest price is unavailable")
    pricing_mode = str(config.get("pricing_mode", "book")).lower()
    tick_size = tick_size_for(signal["code"])
    pricing_base = None

    if pricing_mode == "book":
        if action == "buy":
            pricing_base = _first_positive(tick.get("askPrice"))
        else:
            pricing_base = _first_positive(tick.get("bidPrice"))

    if pricing_base is None:
        pricing_base = last_price
        if action == "buy":
            raw_price = pricing_base * (1.0 + float(config["buy_slippage_pct"]))
        else:
            raw_price = pricing_base * (1.0 - float(config["sell_slippage_pct"]))
    else:
        offset = int(config.get("book_tick_offset", 0)) * tick_size
        raw_price = pricing_base + offset if action == "buy" else pricing_base - offset

    reference_price = float(signal["reference_price"])
    if reference_price <= 0:
        raise ValueError("reference price must be positive")
    deviation = abs(pricing_base - reference_price) / reference_price
    limit = float(config["max_deviation_from_signal_price_pct"])
    if deviation > limit:
        raise PriceDeviationError(
            "pricing base %.6f deviates %.2f%% from reference %.6f"
            % (pricing_base, deviation * 100.0, reference_price)
        )
    return _round_to_tick(raw_price, tick_size, action)


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
        if self._thread is not None and self._thread.is_alive():
            return
        self._thread = threading.Thread(target=self.run, name="bigqmt-redis")
        self._thread.daemon = True
        self._thread.start()

    def stop(self):
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


class BigQmtGateway(object):
    """把大 QMT 全局交易函数收口为可注入、可测试的接口。"""

    def __init__(self, context, config):
        self.context = context
        self.config = config

    def latest_tick(self, code):
        qmt_code = jq_code_to_qmt_code(code)
        ticks = self.context.get_full_tick([qmt_code])
        tick = ticks.get(qmt_code)
        if not tick:
            raise RuntimeError("无法取得行情: %s" % qmt_code)
        return tick

    def available_cash(self):
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

    def list_orders(self):
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
        return bool(
            can_cancel_order(
                order_id, self.config["account_id"], self.config["account_type"]
            )
        )

    def cancel(self, order_id):
        return bool(
            cancel(
                order_id,
                self.config["account_id"],
                self.config["account_type"],
                self.context,
            )
        )


class BigQmtRuntime(object):
    """单次大 QMT 运行期内的 FIFO 内存执行状态机。"""

    def __init__(self, config, worker, gateway_factory=None, clock=None):
        self.config = config
        self.worker = worker
        self.gateway_factory = gateway_factory or BigQmtGateway
        self.clock = clock or time.time
        self.gateway = None
        self.pending = deque()
        self.active = None
        self.seen_signal_ids = set()
        self.universe = set()
        self.halted = False
        self.halt_reason = ""

    @property
    def pending_count(self):
        return len(self.pending)

    def on_timer(self, context):
        if self.gateway is None:
            self.gateway = self.gateway_factory(context, self.config)
        self._drain_inbound(context)
        if self.halted:
            return
        if self.active is None and self.pending:
            message = self.pending.popleft()
            self.active = {
                "message": message,
                "signal": message["signal"],
                "requested_qty": int(message["signal"]["amount"]),
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
            }
            self.active["total_deadline"] = self._deadline_after_auction(
                self.active["started_at"], float(self.config["max_total_duration_sec"])
            )
            self._submit_attempt()
            return
        if self.active is not None:
            self._refresh_active_order()
            if not self.halted:
                self._advance_active()

    def _drain_inbound(self, context):
        while True:
            try:
                message = self.worker.inbound_queue.get_nowait()
            except queue_module.Empty:
                return
            if message["kind"] == "watchlist":
                self.universe.update(jq_code_to_qmt_code(code) for code in message["codes"])
                context.set_universe(sorted(self.universe))
                self.worker.ack_queue.put(message["message_id"])
                continue
            signal = message["signal"]
            if signal.get("mode") != "live":
                self.worker.ack_queue.put(message["message_id"])
                continue
            signal_id = signal["signal_id"]
            if signal_id in self.seen_signal_ids:
                self.worker.ack_queue.put(message["message_id"])
                continue
            self.seen_signal_ids.add(signal_id)
            self.pending.append(message)

    def _submit_attempt(self):
        active = self.active
        signal = active["signal"]
        remaining = active["requested_qty"] - active["filled_qty"]
        if remaining <= 0:
            self._finish_active("FILLED")
            return
        try:
            tick = self.gateway.latest_tick(signal["code"])
            price = calculate_order_price(signal, tick, self.config)
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
                self._finish_active("FAILED_RESOURCE")
                return
        except PriceDeviationError as exc:
            _log("ERROR", "价格偏离保护拒绝信号: %s" % exc)
            self._finish_active("FAILED_PRICE")
            return
        except Exception as exc:
            _log("ERROR", "行情或资源查询失败，信号不下单: %s" % exc)
            self._finish_active("FAILED_DATA")
            return

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
            }
        )
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
                jq_code_to_qmt_code(signal["code"]),
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
        if self.active is None:
            return
        remark = str(getattr(deal_info, "m_strRemark", ""))
        if remark != self.active.get("remark"):
            return
        trade_id = str(getattr(deal_info, "m_strTradeID", ""))
        if not trade_id or trade_id in self.active["deal_ids"]:
            return
        self.active["deal_ids"].add(trade_id)
        volume = int(getattr(deal_info, "m_nVolume", 0) or 0)
        self.active["deal_filled_hint"] = self.active.get("deal_filled_hint", 0) + volume

    def _refresh_active_order(self):
        try:
            orders = self.gateway.list_orders()
        except Exception as exc:
            self._halt("查询活动委托失败，状态不确定: %s" % exc)
            return
        for order in orders:
            if str(getattr(order, "m_strRemark", "")) == self.active.get("remark"):
                self._apply_order(order)
                return

    def _apply_order(self, order_info):
        if self.active is None:
            return
        remark = str(getattr(order_info, "m_strRemark", ""))
        if remark != self.active.get("remark"):
            return
        order_id = str(getattr(order_info, "m_strOrderSysID", "") or "")
        status = int(getattr(order_info, "m_nOrderStatus", 255))
        traded = int(getattr(order_info, "m_nVolumeTraded", 0) or 0)
        if traded < 0:
            traded = 0
        if order_id:
            self.active["order_id"] = order_id
        self.active["order_status"] = status
        self.active["order_class"] = classify_order_status(status)
        self.active["attempt_filled"] = max(self.active.get("attempt_filled", 0), traded)

    def _advance_active(self):
        active = self.active
        now = self.clock()
        order_class = active.get("order_class")

        if active["state"] == "WAITING_ORDER_ID":
            if active.get("order_id"):
                if order_class == "OPEN":
                    active["state"] = "WORKING"
                else:
                    self._handle_terminal_attempt(order_class)
                    return
            elif now >= active["visibility_deadline"]:
                self._halt("passorder 后未找到对应委托，无法确认是否已提交")
                return

        if active["state"] == "WORKING":
            if order_class in ("CANCELED", "FILLED", "REJECTED"):
                self._handle_terminal_attempt(order_class)
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
                self._handle_terminal_attempt(order_class)
                return
            if now - active["cancel_requested_at"] >= float(
                self.config["cancel_confirm_timeout_sec"]
            ):
                self._halt("撤单终态等待超时: %s" % active.get("order_id"))

    def _handle_terminal_attempt(self, order_class):
        active = self.active
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
        remaining = max(active["requested_qty"] - active["filled_qty"], 0)
        _log(
            "INFO",
            "委托终态 status=%s 本次成交=%s 累计=%s 剩余=%s"
            % (order_class, active.get("attempt_filled", 0), active["filled_qty"], remaining),
        )
        if remaining == 0:
            self._finish_active("FILLED")
            return
        if active["attempt"] >= int(self.config["max_attempts"]):
            self._finish_active("PARTIAL_FINAL" if active["filled_qty"] else "FAILED_BROKER")
            return
        if self.clock() >= active["total_deadline"]:
            self._finish_active("PARTIAL_FINAL" if active["filled_qty"] else "FAILED_TIMEOUT")
            return
        active["state"] = "RETRYING"
        self._submit_attempt()

    def _order_deadline(self, submitted_at):
        return self._deadline_after_auction(
            submitted_at, float(self.config["order_timeout_sec"])
        )

    def _deadline_after_auction(self, started_at, duration):
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

    def _finish_active(self, status):
        if self.active is None:
            return
        message_id = self.active["message"]["message_id"]
        _log("INFO", "信号终态 %s status=%s" % (self.active["signal"]["signal_id"], status))
        self.worker.ack_queue.put(message_id)
        self.active = None

    def _halt(self, reason):
        self.halted = True
        self.halt_reason = reason
        if self.active is not None:
            self.active["state"] = "HALTED"
        _log("ERROR", "交易执行端熔断: %s" % reason)


_RUNTIME = None


def validate_config(config):
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
    if _RUNTIME is not None:
        _RUNTIME.on_timer(ContextInfo)


def order_callback(ContextInfo, orderInfo):
    if _RUNTIME is not None:
        _RUNTIME.on_order(orderInfo)


def deal_callback(ContextInfo, dealInfo):
    if _RUNTIME is not None:
        _RUNTIME.on_deal(dealInfo)


def stop(ContextInfo):
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

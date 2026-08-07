from __future__ import annotations

import datetime as dt
import logging
import threading
import time
from dataclasses import dataclass
from typing import Any

from miniqmt_follower.config import TradingConfig
from miniqmt_follower.models import (
    Action,
    BrokerOrderRejected,
    BrokerOrderStatus,
    BrokerRejectionKind,
    BrokerSubmissionUncertain,
    OrderSnapshot,
    Quote,
    TradeSignal,
)

logger = logging.getLogger(__name__)

# 订单列表缓存有效期（秒）—— 短缓存避免重复 query_stock_orders 全量扫描
_ORDERS_CACHE_TTL = 0.05

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

_ORDER_ERROR_WAIT_SEC = 0.05

# 撤单复查用: 到达这些状态就说明"撤单"这件事已经没有意义了。
_TERMINAL_ORDER_STATUSES = frozenset(
    {
        BrokerOrderStatus.FILLED,
        BrokerOrderStatus.CANCELED,
        BrokerOrderStatus.REJECTED,
    }
)


@dataclass(frozen=True)
class _QmtOrderError:
    order_id: str | None
    error_code: str | None
    reason: str
    order_remark: str | None


class QmtAdapterNotConfigured(RuntimeError):
    """QMT 运行环境或账号交易适配尚未配置。"""

    pass


def classify_qmt_rejection(reason: str | None) -> BrokerRejectionKind:
    """把 QMT/柜台废单文案归一化；未识别的已确认废单默认允许重试。"""
    normalized = str(reason or "").strip().lower()
    for keyword in _HARD_STOP_REJECTION_KEYWORDS:
        if keyword in normalized:
            return BrokerRejectionKind.HARD_STOP
    for keyword in _PRICE_REJECTION_KEYWORDS:
        if keyword in normalized:
            return BrokerRejectionKind.PRICE
    for keyword in _RESOURCE_REJECTION_KEYWORDS:
        if keyword in normalized:
            return BrokerRejectionKind.RESOURCE
    for keyword in _TRANSIENT_REJECTION_KEYWORDS:
        if keyword in normalized:
            return BrokerRejectionKind.TRANSIENT
    return BrokerRejectionKind.UNKNOWN


# ---------------------------------------------------------------------------
# Code mapping: JoinQuant ↔ miniQMT
# ---------------------------------------------------------------------------

def jq_code_to_qmt_code(code: str) -> str:
    """将聚宽格式 (000001.XSHE / 510300.XSHG) 转为 QMT 格式 (000001.SZ / 510300.SH)。"""
    if code.endswith(".XSHG"):
        return code.replace(".XSHG", ".SH")
    if code.endswith(".XSHE"):
        return code.replace(".XSHE", ".SZ")
    # Already QMT format or unknown — return as-is
    return code


# ---------------------------------------------------------------------------
# Order status mapping: xtquant → internal BrokerOrderStatus
# ---------------------------------------------------------------------------

def _qmt_order_status_to_broker_status(order_status: int) -> BrokerOrderStatus:
    """将 QMT order_status 常量映射为内部 BrokerOrderStatus。"""
    try:
        from xtquant.xtconstant import (
            ORDER_CANCELED,
            ORDER_JUNK,
            ORDER_PARTSUCC_CANCEL,
            ORDER_PART_CANCEL,
            ORDER_PART_SUCC,
            ORDER_REPORTED,
            ORDER_REPORTED_CANCEL,
            ORDER_SUCCEEDED,
            ORDER_UNKNOWN,
            ORDER_UNREPORTED,
            ORDER_WAIT_REPORTING,
        )
    except ImportError as exc:
        raise QmtAdapterNotConfigured("xtquant is not installed in this Python environment") from exc

    mapping: dict[int, BrokerOrderStatus] = {
        # 未成交、待报、已报 → OPEN (还在市场里等待成交)
        ORDER_UNREPORTED: BrokerOrderStatus.OPEN,
        ORDER_WAIT_REPORTING: BrokerOrderStatus.OPEN,
        ORDER_REPORTED: BrokerOrderStatus.OPEN,
        # 部分成交、部成待撤 → PARTIALLY_FILLED（待撤仍非终态）
        ORDER_PART_SUCC: BrokerOrderStatus.PARTIALLY_FILLED,
        ORDER_PARTSUCC_CANCEL: BrokerOrderStatus.PARTIALLY_FILLED,
        # 完全成交
        ORDER_SUCCEEDED: BrokerOrderStatus.FILLED,
        # 已报待撤仍在等待撤单确认
        ORDER_REPORTED_CANCEL: BrokerOrderStatus.OPEN,
        # 部撤、已撤才是撤单终态
        ORDER_CANCELED: BrokerOrderStatus.CANCELED,
        ORDER_PART_CANCEL: BrokerOrderStatus.CANCELED,
        # 废单
        ORDER_JUNK: BrokerOrderStatus.REJECTED,
        # 未知 → 保守当 OPEN 处理, 让超时逻辑兜底
        ORDER_UNKNOWN: BrokerOrderStatus.OPEN,
    }
    return mapping.get(order_status, BrokerOrderStatus.OPEN)


# ---------------------------------------------------------------------------
# Market data adapter
# ---------------------------------------------------------------------------

class QmtMarketDataAdapter:
    """miniQMT/xtquant 行情适配器。

    这里只做行情快照查询。真实下单逻辑放在 QmtBrokerAdapter, 两者分开便于测试。

    订阅说明: 未订阅时 get_full_tick 可能实时向行情服务器请求, 单次几十到上百毫秒;
    订阅后读本地内存, 亚毫秒级。因此:
    - 启动时对配置的股票池 pre_subscribe_codes 预订阅;
    - 收到未订阅代码时先订阅再取价(当次不省时间, 重挂和后续信号受益)。
    """

    def __init__(self, pre_subscribe_codes: tuple[str, ...] = ()):
        try:
            from xtquant import xtdata
        except ImportError as exc:
            raise QmtAdapterNotConfigured("xtquant is not installed in this Python environment") from exc
        self.xtdata = xtdata
        self._subscribed: set[str] = set()
        self._subscribe_lock = threading.Lock()
        # 静态合约信息缓存: 当日不变, 每代码每天只查一次 get_instrument_detail。
        # 涨跌停价和证券中文名都从这里取, 避免同一只票一天重复请求。
        self._instrument_detail_cache: dict[str, dict[str, Any]] = {}
        self._instrument_detail_cache_day: str = ""
        if pre_subscribe_codes:
            self.subscribe(pre_subscribe_codes)
            logger.info("【行情】📡 启动预订阅完成 | %s只", len(self._subscribed))

    def subscribe(self, codes) -> None:
        """批量订阅聚宽格式代码的 tick 行情。策略盘前推送 watchlist 时调用。"""
        for code in codes:
            self._ensure_subscribed(jq_code_to_qmt_code(str(code)))

    def _ensure_subscribed(self, qmt_code: str) -> None:
        """对代码做一次 tick 订阅; 失败只告警不阻塞, get_full_tick 仍可兜底取价。"""
        if qmt_code in self._subscribed:
            return
        with self._subscribe_lock:
            if qmt_code in self._subscribed:
                return
            try:
                self.xtdata.subscribe_quote(qmt_code, period="tick")
                logger.debug("📡 已订阅行情 | code=%s", qmt_code)
            except Exception as exc:
                logger.warning("【行情】⚠️ %s | 订阅失败，改用实时请求 | %s", qmt_code, exc)
            # 失败也记入集合, 避免每次取价都重试订阅拖慢热路径。
            self._subscribed.add(qmt_code)

    def latest_quote(self, code: str) -> Quote:
        # get_full_tick 返回以证券代码为 key 的 tick 字典。
        qmt_code = jq_code_to_qmt_code(code)
        self._ensure_subscribed(qmt_code)
        ticks = self.xtdata.get_full_tick([qmt_code])
        tick = ticks.get(qmt_code)
        if not tick:
            raise RuntimeError(f"no tick data for {code} (qmt: {qmt_code})")
        last_price = float(tick.get("lastPrice") or tick.get("last_price"))
        high_limit, low_limit = self._limit_prices_of(qmt_code)
        return Quote(
            last_price=last_price,
            ask1=_first_book_level(tick.get("askPrice")),
            bid1=_first_book_level(tick.get("bidPrice")),
            high_limit=high_limit,
            low_limit=low_limit,
        )

    def latest_price(self, code: str) -> float:
        return self.latest_quote(code).last_price

    def instrument_name(self, code: str) -> str | None:
        """返回证券中文名; 合约信息不可得时返回 None, 日志回退为只显示代码。"""
        qmt_code = jq_code_to_qmt_code(code)
        detail = self._instrument_detail_of(qmt_code)
        if detail is None:
            return None
        name = detail.get("InstrumentName") or detail.get("instrument_name")
        return str(name).strip() if name else None

    def _instrument_detail_of(self, qmt_code: str) -> dict[str, Any] | None:
        """取静态合约信息并按代码+交易日缓存; 查询失败不缓存, 下次调用重试。"""
        today = dt.date.today().isoformat()
        if today != self._instrument_detail_cache_day:
            self._instrument_detail_cache = {}
            self._instrument_detail_cache_day = today
        cached = self._instrument_detail_cache.get(qmt_code)
        if cached is not None:
            return cached
        try:
            detail = self.xtdata.get_instrument_detail(qmt_code)
        except Exception as exc:
            logger.warning("【行情】⚠️ %s | 合约静态信息查询失败 | %s", qmt_code, exc)
            return None
        if not detail:
            logger.warning("【行情】⚠️ %s | 合约静态信息为空", qmt_code)
            return None
        self._instrument_detail_cache[qmt_code] = detail
        return detail

    def _limit_prices_of(self, qmt_code: str) -> tuple[float | None, float | None]:
        """从静态合约信息取当日涨跌停价 (UpStopPrice/DownStopPrice)。

        QMT tick 不含涨跌停价, 只能从静态合约信息 get_instrument_detail 取。
        查询失败不缓存 (下次调用重试) 并返回 (None, None) —— 依赖方 (跌停排队
        卖出) 会因此回退保守路径, 宁可跳过也不用错误价格挂单。
        """
        detail = self._instrument_detail_of(qmt_code)
        if detail is None:
            return None, None
        return (
            _positive_price_or_none(detail.get("UpStopPrice")),
            _positive_price_or_none(detail.get("DownStopPrice")),
        )


def _positive_price_or_none(value: Any) -> float | None:
    """静态信息里的涨跌停价字段转 float; 0/负数/缺失/非法都返回 None。"""
    try:
        price = float(value)
    except (TypeError, ValueError):
        return None
    return price if price > 0 else None


def _first_book_level(levels: Any) -> float | None:
    """从 tick 的五档数组里取第一档价格; 0 或缺失(涨跌停单边无档)返回 None。"""
    if not levels:
        return None
    try:
        first = float(levels[0])
    except (TypeError, ValueError, IndexError):
        return None
    return first if first > 0 else None


# ---------------------------------------------------------------------------
# Broker adapter
# ---------------------------------------------------------------------------

class QmtBrokerAdapter:
    """miniQMT 交易适配器。

    实现 executor.py 中的 BrokerAdapter 协议: submit_order / get_order_snapshot / cancel_order。
    """

    def __init__(self, config: TradingConfig):
        if not config.enabled:
            raise QmtAdapterNotConfigured(
                "trading.enabled is false. Set it to true in config.yaml to enable live trading."
            )
        if not config.account_id:
            raise QmtAdapterNotConfigured("trading.account_id is empty")
        if not config.miniqmt_path:
            raise QmtAdapterNotConfigured("trading.miniqmt_path is empty")

        self._account_id: str = config.account_id
        self._strategy_name: str = config.strategy_name

        # ---- 线程安全锁: 保护 QMT 交易 API 调用 ----
        self._trading_lock = threading.Lock()

        # ---- 订单缓存: 减少轮询时重复 QMT API 调用 ----
        # 多个 worker 线程会并发读写它, 必须有锁: 否则 submit_order 的预填可能
        # 被并发的全量刷新整体替换掉, 而且缓存过期瞬间多个线程会同时发起
        # query_stock_orders 全量查询(单飞由本锁保证)。
        self._orders_cache: dict[str, OrderSnapshot] = {}  # order_id → snapshot
        self._orders_cache_time: float = 0.0
        self._orders_cache_lock = threading.RLock()
        # ---- 报单失败回调缓存: 与交易 API 锁分离，避免阻塞 QMT 回调线程 ----
        self._order_error_lock = threading.Lock()
        self._order_errors_by_remark: dict[str, _QmtOrderError] = {}
        self._order_errors_by_id: dict[str, _QmtOrderError] = {}

        logger.debug("🔌 正在连接QMT交易端 | 账号=%s 路径=%s 会话=%s",
                     config.account_id, config.miniqmt_path, config.session_id)

        try:
            from xtquant.xtconstant import FIX_PRICE, STOCK_BUY, STOCK_SELL
            from xtquant.xttrader import XtQuantTrader, XtQuantTraderCallback
            from xtquant.xttype import StockAccount
        except ImportError as exc:
            raise QmtAdapterNotConfigured("xtquant is not installed in this Python environment") from exc

        self._STOCK_BUY: int = STOCK_BUY
        self._STOCK_SELL: int = STOCK_SELL
        self._FIX_PRICE: int = FIX_PRICE

        self._account = StockAccount(config.account_id)
        self._trader = XtQuantTrader(config.miniqmt_path, config.session_id)

        adapter = self

        class _OrderErrorCallback(XtQuantTraderCallback):
            def on_order_error(self, error):
                adapter._cache_order_error(error)

        # 保留强引用，避免回调对象被回收。
        self._callback = _OrderErrorCallback()
        self._trader.register_callback(self._callback)
        self._trader.start()

        connect_result = self._trader.connect()
        if connect_result != 0:
            self._trader.stop()
            raise QmtAdapterNotConfigured(
                f"QMT connect failed with code {connect_result}. "
                f"Check that miniQMT is running and the path is correct: {config.miniqmt_path}"
            )
        logger.info("【QMT】🔌 交易端已连接 | 返回码 %s", connect_result)

        subscribe_result: int = self._trader.subscribe(self._account)
        if subscribe_result != 0:
            self._trader.stop()
            raise QmtAdapterNotConfigured(
                f"QMT subscribe failed with code {subscribe_result} for account {config.account_id}"
            )
        logger.debug("✅ 已订阅账户 | 账号=%s 返回码=%s", config.account_id, subscribe_result)

    # ---- BrokerAdapter protocol -------------------------------------------

    def query_available_cash(self) -> float:
        """查询账户当前可用资金。"""
        with self._trading_lock:
            asset = self._trader.query_stock_asset(self._account)
        # xtquant asset 对象的常见属性名, 按优先级尝试
        # xtquant 的规范字段是 cash，放最前；其余为兼容旧版本/大 QMT 命名。
        for attr in ("cash", "m_dAvailable", "available_cash", "m_dBalance"):
            val = getattr(asset, attr, None)
            if val is not None:
                return float(val)
        raise RuntimeError(f"Cannot extract available cash from asset object: {asset}")

    def query_available_position(self, code: str) -> int:
        """实时查询某只股票的可用持仓（可卖数量）。"""
        qmt_code = jq_code_to_qmt_code(code)
        with self._trading_lock:
            positions: list[Any] = self._trader.query_stock_positions(self._account)
        for pos in positions:
            pos_code = str(getattr(pos, "stock_code", ""))
            if pos_code != qmt_code:
                continue
            for attr in ("can_use_volume", "m_nCanUseVolume", "volume"):
                val = getattr(pos, attr, None)
                if val is not None:
                    return int(val)
            return 0
        return 0

    def query_position(self, code: str) -> int:
        """实时查询某只股票的总持仓（含当日不可卖部分）。"""
        qmt_code = jq_code_to_qmt_code(code)
        with self._trading_lock:
            positions: list[Any] = self._trader.query_stock_positions(self._account)
        for pos in positions:
            pos_code = str(getattr(pos, "stock_code", ""))
            if pos_code != qmt_code:
                continue
            for attr in ("volume", "m_nVolume", "can_use_volume"):
                val = getattr(pos, attr, None)
                if val is not None:
                    return int(val)
            return 0
        return 0

    def query_total_assets(self) -> float:
        """查询账户当前总资产（现金 + 股票市值）。

        xtquant 的 XtAsset 字段是 account_id / cash / frozen_cash /
        market_value / **total_asset**（单数）。此前候选列表里只有大 QMT
        get_trade_detail_data 风格的 m_dTotalAssets 和一个拼错的复数
        total_assets，三个候选全部落空 → 抛 RuntimeError → 每条 auto_buy 都落
        FAILED_BROKER，实盘一股买不进。total_asset 放在最前面。
        """
        with self._trading_lock:
            asset = self._trader.query_stock_asset(self._account)
        for attr in ("total_asset", "m_dTotalAssets", "total_assets", "m_dBalance"):
            val = getattr(asset, attr, None)
            if val is not None:
                return float(val)
        # 兜底：市值 + 可用现金也能拼出总资产，好过直接让买单全废。
        market_value = getattr(asset, "market_value", None)
        cash = getattr(asset, "cash", None)
        if market_value is not None and cash is not None:
            logger.warning(
                "【QMT】⚠️ 未找到总资产字段 | 退化为 market_value + cash 估算",
            )
            return float(market_value) + float(cash)
        raise RuntimeError(
            f"Cannot extract total assets from asset object: {asset} "
            f"(available attrs: {[a for a in dir(asset) if not a.startswith('_')]})"
        )

    def submit_order(self, signal: TradeSignal, quantity: int, price: float) -> str:
        qmt_code = jq_code_to_qmt_code(signal.code)
        order_type = self._STOCK_BUY if signal.action == Action.BUY else self._STOCK_SELL

        try:
            with self._trading_lock:
                order_id = self._trader.order_stock(
                    self._account,
                    qmt_code,
                    order_type,
                    int(quantity),
                    self._FIX_PRICE,
                    float(price),
                    self._strategy_name,
                    signal.signal_id,  # order_remark — 方便在 QMT 客户端追溯到信号
                )
        except Exception as exc:
            raise BrokerSubmissionUncertain(
                f"QMT order submission state is uncertain: {exc}"
            ) from exc

        if order_id is None or (isinstance(order_id, int) and order_id < 0):
            rejection = self._take_order_error_by_remark(
                signal.signal_id, wait_timeout=_ORDER_ERROR_WAIT_SEC,
            )
            reason = (
                rejection.reason
                if rejection is not None
                else f"QMT order_stock failed: returned {order_id}"
            )
            raise BrokerOrderRejected(
                reason,
                error_code=(rejection.error_code if rejection is not None else str(order_id)),
                kind=classify_qmt_rejection(reason),
            )

        oid = str(order_id)
        # 预填缓存: 新订单初始为 OPEN
        with self._orders_cache_lock:
            self._orders_cache[oid] = OrderSnapshot(
                order_id=oid, status=BrokerOrderStatus.OPEN, filled_qty=0,
            )

        logger.debug(
            "📤 委托已提交 | QMT单号=%s 信号=%s 代码=%s 数量=%s 价格=%.3f",
            order_id,
            signal.label,
            qmt_code,
            quantity,
            price,
        )
        return oid

    def _cache_order_error(self, error: Any) -> None:
        """QMT 回调线程只写轻量内存缓存，不触达交易 API 或 SQLite。"""
        raw_order_id = getattr(error, "order_id", None)
        raw_error_code = getattr(error, "error_id", None)
        raw_reason = getattr(error, "error_msg", None)
        raw_remark = getattr(error, "order_remark", None)
        cached = _QmtOrderError(
            order_id=(str(raw_order_id) if raw_order_id is not None else None),
            error_code=(str(raw_error_code) if raw_error_code is not None else None),
            reason=str(raw_reason or "QMT order rejected"),
            order_remark=(str(raw_remark) if raw_remark else None),
        )
        with self._order_error_lock:
            if cached.order_id is not None:
                self._order_errors_by_id[cached.order_id] = cached
            if cached.order_remark is not None:
                self._order_errors_by_remark[cached.order_remark] = cached

    def _take_order_error_by_remark(
        self,
        order_remark: str,
        *,
        wait_timeout: float,
    ) -> _QmtOrderError | None:
        """短暂等待异步错误回调，并按 signal_id/order_remark 原子消费。"""
        deadline = time.monotonic() + max(0.0, wait_timeout)
        while True:
            with self._order_error_lock:
                cached = self._order_errors_by_remark.pop(order_remark, None)
                if cached is not None:
                    if cached.order_id is not None:
                        self._order_errors_by_id.pop(cached.order_id, None)
                    return cached
            if time.monotonic() >= deadline:
                return None
            time.sleep(min(0.005, max(0.0, deadline - time.monotonic())))

    def _take_order_error_by_id(self, order_id: str) -> _QmtOrderError | None:
        """按 QMT 订单号消费回调错误，并清理同一记录的 remark 索引。"""
        with self._order_error_lock:
            cached = self._order_errors_by_id.pop(order_id, None)
            if cached is not None and cached.order_remark is not None:
                self._order_errors_by_remark.pop(cached.order_remark, None)
            return cached

    def get_order_snapshot(self, order_id: str) -> OrderSnapshot:
        """查询单个订单快照，带 50ms 短期缓存避免轮询时重复全量查询。"""
        with self._orders_cache_lock:
            now = time.monotonic()
            # 缓存过期才刷新全量订单列表。持锁刷新即天然单飞:
            # 多个 worker 同时发现过期时，只有一个真的去查 QMT。
            if now - self._orders_cache_time >= _ORDERS_CACHE_TTL:
                self._refresh_orders_cache()
                self._orders_cache_time = time.monotonic()

            cached = self._orders_cache.get(order_id)
            if cached is not None:
                return cached

        # 缓存没有（可能刚下单还没刷新），返回保守值
        return OrderSnapshot(order_id=order_id, status=BrokerOrderStatus.OPEN, filled_qty=0)

    def _refresh_orders_cache(self) -> None:
        """从 QMT 全量拉取订单列表并更新缓存。调用方需持有 _orders_cache_lock。"""
        with self._trading_lock:
            orders: list[Any] = self._trader.query_stock_orders(self._account)
        fresh: dict[str, OrderSnapshot] = {}
        for o in orders:
            oid = str(o.order_id)
            status = _qmt_order_status_to_broker_status(o.order_status)
            callback_error = (
                self._take_order_error_by_id(oid)
                if status == BrokerOrderStatus.REJECTED
                else None
            )
            raw_status_msg = str(getattr(o, "status_msg", "") or "").strip()
            rejection_reason = None
            rejection_code = None
            if status == BrokerOrderStatus.REJECTED:
                rejection_reason = raw_status_msg or (
                    callback_error.reason if callback_error is not None else "QMT order rejected"
                )
                rejection_code = (
                    callback_error.error_code if callback_error is not None else None
                )
            fresh[oid] = OrderSnapshot(
                order_id=oid,
                status=status,
                filled_qty=int(getattr(o, "traded_volume", 0)),
                rejection_reason=rejection_reason,
                rejection_code=rejection_code,
                rejection_kind=(
                    classify_qmt_rejection(rejection_reason)
                    if status == BrokerOrderStatus.REJECTED
                    else None
                ),
            )
        self._orders_cache = fresh

    def cancel_order(self, order_id: str) -> None:
        """提交撤单请求。

        关键: 返回码非 0 **不等于**出了事。最常见的情形是订单在"引擎判超时"和
        "撤单送达柜台"之间刚好成交或已被撤，柜台自然拒绝撤一笔终态单 —— 而
        order_timeout_sec 只有零点几秒时，这个竞态每天都会撞上好几次。

        执行引擎把 cancel_order 的任何异常都当作"撤单终态不明"并熔断当天全部
        交易（含后续所有止损、清仓），所以这里绝不能一看到非 0 就抛。正确做法是
        先向 QMT 复查该单的真实状态：已是终态就说明撤单目的已经达成，直接返回；
        只有"复查后仍非终态、撤单又失败"才是真的不确定，那时才抛。
        """
        with self._trading_lock:
            cancel_result: int = self._trader.cancel_order_stock(self._account, int(order_id))

        # 无论成功与否都让下一次查询穿透缓存，拿 QMT 的真实状态。
        self._invalidate_order_cache(order_id)

        if cancel_result != 0:
            snapshot = self.get_order_snapshot(order_id)
            if snapshot.status in _TERMINAL_ORDER_STATUSES:
                logger.info(
                    "【QMT】ℹ️ 撤单返回 %s，但复查确认订单已终态 | QMT单号=%s | 状态=%s | 成交=%s股",
                    cancel_result, order_id, snapshot.status.value, snapshot.filled_qty,
                )
                return
            raise RuntimeError(
                f"QMT cancel_order_stock failed for order {order_id}: "
                f"result={cancel_result}, and the order is still {snapshot.status.value}"
            )
        # QMT 返回 0 只表示撤单请求已受理，不代表订单已经终态。
        logger.debug("🔙 撤单请求已提交 | QMT单号=%s", order_id)

    def _invalidate_order_cache(self, order_id: str) -> None:
        with self._orders_cache_lock:
            self._orders_cache.pop(order_id, None)
            self._orders_cache_time = 0.0

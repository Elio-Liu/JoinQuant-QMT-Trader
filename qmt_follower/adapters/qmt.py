from __future__ import annotations

import logging
import threading
import time
from typing import Any

from qmt_follower.config import TradingConfig
from qmt_follower.models import (
    Action,
    BrokerOrderStatus,
    OrderSnapshot,
    Quote,
    TradeSignal,
)

logger = logging.getLogger(__name__)

# 订单列表缓存有效期（秒）—— 短缓存避免重复 query_stock_orders 全量扫描
_ORDERS_CACHE_TTL = 0.05


class QmtAdapterNotConfigured(RuntimeError):
    """QMT 运行环境或账号交易适配尚未配置。"""

    pass


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
        return Quote(
            last_price=last_price,
            ask1=_first_book_level(tick.get("askPrice")),
            bid1=_first_book_level(tick.get("bidPrice")),
        )

    def latest_price(self, code: str) -> float:
        return self.latest_quote(code).last_price


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
        self._orders_cache: dict[str, OrderSnapshot] = {}  # order_id → snapshot
        self._orders_cache_time: float = 0.0

        logger.debug("🔌 正在连接QMT交易端 | 账号=%s 路径=%s 会话=%s",
                     config.account_id, config.miniqmt_path, config.session_id)

        try:
            from xtquant.xtconstant import FIX_PRICE, STOCK_BUY, STOCK_SELL
            from xtquant.xttrader import XtQuantTrader
            from xtquant.xttype import StockAccount
        except ImportError as exc:
            raise QmtAdapterNotConfigured("xtquant is not installed in this Python environment") from exc

        self._STOCK_BUY: int = STOCK_BUY
        self._STOCK_SELL: int = STOCK_SELL
        self._FIX_PRICE: int = FIX_PRICE

        self._account = StockAccount(config.account_id)
        self._trader = XtQuantTrader(config.miniqmt_path, config.session_id)
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
        for attr in ("m_dAvailable", "available_cash", "cash", "m_dBalance"):
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

    def submit_order(self, signal: TradeSignal, quantity: int, price: float) -> str:
        qmt_code = jq_code_to_qmt_code(signal.code)
        order_type = self._STOCK_BUY if signal.action == Action.BUY else self._STOCK_SELL

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

        if order_id is None or (isinstance(order_id, int) and order_id < 0):
            raise RuntimeError(f"QMT order_stock failed: returned {order_id}")

        oid = str(order_id)
        # 预填缓存: 新订单初始为 OPEN
        self._orders_cache[oid] = OrderSnapshot(order_id=oid, status=BrokerOrderStatus.OPEN, filled_qty=0)

        logger.debug(
            "📤 委托已提交 | QMT单号=%s 信号=%s 代码=%s 数量=%s 价格=%.3f",
            order_id,
            signal.label,
            qmt_code,
            quantity,
            price,
        )
        return oid

    def get_order_snapshot(self, order_id: str) -> OrderSnapshot:
        """查询单个订单快照，带 50ms 短期缓存避免轮询时重复全量查询。"""
        now = time.monotonic()

        # 缓存过期才刷新全量订单列表
        if now - self._orders_cache_time >= _ORDERS_CACHE_TTL:
            self._refresh_orders_cache()
            self._orders_cache_time = now

        cached = self._orders_cache.get(order_id)
        if cached is not None:
            return cached

        # 缓存没有（可能刚下单还没刷新），返回保守值
        return OrderSnapshot(order_id=order_id, status=BrokerOrderStatus.OPEN, filled_qty=0)

    def _refresh_orders_cache(self) -> None:
        """从 QMT 全量拉取订单列表并更新缓存。"""
        with self._trading_lock:
            orders: list[Any] = self._trader.query_stock_orders(self._account)
        fresh: dict[str, OrderSnapshot] = {}
        for o in orders:
            oid = str(o.order_id)
            fresh[oid] = OrderSnapshot(
                order_id=oid,
                status=_qmt_order_status_to_broker_status(o.order_status),
                filled_qty=int(getattr(o, "traded_volume", 0)),
            )
        self._orders_cache = fresh

    def cancel_order(self, order_id: str) -> None:
        with self._trading_lock:
            cancel_result: int = self._trader.cancel_order_stock(self._account, int(order_id))
        if cancel_result != 0:
            raise RuntimeError(
                f"QMT cancel_order_stock failed for order {order_id}: result={cancel_result}"
            )
        # QMT 返回 0 只表示撤单请求已受理，不代表订单已经终态。
        # 清掉旧快照，强制执行引擎下一次查询从 QMT 拉取最终状态和成交量。
        self._orders_cache.pop(order_id, None)
        self._orders_cache_time = 0.0
        logger.debug("🔙 撤单请求已提交 | QMT单号=%s", order_id)

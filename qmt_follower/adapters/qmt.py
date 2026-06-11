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
    TradeSignal,
)

logger = logging.getLogger(__name__)

# 持仓缓存有效期（秒）
_POSITION_CACHE_TTL = 5.0
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
        # 部分成交
        ORDER_PART_SUCC: BrokerOrderStatus.PARTIALLY_FILLED,
        # 完全成交
        ORDER_SUCCEEDED: BrokerOrderStatus.FILLED,
        # 已撤
        ORDER_CANCELED: BrokerOrderStatus.CANCELED,
        ORDER_PART_CANCEL: BrokerOrderStatus.CANCELED,
        ORDER_PARTSUCC_CANCEL: BrokerOrderStatus.CANCELED,
        ORDER_REPORTED_CANCEL: BrokerOrderStatus.CANCELED,
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

    这里只做最新价查询。真实下单逻辑放在 QmtBrokerAdapter, 两者分开便于测试。
    """

    def __init__(self):
        try:
            from xtquant import xtdata
        except ImportError as exc:
            raise QmtAdapterNotConfigured("xtquant is not installed in this Python environment") from exc
        self.xtdata = xtdata

    def latest_price(self, code: str) -> float:
        # get_full_tick 返回以证券代码为 key 的 tick 字典。
        # 先尝试 QMT 格式，如果失败再尝试原始 code。
        qmt_code = jq_code_to_qmt_code(code)
        ticks = self.xtdata.get_full_tick([qmt_code])
        tick = ticks.get(qmt_code)
        if not tick:
            raise RuntimeError(f"no tick data for {code} (qmt: {qmt_code})")
        return float(tick.get("lastPrice") or tick.get("last_price"))


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
                "trading.enabled is false. Set it to true in config.json to enable live trading."
            )
        if not config.account_id:
            raise QmtAdapterNotConfigured("trading.account_id is empty")
        if not config.miniqmt_path:
            raise QmtAdapterNotConfigured("trading.miniqmt_path is empty")

        self._account_id: str = config.account_id
        self._strategy_name: str = config.strategy_name

        # ---- 线程安全锁: 保护 QMT 交易 API 调用 ----
        self._trading_lock = threading.Lock()

        # ---- 缓存: 减少重复 QMT API 调用 ----
        self._positions_cache: dict[str, int] = {}       # code → available_qty
        self._positions_cache_time: float = 0.0
        self._orders_cache: dict[str, OrderSnapshot] = {}  # order_id → snapshot
        self._orders_cache_time: float = 0.0

        logger.info("🔌 正在连接QMT交易端 | 账号=%s 路径=%s 会话=%s",
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
        logger.info("✅ QMT交易端已连接 (返回码=%s)", connect_result)

        subscribe_result: int = self._trader.subscribe(self._account)
        if subscribe_result != 0:
            self._trader.stop()
            raise QmtAdapterNotConfigured(
                f"QMT subscribe failed with code {subscribe_result} for account {config.account_id}"
            )
        logger.info("✅ 已订阅账户 | 账号=%s 返回码=%s", config.account_id, subscribe_result)

    # ---- BrokerAdapter protocol -------------------------------------------

    def query_available_cash(self) -> float:
        """查询账户当前可用资金。"""
        asset = self._trader.query_stock_asset(self._account)
        # xtquant asset 对象的常见属性名, 按优先级尝试
        for attr in ("m_dAvailable", "available_cash", "cash", "m_dBalance"):
            val = getattr(asset, attr, None)
            if val is not None:
                return float(val)
        raise RuntimeError(f"Cannot extract available cash from asset object: {asset}")

    def query_available_position(self, code: str) -> int:
        """查询某只股票的可用持仓（可卖数量），带 5 秒缓存。"""
        now = time.monotonic()
        if now - self._positions_cache_time < _POSITION_CACHE_TTL:
            return self._positions_cache.get(code, 0)

        # 缓存过期，全量刷新
        qmt_code = jq_code_to_qmt_code(code)
        positions: list[Any] = self._trader.query_stock_positions(self._account)
        self._positions_cache.clear()
        target_qty = 0
        for pos in positions:
            pos_code = str(getattr(pos, "stock_code", ""))
            for attr in ("can_use_volume", "m_nCanUseVolume", "volume"):
                val = getattr(pos, attr, None)
                if val is not None:
                    qty = int(val)
                    self._positions_cache[pos_code] = qty
                    if pos_code == qmt_code:
                        target_qty = qty
                    break
            else:
                self._positions_cache[pos_code] = 0
        self._positions_cache_time = now
        return target_qty

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

        logger.info(
            "📤 委托已提交 | QMT单号=%s 信号=%s 代码=%s 方向=%s 数量=%s 价格=%.2f",
            order_id,
            signal.signal_id,
            qmt_code,
            signal.action.value,
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
        # 更新缓存: 撤单后立即标记
        self._orders_cache[order_id] = OrderSnapshot(
            order_id=order_id, status=BrokerOrderStatus.CANCELED, filled_qty=0,
        )
        logger.info("🔙 委托已撤销 | QMT单号=%s", order_id)

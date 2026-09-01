"""miniQMT/xtquant 行情与交易适配器。

封装 QMT 行情取数与下单/查单/撤单的底层调用，落地 executor.py 定义的
MarketDataAdapter 与 BrokerAdapter 协议；执行引擎与测试均通过协议接口
依赖本模块，不直接触碰 xtquant。

本模块约定:
- 行情 tick 与合约静态信息按"代码+交易日"缓存，静态信息查询单飞防重复请求；
- 订单缓存只登记本进程报出的订单，外部手工单回调一律忽略；
- 报单/成交/错误回调账本按日期翻转清空，防止长跑进程缓慢泄漏内存；
- 撤单只确认请求已受理，终态由后续查单判定；非 0 返回码先复查再决定是否抛错。
"""

from __future__ import annotations

import datetime as dt
import hashlib
import logging
import threading
import time
from dataclasses import dataclass, replace
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
    format_stock_label,
)
from miniqmt_follower.strategy_models import (
    AccountSnapshot,
    MarketSnapshot,
    PositionSnapshot,
)

logger = logging.getLogger(__name__)

# 订单列表缓存有效期（秒）—— 短缓存避免重复 query_stock_orders 全量扫描。
# 0.10s: 回调在实时喂单笔快照, 单笔新鲜度(见 _orders_updated_mono)先行拦截;
# 全局 TTL 只兜底"静默挂单没有回调"的成交观察, 放宽到 0.10s 把全量扫描频率
# 减半, 同时撤单路径靠 _invalidate_order_cache 强制穿透, 不牺牲撤单确认时效。
_ORDERS_CACHE_TTL = 0.10

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
_QMT_ORDER_REMARK_MAX_ASCII_BYTES = 24
# QMT 调用耗时日志的门控阈值: 正常毫秒级调用不再刷屏刷盘, 只有慢得可疑
# (≥20ms, 通常是抢交易锁排队)才落日志, 给锁竞争诊断留信号。
_SLOW_CALL_LOG_THRESHOLD_SEC = 0.02


def _log_slow_call(label: str, started: float) -> None:
    """QMT 调用耗时只在"慢得可疑"时落日志: 正常毫秒级调用不再刷屏刷盘。"""
    if not logger.isEnabledFor(logging.DEBUG):
        return
    elapsed = time.monotonic() - started
    if elapsed >= _SLOW_CALL_LOG_THRESHOLD_SEC:
        logger.debug("⏱️ %s | %.1fms", label, elapsed * 1000)

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


def _order_remark_for_signal_id(signal_id: str) -> str:
    """生成 miniQMT 不会截断的稳定委托备注。

    miniQMT 只保留 24 个英文字符。长 signal_id 如果直接写入，
    同一 plan 的多笔委托会被截成同一前缀，重启后无法安全核单。
    """
    raw = str(signal_id)
    try:
        encoded = raw.encode("ascii")
    except UnicodeEncodeError:
        encoded = b""
    if raw and len(encoded) <= _QMT_ORDER_REMARK_MAX_ASCII_BYTES:
        return raw
    digest = hashlib.sha256(raw.encode("utf-8")).hexdigest()
    return "tq" + digest[: _QMT_ORDER_REMARK_MAX_ASCII_BYTES - 2]


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
# 代码映射: JoinQuant ↔ miniQMT
# ---------------------------------------------------------------------------

def jq_code_to_qmt_code(code: str) -> str:
    """将聚宽格式 (000001.XSHE / 510300.XSHG) 转为 QMT 格式 (000001.SZ / 510300.SH)。"""
    if code.endswith(".XSHG"):
        return code.replace(".XSHG", ".SH")
    if code.endswith(".XSHE"):
        return code.replace(".XSHE", ".SZ")
    # 已是 QMT 格式或未知后缀 —— 原样返回
    return code


def qmt_code_to_jq_code(code: str) -> str:
    """将 QMT 证券代码转换为聚宽格式，供真实账户持仓快照使用。"""
    if code.endswith(".SH"):
        return code[:-3] + ".XSHG"
    if code.endswith(".SZ"):
        return code[:-3] + ".XSHE"
    return code


# ---------------------------------------------------------------------------
# 订单状态映射: xtquant → 内部 BrokerOrderStatus
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
# 行情适配器
# ---------------------------------------------------------------------------

class QmtMarketDataAdapter:
    """miniQMT/xtquant 行情适配器。

    这里只做行情快照查询。真实下单逻辑放在 QmtBrokerAdapter, 两者分开便于测试。

    订阅说明: 未订阅时 get_full_tick 可能实时向行情服务器请求, 单次几十到上百毫秒;
    订阅后读本地内存, 亚毫秒级。因此:
    - 启动时对配置的股票池 pre_subscribe_codes 预订阅;
    - 收到未订阅代码时先订阅再取价(当次不省时间, 重挂和后续信号受益);
    - 订阅同时挂接单票推送回调, 记录最近一帧行情到达时间 —— tick 自带的
      time 是"最后一笔成交时间", 不随盘口挂撤单刷新, 时效门控以到达时间为准
      (无推送记录时回退成交时间)。
    """

    def __init__(self, pre_subscribe_codes: tuple[str, ...] = ()):
        try:
            from xtquant import xtdata
        except ImportError as exc:
            raise QmtAdapterNotConfigured("xtquant is not installed in this Python environment") from exc
        self.xtdata = xtdata
        self._subscribed: set[str] = set()
        self._subscribe_retry_after: dict[str, float] = {}
        self._subscribe_lock = threading.Lock()
        # 合约静态信息查询单飞锁: 并发首触同一代码只发一次 QMT 查询。
        self._instrument_detail_lock = threading.Lock()
        # 静态合约信息缓存: 当日不变, 每代码每天只查一次 get_instrument_detail。
        # 涨跌停价和证券中文名都从这里取, 避免同一只票一天重复请求。
        self._instrument_detail_cache: dict[str, dict[str, Any]] = {}
        self._instrument_detail_cache_day: str = ""
        # 每票最近一帧行情推送的到达时间(墙钟): tick 自带的 time 是"最后一笔
        # 成交时间", 盘口随挂撤单更新但不刷新它 —— 用到达时间做时效门控才能
        # 反映数据新鲜度。xtquant 回调线程写, 取价线程读, 日期翻转清空。
        self._last_arrival_at: dict[str, dt.datetime] = {}
        self._last_arrival_lock = threading.Lock()
        self._last_arrival_day: str = dt.date.today().isoformat()
        if pre_subscribe_codes:
            self.subscribe(pre_subscribe_codes)
            logger.info("【行情】📡 启动预订阅完成 | %s只", len(self._subscribed))

    def subscribe(self, codes) -> None:
        """批量订阅聚宽格式代码的 tick 行情。策略盘前推送 watchlist 时调用。"""
        for code in codes:
            self._ensure_subscribed(jq_code_to_qmt_code(str(code)))

    def _push_arrival_callback_for(self, qmt_code: str):
        """构造单票推送回调: 只记录到达时间, 不解析推送内容。

        回调数据形状因 xtquant 版本而异(单票 dict 或按代码为 key 的 dict),
        但对新鲜度而言只需"这一帧到了", 形状无关紧要。
        """

        def on_push(_data):
            with self._last_arrival_lock:
                self._last_arrival_at[qmt_code] = dt.datetime.now()

        return on_push

    def _last_arrival(self, qmt_code: str) -> dt.datetime | None:
        """返回该票最近一帧推送的到达时间; 日期翻转时清空账本防内存泄漏。"""
        today = dt.date.today().isoformat()
        with self._last_arrival_lock:
            if self._last_arrival_day != today:
                self._last_arrival_at.clear()
                self._last_arrival_day = today
            return self._last_arrival_at.get(qmt_code)

    def _freshness_time(
        self, qmt_code: str, tick: dict[str, Any]
    ) -> dt.datetime | None:
        """行情快照的新鲜度时间: 取 max(最后一笔成交时间, 最近推送到达时间)。

        无推送记录(订阅失败或尚未收到任何帧)时回退成交时间, 与旧版一致。
        """
        tick_time = _quote_datetime(
            tick.get("time") or tick.get("timetag") or tick.get("stime")
        )
        arrival = self._last_arrival(qmt_code)
        if arrival is None:
            return tick_time
        if tick_time is None:
            return arrival
        return max(tick_time, arrival)

    def _ensure_subscribed(self, qmt_code: str) -> None:
        """对代码做一次 tick 订阅; 失败只告警不阻塞, get_full_tick 仍可兜底取价。"""
        if qmt_code in self._subscribed:
            return
        retry_after = getattr(self, "_subscribe_retry_after", {})
        if time.monotonic() < retry_after.get(qmt_code, 0):
            return
        with self._subscribe_lock:
            if qmt_code in self._subscribed:
                return
            if time.monotonic() < retry_after.get(qmt_code, 0):
                return
            try:
                self.xtdata.subscribe_quote(
                    qmt_code, period="tick",
                    callback=self._push_arrival_callback_for(qmt_code),
                )
                logger.debug(
                    "📡 已订阅行情 | %s",
                    format_stock_label(qmt_code, self.instrument_name(qmt_code)),
                )
            except Exception as exc:
                logger.warning(
                    "【行情】⚠️ %s | 订阅失败，改用实时请求 | %s",
                    format_stock_label(qmt_code, self.instrument_name(qmt_code)),
                    exc,
                )
                retry_after[qmt_code] = time.monotonic() + 5
                self._subscribe_retry_after = retry_after
                return
            retry_after.pop(qmt_code, None)
            self._subscribed.add(qmt_code)

    def latest_quote(self, code: str) -> Quote:
        """取最新盘口快照（最新价 + 卖一/买一 + 涨跌停价），供常规下单定价。"""
        # get_full_tick 返回以证券代码为 key 的 tick 字典。
        qmt_code, tick = self._latest_tick(code)
        last_price = float(tick.get("lastPrice") or tick.get("last_price"))
        high_limit, low_limit = self._limit_prices_of(qmt_code)
        quote_time = self._freshness_time(qmt_code, tick)
        return Quote(
            last_price=last_price,
            ask1=_first_book_level(tick.get("askPrice")),
            bid1=_first_book_level(tick.get("bidPrice")),
            high_limit=high_limit,
            low_limit=low_limit,
            quote_time=quote_time,
        )

    def latest_strategy_snapshot(self, code: str) -> MarketSnapshot:
        """读取策略规则所需的完整行情快照，不在适配器内进行任何策略判断。"""
        qmt_code, tick = self._latest_tick(code)
        high_limit, low_limit = self._limit_prices_of(qmt_code)
        return _strategy_snapshot_from_tick(
            code,
            tick,
            high_limit=high_limit,
            low_limit=low_limit,
            quote_time=self._freshness_time(qmt_code, tick),
        )

    def _latest_tick(self, code: str) -> tuple[str, dict[str, Any]]:
        """订阅后取单只 tick；无数据时抛错，让调用方走重试/兜底而非静默拿空。"""
        qmt_code = jq_code_to_qmt_code(code)
        self._ensure_subscribed(qmt_code)
        ticks = self.xtdata.get_full_tick([qmt_code])
        tick = ticks.get(qmt_code)
        if not tick:
            raise RuntimeError(
                f"no tick data for {format_stock_label(code, self.instrument_name(code))}"
            )
        return qmt_code, tick

    def latest_price(self, code: str) -> float:
        """返回最新成交价（latest_quote 的便捷封装）。"""
        return self.latest_quote(code).last_price

    def instrument_name(self, code: str) -> str | None:
        """返回证券中文名; 合约信息不可得时返回 None, 日志回退为只显示代码。"""
        qmt_code = jq_code_to_qmt_code(code)
        detail = self._instrument_detail_of(qmt_code)
        if detail is None:
            return None
        name = detail.get("InstrumentName") or detail.get("instrument_name")
        return str(name).strip() if name else None

    def cached_instrument_name(self, code: str) -> str | None:
        """只读缓存的证券中文名, 未命中返回 None 且不触发 QMT 查询 —— 收信线程专用。

        收信线程绝不能为了日志里的中文名去等一次 get_instrument_detail;
        名字没缓存就先用代码显示, 工作线程里的 instrument_name 兜底补名。
        """
        today = dt.date.today().isoformat()
        if today != self._instrument_detail_cache_day:
            return None
        qmt_code = jq_code_to_qmt_code(code)
        detail = self._instrument_detail_cache.get(qmt_code)
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
        # 单飞: 并发首触同一代码(例如两个 worker 同时拿到同一只票的信号)
        # 只发一次 QMT 查询, 其余线程等在锁上直接吃缓存。
        with self._instrument_detail_lock:
            cached = self._instrument_detail_cache.get(qmt_code)
            if cached is not None:
                return cached
            try:
                detail = self.xtdata.get_instrument_detail(qmt_code)
            except Exception as exc:
                logger.warning(
                    "【行情】⚠️ %s | 合约静态信息查询失败 | %s",
                    format_stock_label(qmt_code),
                    exc,
                )
                return None
            if not detail:
                logger.warning("【行情】⚠️ %s | 合约静态信息为空", format_stock_label(qmt_code))
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


def _optional_positive_price(value: Any) -> float | None:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if result > 0 else None


def _quote_datetime(raw: Any) -> dt.datetime | None:
    if raw is None:
        return None
    if isinstance(raw, (int, float)):
        value = float(raw)
        if value > 10_000_000_000:
            value /= 1000
        try:
            return dt.datetime.fromtimestamp(value)
        except (OSError, OverflowError, ValueError):
            return None
    text = str(raw).strip()
    if text.isdigit():
        return _quote_datetime(int(text))
    for fmt in (
        "%Y%m%d %H:%M:%S.%f",
        "%Y%m%d %H:%M:%S",
        "%Y%m%d%H%M%S%f",
        "%Y%m%d%H%M%S",
        "%Y-%m-%d %H:%M:%S.%f",
        "%Y-%m-%d %H:%M:%S",
    ):
        try:
            return dt.datetime.strptime(text, fmt)
        except ValueError:
            continue
    return None


def _strategy_snapshot_from_tick(
    code: str,
    tick: dict[str, Any],
    *,
    high_limit: float | None,
    low_limit: float | None,
    quote_time: dt.datetime | None = None,
) -> MarketSnapshot:
    """把 QMT tick 字段机械映射为策略快照，缺失字段保留 None 供规则封闭失败。

    quote_time 传新鲜度时间(最后一笔成交时间与最近推送到达时间的较新者);
    不传时回退到 tick 自带的时间戳, 与旧版行为一致。
    """
    last_price = _optional_positive_price(
        tick.get("lastPrice") or tick.get("last_price")
    )
    if last_price is None:
        raise RuntimeError(f"invalid last price for {code}")
    if quote_time is None:
        quote_time = _quote_datetime(
            tick.get("time") or tick.get("timetag") or tick.get("stime")
        )
    return MarketSnapshot(
        code=code,
        last_price=last_price,
        open_price=_optional_positive_price(
            tick.get("open") or tick.get("openPrice") or tick.get("open_price")
        ),
        previous_close=_optional_positive_price(
            tick.get("lastClose")
            or tick.get("preClose")
            or tick.get("previous_close")
        ),
        ask1=_first_book_level(tick.get("askPrice") or tick.get("ask_price")),
        bid1=_first_book_level(tick.get("bidPrice") or tick.get("bid_price")),
        high_limit=high_limit,
        low_limit=low_limit,
        quote_time=quote_time,
        trading_date=quote_time.date() if quote_time is not None else None,
        day_high=_optional_positive_price(tick.get("high") or tick.get("high_price")),
    )


def _optional_numeric_attr(obj: Any, names: tuple[str, ...]) -> float | None:
    for name in names:
        value = getattr(obj, name, None)
        if value is not None:
            try:
                return float(value)
            except (TypeError, ValueError) as exc:
                raise RuntimeError(f"QMT field {name} is not numeric: {value!r}") from exc
    return None


def _required_numeric_attr(
    obj: Any, names: tuple[str, ...], field: str
) -> float:
    value = _optional_numeric_attr(obj, names)
    if value is None:
        raise RuntimeError(f"Cannot extract {field} from QMT object: {obj}")
    return value


# ---------------------------------------------------------------------------
# 交易适配器
# ---------------------------------------------------------------------------

class QmtBrokerAdapter:
    """miniQMT 交易适配器。

    实现 executor.py 中的 BrokerAdapter 协议: submit_order / get_order_snapshot / cancel_order。
    """

    def __init__(
        self,
        config: TradingConfig,
        *,
        ghost_order_detect_grace_sec: float = 0.0,
    ):
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
        # 幽灵单检测宽限期(秒): 自有单超过该时长仍不出现在 QMT 全量清单,
        # 就不再以预填 OPEN/0 伪装健康排队单, 改报 NOT_VISIBLE 让引擎启动
        # 三重校验。0 = 关闭检测, 维持旧行为。
        self._ghost_detect_grace_sec: float = max(0.0, float(ghost_order_detect_grace_sec))
        # 每笔自有单的报单受理时刻: 幽灵单判定从它起算宽限期。
        self._owned_order_submitted_mono: dict[str, float] = {}

        # ---- 线程安全锁: 保护 QMT 交易 API 调用 ----
        self._trading_lock = threading.Lock()

        # ---- 订单缓存: 减少轮询时重复 QMT API 调用 ----
        # 多个 worker 线程会并发读写它, 必须有锁: 否则 submit_order 的预填可能
        # 被并发的全量刷新整体替换掉, 而且缓存过期瞬间多个线程会同时发起
        # query_stock_orders 全量查询(单飞由本锁保证)。
        self._orders_cache: dict[str, OrderSnapshot] = {}  # order_id → 快照
        self._orders_cache_time: float = 0.0
        # 单笔订单快照的最后更新时间(回调/预填/全量刷新): 该单新鲜时查单直接
        # 采信缓存, 不触发全量扫描 —— 开盘并发轮询时全量扫描与报单抢同一把锁。
        self._orders_updated_mono: dict[str, float] = {}
        self._orders_cache_lock = threading.RLock()
        # 只登记本进程 order_stock 成功返回的订单。QMT 会推送账户内所有订单，
        # 手工单或其他程序的回调绝不能改变 follower 的订单状态。
        self._owned_order_remarks: dict[str, str] = {}
        self._owned_trade_ids: dict[str, set[str]] = {}
        self._owned_trade_filled_qty: dict[str, int] = {}
        # 报单/成交/错误回调账本只对当日订单有意义: 日期翻转时整本清空,
        # 进程连跑数月不会缓慢泄漏内存。
        self._bookkeeping_date: str = dt.date.today().isoformat()
        # 全量刷新的单飞锁，与上面的缓存锁分开：见 get_order_snapshot。
        self._orders_refresh_lock = threading.Lock()
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

        class _OrderStateCallback(XtQuantTraderCallback):
            def on_order_error(self, error):
                adapter._cache_order_error(error)

            def on_stock_order(self, order):
                adapter._cache_owned_order_update(order)

            def on_stock_trade(self, trade):
                adapter._cache_owned_trade_update(trade)

        # 保留强引用，避免回调对象被回收。
        self._callback = _OrderStateCallback()
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

    # ---- BrokerAdapter 协议 -----------------------------------------------

    def query_available_cash(self) -> float:
        """查询账户当前可用资金。"""
        started = time.monotonic()
        with self._trading_lock:
            asset = self._trader.query_stock_asset(self._account)
        _log_slow_call("QMT资金查询耗时", started)
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
        # 兜底：市值 + 现金也能拼出总资产，好过直接让买单全废。
        # 冻结资金必须算进来 —— 它是已报未成委托占用的钱，仍然是账户的资产。
        # 漏掉它会在挂单期间低估总资产，进而把单票集中度上限算小、少买。
        market_value = getattr(asset, "market_value", None)
        cash = getattr(asset, "cash", None)
        if market_value is not None and cash is not None:
            frozen_cash = getattr(asset, "frozen_cash", None) or 0.0
            logger.warning(
                "【QMT】⚠️ 未找到总资产字段 | 退化为 市值+可用+冻结 估算",
            )
            return float(market_value) + float(cash) + float(frozen_cash)
        raise RuntimeError(
            f"Cannot extract total assets from asset object: {asset} "
            f"(available attrs: {[a for a in dir(asset) if not a.startswith('_')]})"
        )

    def query_account_snapshot(self) -> AccountSnapshot:
        """单次读取真实账户资产与全部持仓；查询失败和空仓必须严格区分。"""
        with self._trading_lock:
            asset = self._trader.query_stock_asset(self._account)
            positions_raw = self._trader.query_stock_positions(self._account)
        if asset is None:
            raise RuntimeError("QMT account asset query returned None")
        if positions_raw is None:
            raise RuntimeError("QMT account positions query returned None")

        available_cash = _required_numeric_attr(
            asset,
            ("cash", "m_dAvailable", "available_cash", "m_dBalance"),
            "available cash",
        )
        # 冻结资金: 已报未成委托占用的钱。幽灵单三重校验拿它排除"订单在途
        # 只是回报丢失"; 缺字段时按 0 处理(老版本 QMT 无此字段则校验退化)。
        frozen_cash = _optional_numeric_attr(asset, ("frozen_cash",)) or 0.0
        positions: list[PositionSnapshot] = []
        for raw in positions_raw:
            qmt_code = str(getattr(raw, "stock_code", "") or "")
            if not qmt_code:
                raise RuntimeError(f"QMT position has no stock_code: {raw}")
            total_qty = int(
                _required_numeric_attr(
                    raw, ("volume", "m_nVolume"), f"{qmt_code} total volume"
                )
            )
            if total_qty <= 0:
                continue
            available_qty = int(
                _required_numeric_attr(
                    raw,
                    ("can_use_volume", "m_nCanUseVolume"),
                    f"{qmt_code} available volume",
                )
            )
            positions.append(
                PositionSnapshot(
                    code=qmt_code_to_jq_code(qmt_code),
                    total_qty=total_qty,
                    available_qty=max(0, available_qty),
                    cost_price=_required_numeric_attr(
                        raw,
                        ("open_price", "avg_price", "m_dOpenPrice"),
                        f"{qmt_code} cost price",
                    ),
                    market_value=_required_numeric_attr(
                        raw,
                        ("market_value", "m_dMarketValue"),
                        f"{qmt_code} market value",
                    ),
                )
            )
        positions_tuple = tuple(positions)
        position_market_value = sum(item.market_value for item in positions_tuple)
        market_value = _optional_numeric_attr(
            asset, ("market_value", "m_dMarketValue")
        )
        if market_value is None:
            market_value = position_market_value
        total_assets = _optional_numeric_attr(
            asset, ("total_asset", "m_dTotalAssets", "total_assets")
        )
        if total_assets is None:
            total_assets = market_value + available_cash + frozen_cash
        return AccountSnapshot(
            available_cash=available_cash,
            total_assets=total_assets,
            market_value=market_value,
            positions=positions_tuple,
            frozen_cash=frozen_cash,
        )

    def submit_order(self, signal: TradeSignal, quantity: int, price: float) -> str:
        """向 QMT 提交固定价委托并登记自有订单缓存，返回 QMT 单号。

        废单返回码走回调错误归因并抛 BrokerOrderRejected；报单过程异常或状态不明
        则抛 BrokerSubmissionUncertain，交由执行引擎的撤单复查/熔断逻辑兜底。
        """
        self._prune_bookkeeping_if_new_day()
        qmt_code = jq_code_to_qmt_code(signal.code)
        order_type = self._STOCK_BUY if signal.action == Action.BUY else self._STOCK_SELL
        order_remark = _order_remark_for_signal_id(signal.signal_id)

        started = time.monotonic()
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
                    order_remark,
                )
        except Exception as exc:
            logger.debug(
                "⏱️ QMT报单调用失败 | %s | %.1fms | %s",
                signal.label, (time.monotonic() - started) * 1000, exc,
            )
            raise BrokerSubmissionUncertain(
                f"QMT order submission state is uncertain: {exc}"
            ) from exc

        _log_slow_call(f"QMT报单调用耗时 {signal.label}", started)

        if order_id is None or (isinstance(order_id, int) and order_id < 0):
            rejection = self._take_order_error_by_remark(
                order_remark, wait_timeout=_ORDER_ERROR_WAIT_SEC,
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
        now_mono = time.monotonic()
        with self._orders_cache_lock:
            self._orders_cache[oid] = OrderSnapshot(
                order_id=oid, status=BrokerOrderStatus.OPEN, filled_qty=0,
            )
            self._orders_updated_mono[oid] = now_mono
            self._owned_order_remarks[oid] = order_remark
            self._owned_order_submitted_mono[oid] = now_mono

        logger.debug(
            "📤 委托已提交 | QMT单号=%s 信号=%s 代码=%s 数量=%s 价格=%.3f",
            order_id,
            signal.label,
            signal.display_code,
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

    def _cache_owned_order_update(self, order: Any) -> None:
        """接收订单状态回调，仅采信本服务报出的匹配订单。

        回调线程只更新内存，不能调用 QMT、Redis 或 SQLite。外部手工单、其他程序
        的订单以及字段不完整的回调全部忽略，原有轮询会继续兜底。
        """
        raw_order_id = getattr(order, "order_id", None)
        order_id = str(raw_order_id) if raw_order_id is not None else ""
        order_remark = str(getattr(order, "order_remark", "") or "")
        with self._orders_cache_lock:
            expected_remark = self._owned_order_remarks.get(order_id)
        if not order_id or expected_remark is None or order_remark != expected_remark:
            # 纯噪音: QMT 会推送账户内所有订单, 非本服务订单每次回调都打一行,
            # 开盘时刷屏刷盘, 删掉不留。
            return

        snapshot = self._snapshot_from_qmt_order(order)
        with self._orders_cache_lock:
            current = self._orders_cache.get(order_id)
            if current is not None and snapshot.filled_qty < current.filled_qty:
                snapshot = replace(snapshot, filled_qty=current.filled_qty)
            if current is not None and current.status in _TERMINAL_ORDER_STATUSES and (
                snapshot.status not in _TERMINAL_ORDER_STATUSES
                or snapshot.filled_qty < current.filled_qty
            ):
                logger.debug(
                    "📭 忽略订单状态回退回调 | QMT单号=%s | 当前=%s | 回调=%s",
                    order_id, current.status.value, snapshot.status.value,
                )
                return
            self._orders_cache[order_id] = snapshot
            self._orders_updated_mono[order_id] = time.monotonic()
        logger.debug(
            "📬 已采信订单回调 | QMT单号=%s | 状态=%s | 成交=%s",
            order_id, snapshot.status.value, snapshot.filled_qty,
        )

    def _cache_owned_trade_update(self, trade: Any) -> None:
        """接收成交回调，单调更新成交量但不擅自标记订单终态。"""
        # 成交回调没有 order_status，订单状态回调和原有轮询仍负责最终状态。
        raw_order_id = getattr(trade, "order_id", None)
        order_id = str(raw_order_id) if raw_order_id is not None else ""
        order_remark = str(getattr(trade, "order_remark", "") or "")
        trade_id = str(getattr(trade, "traded_id", "") or "")
        try:
            filled_qty = int(getattr(trade, "traded_volume", 0) or 0)
        except (TypeError, ValueError):
            filled_qty = 0
        with self._orders_cache_lock:
            expected_remark = self._owned_order_remarks.get(order_id)
            if (
                not order_id
                or expected_remark is None
                or order_remark != expected_remark
                or not trade_id
                or filled_qty <= 0
            ):
                return
            seen_trade_ids = self._owned_trade_ids.setdefault(order_id, set())
            if trade_id in seen_trade_ids:
                return
            seen_trade_ids.add(trade_id)
            cumulative = self._owned_trade_filled_qty.get(order_id, 0) + filled_qty
            self._owned_trade_filled_qty[order_id] = cumulative
            current = self._orders_cache.get(order_id)
            if current is not None:
                self._orders_cache[order_id] = replace(
                    current, filled_qty=max(current.filled_qty, cumulative),
                )
                self._orders_updated_mono[order_id] = time.monotonic()
        if current is None:
            return
        logger.debug(
            "📬 已采信成交回调 | QMT单号=%s | 本次=%s | 累计=%s",
            order_id, filled_qty, cumulative,
        )

    def _orders_cache_is_stale(self) -> bool:
        with self._orders_cache_lock:
            return time.monotonic() - self._orders_cache_time >= _ORDERS_CACHE_TTL

    def get_order_snapshot(self, order_id: str) -> OrderSnapshot:
        """查询单个订单快照，带 100ms 短期缓存避免轮询时重复全量查询。"""
        with self._orders_cache_lock:
            callback_snapshot = self._orders_cache.get(order_id)
            updated_mono = self._orders_updated_mono.get(order_id, 0.0)
        if (
            callback_snapshot is not None
            and callback_snapshot.status in _TERMINAL_ORDER_STATUSES
        ):
            return callback_snapshot
        # 单笔新鲜度: 该订单刚被回调/成交推送/预填更新过, 直接采信缓存,
        # 不触发全量扫描 —— 开盘时全量扫描与报单在 _trading_lock 上互堵。
        if (
            callback_snapshot is not None
            and time.monotonic() - updated_mono < _ORDERS_CACHE_TTL
        ):
            return callback_snapshot
        if self._orders_cache_is_stale():
            # 单飞放在专用锁上, 而不是压在缓存锁里: QMT 全量查询是一次同步的
            # 跨进程调用, 把缓存锁攥着等它返回, 会让其余轮询线程连"读一眼旧
            # 快照"都做不到 —— 9:30 一批并发委托时这就是一次整齐的串行。
            with self._orders_refresh_lock:
                if self._orders_cache_is_stale():  # 双检: 可能已被别的线程刷过
                    self._refresh_orders_cache()

        with self._orders_cache_lock:
            cached = self._orders_cache.get(order_id)
        if cached is not None:
            return cached

        # 缓存没有（可能刚下单还没刷新），返回保守值
        return OrderSnapshot(order_id=order_id, status=BrokerOrderStatus.OPEN, filled_qty=0)

    def query_orders_by_signal_id(self, signal_id: str) -> tuple[OrderSnapshot, ...]:
        """直接向 QMT 查询并按 order_remark 精确匹配，绕过短缓存。"""
        with self._trading_lock:
            orders: list[Any] = self._trader.query_stock_orders(self._account)
        accepted_remarks = {
            str(signal_id),  # 兼容历史短 ID 和不截断备注的大 QMT
            _order_remark_for_signal_id(signal_id),
        }
        return tuple(
            self._snapshot_from_qmt_order(order)
            for order in orders
            if str(getattr(order, "order_remark", "") or "") in accepted_remarks
        )

    def _refresh_orders_cache(self) -> None:
        """从 QMT 全量拉取订单列表并整体替换缓存。

        QMT 查询在缓存锁之外完成，只有最后的整体替换持锁。调用方应持有
        _orders_refresh_lock 以保证单飞。
        """
        self._prune_bookkeeping_if_new_day()
        started = time.monotonic()
        with self._trading_lock:
            orders: list[Any] = self._trader.query_stock_orders(self._account)
        _log_slow_call(f"QMT全量查单耗时 {len(orders)}笔", started)
        fresh = {
            str(order.order_id): self._snapshot_from_qmt_order(order)
            for order in orders
        }
        with self._orders_cache_lock:
            for order_id, current in self._orders_cache.items():
                refreshed = fresh.get(order_id)
                preserved_current = current
                if refreshed is not None and refreshed.filled_qty > current.filled_qty:
                    preserved_current = replace(current, filled_qty=refreshed.filled_qty)
                if order_id in self._owned_order_remarks and refreshed is None:
                    # 刚预填的新单尚未出现在 QMT 全量清单(查询早于受理可见):
                    # 宽限期内保留预填快照等下一轮刷新接上。超过宽限期仍不可见
                    # 是幽灵单的典型特征 —— 继续用 OPEN/0 伪装健康排队单会让
                    # 引擎傻等到超时(实测曾等满数分钟零成交),
                    # 改报 NOT_VISIBLE 交给引擎三重校验后重挂。终态快照的保留
                    # 逻辑不受影响(已成交/已撤的单不允许被降级成不可见)。
                    if (
                        self._ghost_detect_grace_sec > 0
                        and current.status not in _TERMINAL_ORDER_STATUSES
                        and time.monotonic()
                        - self._owned_order_submitted_mono.get(order_id, 0.0)
                        > self._ghost_detect_grace_sec
                    ):
                        fresh[order_id] = replace(
                            current, status=BrokerOrderStatus.NOT_VISIBLE,
                        )
                    else:
                        fresh[order_id] = current
                elif (
                    order_id in self._owned_order_remarks
                    and current.status in _TERMINAL_ORDER_STATUSES
                    and (
                        refreshed is None
                        or refreshed.status not in _TERMINAL_ORDER_STATUSES
                        or refreshed.filled_qty < current.filled_qty
                    )
                ):
                    fresh[order_id] = preserved_current
                elif (
                    order_id in self._owned_order_remarks
                    and refreshed is not None
                    and refreshed.filled_qty < current.filled_qty
                ):
                    fresh[order_id] = replace(
                        refreshed, filled_qty=current.filled_qty,
                    )
            self._orders_cache = fresh
            self._orders_cache_time = time.monotonic()
            self._orders_updated_mono = {
                order_id: self._orders_cache_time for order_id in fresh
            }

    def _snapshot_from_qmt_order(self, order: Any) -> OrderSnapshot:
        oid = str(order.order_id)
        status = _qmt_order_status_to_broker_status(order.order_status)
        callback_error = (
            self._take_order_error_by_id(oid)
            if status == BrokerOrderStatus.REJECTED
            else None
        )
        raw_status_msg = str(getattr(order, "status_msg", "") or "").strip()
        rejection_reason = None
        rejection_code = None
        if status == BrokerOrderStatus.REJECTED:
            rejection_reason = raw_status_msg or (
                callback_error.reason if callback_error is not None else "QMT order rejected"
            )
            rejection_code = (
                callback_error.error_code if callback_error is not None else None
            )
        quantity = 0
        for attr in ("order_volume", "volume", "order_qty"):
            raw_quantity = getattr(order, attr, None)
            if raw_quantity is not None:
                try:
                    quantity = int(raw_quantity)
                    break
                except (TypeError, ValueError):
                    continue
        raw_price = getattr(order, "price", None)
        if raw_price is None:
            raw_price = getattr(order, "order_price", 0.0)
        try:
            price = float(raw_price or 0.0)
        except (TypeError, ValueError):
            price = 0.0
        return OrderSnapshot(
            order_id=oid,
            status=status,
            filled_qty=int(getattr(order, "traded_volume", 0)),
            rejection_reason=rejection_reason,
            rejection_code=rejection_code,
            rejection_kind=(
                classify_qmt_rejection(rejection_reason)
                if status == BrokerOrderStatus.REJECTED
                else None
            ),
            quantity=quantity,
            price=price,
        )

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
        started = time.monotonic()
        with self._trading_lock:
            cancel_result: int = self._trader.cancel_order_stock(self._account, int(order_id))
        _log_slow_call(f"QMT撤单调用耗时 QMT单号={order_id} 返回={cancel_result}", started)

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
        """使指定订单缓存失效并强制下一次查单穿透到 QMT 取真实状态。"""
        with self._orders_cache_lock:
            self._orders_cache.pop(order_id, None)
            self._orders_updated_mono.pop(order_id, None)
            self._orders_cache_time = 0.0

    def _prune_bookkeeping_if_new_day(self) -> None:
        """日期翻转时清空报单/成交/错误回调账本, 防止进程连跑数月缓慢泄漏。

        订单回调账本只对当日订单有意义(QMT 查询本身也只返回当日订单),
        新的一天从空账本开始没有任何正确性损失。
        """
        today = dt.date.today().isoformat()
        if today == self._bookkeeping_date:
            return
        self._bookkeeping_date = today
        with self._orders_cache_lock:
            self._owned_order_remarks.clear()
            self._owned_order_submitted_mono.clear()
            self._owned_trade_ids.clear()
            self._owned_trade_filled_qty.clear()
        with self._order_error_lock:
            self._order_errors_by_remark.clear()
            self._order_errors_by_id.clear()

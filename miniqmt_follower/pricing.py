"""委托价计算：根据实时行情快照与配置生成可提交的委托价。

核心职责: slippage/book 两种常规定价、竞价与开盘时段的排队激进报价、沪深 A 股
动态价格笼子，以及按品种 tick size 的合法报价取整。

本模块约定:
- 普通委托绝不直接挂涨跌停价，只做有界的百分比激进报价并夹进涨跌停带；
- 不做 reference_price 偏离拒单拦截，价格风险由实时盘口 + 涨跌停带兜底；
- 竞价/开盘时段判断依赖本模块级函数，测试可替换以解耦真实时钟。
"""

from __future__ import annotations

import datetime as dt
import logging
from decimal import ROUND_HALF_UP, Decimal

from miniqmt_follower.config import MachineScheduleConfig
from miniqmt_follower.models import Action, ExecutionConfig, Quote, TradeSignal

logger = logging.getLogger(__name__)

def _in_call_auction(machine_schedule: MachineScheduleConfig) -> bool:
    """当前是否处于配置的竞价排队时段。

    测试可替换本模块级函数, 让定价与真实时钟解耦。
    """
    market_session = machine_schedule.market_session
    return (
        market_session.call_auction_start_at
        <= dt.datetime.now().time()
        < market_session.continuous_trading_start_at
    )


def _in_opening_window(
    machine_schedule: MachineScheduleConfig, window_sec: float
) -> bool:
    """连续竞价开始后的前 window_sec 秒内返回 True; window<=0 恒 False。

    开盘首挂强化用: 开盘后短窗口内沿用竞价激进报价, 买时间优先。
    测试可替换本模块级函数。
    """
    if window_sec <= 0:
        return False
    session = machine_schedule.market_session
    now = dt.datetime.now()
    if now.time() < session.continuous_trading_start_at:
        return False
    open_dt = now.replace(
        hour=session.continuous_trading_start_at.hour,
        minute=session.continuous_trading_start_at.minute,
        second=session.continuous_trading_start_at.second,
        microsecond=0,
    )
    return 0 <= (now - open_dt).total_seconds() <= window_sec


def tick_size_for(code: str) -> float:
    """按证券类型返回最小报价单位。

    股票 0.01 元; 基金/ETF 0.001 元(沪市 5 开头, 深市 1 开头)。
    ETF 若按 0.01 取整, 买单会被压低 1~9 厘, 盘口紧时根本挂不到对手价。
    """
    prefix = code.split(".")[0]
    suffix = code.rsplit(".", 1)[-1] if "." in code else ""
    if suffix in ("XSHG", "SH") and prefix.startswith("5"):
        return 0.001
    if suffix in ("XSHE", "SZ") and prefix.startswith("1"):
        return 0.001
    return 0.01


def _is_a_share(code: str) -> bool:
    """只识别沪深 A 股，避免把股票价格笼子套到 ETF、基金或债券。"""
    security_code = code.split(".", 1)[0]
    suffix = code.rsplit(".", 1)[-1] if "." in code else ""
    if suffix in ("XSHG", "SH"):
        return security_code.startswith("6")
    if suffix in ("XSHE", "SZ"):
        return security_code.startswith(("0", "3"))
    return False


def _round_to_tick_half_up(value: float, tick: float) -> float:
    """按交易所常用的四舍五入取到合法 tick，避开 Python 银行家舍入。"""
    # 先清理浮点乘法产生的 10.004999999999999 之类尾差，再交给 Decimal。
    value_decimal = Decimal(str(round(value, 12)))
    tick_decimal = Decimal(str(tick))
    tick_units = (value_decimal / tick_decimal).quantize(
        Decimal("1"), rounding=ROUND_HALF_UP,
    )
    return float(tick_units * tick_decimal)


def calculate_order_price(
    signal: TradeSignal,
    quote: Quote,
    config: ExecutionConfig,
    machine_schedule: MachineScheduleConfig,
    *,
    prefer_book: bool = False,
) -> float:
    """根据行情快照计算委托价。

    slippage 模式: 最新成交价 ± 固定百分比滑点。
    book 模式: 直接吃对手盘 —— 买入=卖一价+N个tick, 卖出=买一价-N个tick,
      目标是首次挂单即成交, 避免撤单重挂的秒级损耗; 对手盘缺失时回退 slippage。
    配置的竞价时段另走排队报价, 见 _auction_queue_price。

    刻意不做"行情价偏离 reference_price 就拒单"的拦截: 本执行端是跟单器,
    被拦下的委托会让实盘持仓与聚宽模拟盘永久分叉, 而且没有补单机制 ——
    一次拦截换来的是长期的持仓不一致, 不划算。价格风险由三层兜底:
    委托价始终基于实时盘口(成交发生在真实对手价上, 不是凭空报价)、
    竞价排队报价强制夹在涨跌停内、以及 ±10% 的日内涨跌停带本身。
    择时与选股层面的风控属于策略端职责, 不在这里重复。
    """
    if quote.last_price <= 0:
        raise ValueError("last_price must be positive")

    tick = tick_size_for(signal.code)
    if prefer_book:
        # 价格拒单后的重挂: 盘口锚定报价, 不再走竞价/开盘的百分比激进报价。
        order_price, mode_label = _book_anchor_price(signal.action, quote, tick)
    else:
        auction_price = _auction_queue_price(
            signal.action, quote, config, tick, machine_schedule
        )
        if auction_price is not None:
            order_price, mode_label = auction_price, "auction/opening"
        else:
            order_price, mode_label = _price_by_mode(signal.action, quote, config, tick)

    is_a_share = _is_a_share(signal.code)
    if is_a_share:
        order_price = _apply_stock_dynamic_price_cage(
            signal.action, quote, order_price, tick,
        )

    logger.debug(
        "💹 定价计算 | 代码=%s 方向=%s 模式=%s 最新价=%.3f 卖一=%s 买一=%s 委托价=%.3f",
        signal.display_code,
        signal.action.value,
        mode_label,
        quote.last_price,
        quote.ask1,
        quote.bid1,
        order_price,
    )
    # 按品种 tick size 取整到合法报价, 再消除浮点尾差。
    if is_a_share:
        return round(_round_to_tick_half_up(order_price, tick), 3)
    return round(round(order_price / tick) * tick, 3)


def _apply_stock_dynamic_price_cage(
    action: Action,
    quote: Quote,
    candidate_price: float,
    tick: float,
) -> float:
    """把沪深 A 股候选委托价夹进动态价格笼子和当日涨跌停范围。"""
    if action == Action.BUY:
        reference = quote.ask1 or quote.bid1 or quote.last_price
        percent_boundary = _round_to_tick_half_up(reference * 1.02, tick)
        dynamic_boundary = max(percent_boundary, reference + 10 * tick)
        price = min(candidate_price, dynamic_boundary)
        if price == dynamic_boundary:
            # 恰好贴在笼顶的报价回退 1 tick: 102% 边界经过 tick 取整可能
            # 比交易所动态有效申报范围高出 1~2 分, 贴边即拒单。
            price = max(price - tick, 0.0)
        if quote.high_limit is not None:
            price = min(price, quote.high_limit)
        if quote.low_limit is not None:
            price = max(price, quote.low_limit)
    else:
        reference = quote.bid1 or quote.ask1 or quote.last_price
        percent_boundary = _round_to_tick_half_up(reference * 0.98, tick)
        dynamic_boundary = min(percent_boundary, reference - 10 * tick)
        price = max(candidate_price, dynamic_boundary)
        if price == dynamic_boundary:
            # 恰好贴在笼底的报价抬升 1 tick, 同理避免取整越界被拒单。
            price = price + tick
        if quote.low_limit is not None:
            price = max(price, quote.low_limit)
        if quote.high_limit is not None:
            price = min(price, quote.high_limit)

    logger.debug(
        "💹 股票价格笼子 | 方向=%s 基准=%.3f 动态边界=%.3f 候选=%.3f 最终=%.3f",
        action.value, reference, dynamic_boundary, candidate_price, price,
    )
    return price


def _auction_queue_price(
    action: Action,
    quote: Quote,
    config: ExecutionConfig,
    tick: float,
    machine_schedule: MachineScheduleConfig,
) -> float | None:
    """配置竞价时段的排队报价; 不适用时返回 None 由常规模式定价。

    为什么普通单不挂涨跌停价: 9:27 等盘前信号早已错过 9:25 的开盘集合竞价,
    这些委托排队等 9:30 连续竞价开撮, 成交价按对手方挂单价逐档确定。挂跌停价卖
    能成交的部分确实按买一价成交, 但吃不完的剩余会留在跌停价当卖一, 被后续买单
    以跌停价扫走 —— 清仓低开股时买盘薄, 等于自己把票砸到跌停。买单挂涨停价同理
    会被反向宰。所以只做有界的百分比激进报价:

    - 幅度 quote_band_pct (默认 1.5%) 远小于 ±10% 涨跌停带, 剩余暴露有限;
    - 结果强制夹进 [跌停价, 涨停价], 越界报价会被交易所废单;
    - 取不到涨跌停价则整个回退常规定价 —— 宁可排位靠后, 不能裸奔。
    """
    if config.quote_band_pct > 0 and _in_call_auction(machine_schedule):
        aggressive_pct = config.quote_band_pct
        mode_label = "竞价"
    elif config.quote_band_pct > 0 and _in_opening_window(
        machine_schedule, config.opening_aggressive_window_sec
    ):
        aggressive_pct = config.quote_band_pct
        mode_label = "开盘"
    else:
        return None
    if quote.high_limit is None or quote.low_limit is None:
        logger.debug(
            "💹 %s排队定价回退 | 行情源无涨跌停价, 无法封顶 | 方向=%s",
            mode_label, action.value,
        )
        return None

    if action == Action.BUY:
        raw_price = quote.last_price * (1 + aggressive_pct)
    else:
        raw_price = quote.last_price * (1 - aggressive_pct)

    price = round(raw_price / tick) * tick
    clamped = min(max(price, quote.low_limit), quote.high_limit)
    logger.debug(
        "💹 %s排队定价 | 方向=%s 最新价=%.3f 激进=%.1f%% 报价=%.3f 涨跌停=[%.3f, %.3f]",
        mode_label, action.value, quote.last_price, aggressive_pct * 100,
        clamped, quote.low_limit, quote.high_limit,
    )
    return clamped


def _book_anchor_price(
    action: Action, quote: Quote, tick: float
) -> tuple[float, str]:
    """价格拒单后的盘口锚定报价: 卖挂买一、买挂卖一, 缺档回退最新价。

    交易所价格笼子只拒"低于实时买一×98%(或买一-10tick)的卖单"与"高于实时
    卖一×102%(或卖一+10tick)的买单"。锚在对手盘现价上天然合法, 且不依赖
    可能滞后于盘口的"最新价×激进幅度"——这正是开盘宽限内冻结快照导致连续
    拒单的场景。报价仍会过 A 股动态价格笼子夹取, 双保险。
    """
    if action == Action.BUY:
        anchor = quote.ask1 or quote.bid1 or quote.last_price
    else:
        anchor = quote.bid1 or quote.ask1 or quote.last_price
    if anchor is None or anchor <= 0:
        raise ValueError("book anchor unavailable for price-rejection requote")
    return round(anchor / tick) * tick, "book-anchor"


def _price_by_mode(
    action: Action, quote: Quote, config: ExecutionConfig, tick: float
) -> tuple[float, str]:
    """返回 (委托价, 模式标签)。"""
    offset = config.book_tick_offset * tick

    if config.pricing_mode == "book":
        if action == Action.BUY and quote.ask1 and quote.ask1 > 0:
            return quote.ask1 + offset, "book"
        if action == Action.SELL and quote.bid1 and quote.bid1 > 0:
            return quote.bid1 - offset, "book"
        # 对手盘缺失(涨跌停单边、行情源无档位) → 回退 slippage
        logger.debug("💹 盘口缺失, 回退滑点定价 | 方向=%s 卖一=%s 买一=%s", action.value, quote.ask1, quote.bid1)

    if action == Action.BUY:
        return quote.last_price * (1 + config.quote_band_pct), "slippage"
    return quote.last_price * (1 - config.quote_band_pct), "slippage"

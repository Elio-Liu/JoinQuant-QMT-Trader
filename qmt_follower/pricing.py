from __future__ import annotations

import logging

from qmt_follower.models import Action, ExecutionConfig, Quote, TradeSignal

logger = logging.getLogger(__name__)

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


class PriceDeviationError(ValueError):
    """行情价格相对策略参考价偏离过大, 触发风控保护。"""

    pass


def calculate_order_price(signal: TradeSignal, quote: Quote, config: ExecutionConfig) -> float:
    """根据行情快照计算委托价。

    slippage 模式: 最新成交价 ± 固定百分比滑点。
    book 模式: 直接吃对手盘 —— 买入=卖一价+N个tick, 卖出=买一价-N个tick,
      目标是首次挂单即成交, 避免撤单重挂的秒级损耗; 对手盘缺失时回退 slippage。

    偏离保护针对"定价基准价"(slippage 用最新价, book 用对手盘价)和参考价比较,
    防止极端行情或错误行情导致盲目追单。
    """
    if quote.last_price <= 0:
        raise ValueError("last_price must be positive")

    tick = tick_size_for(signal.code)
    base_price, order_price, mode_label = _price_by_mode(signal.action, quote, config, tick)

    deviation = abs(base_price - signal.reference_price) / signal.reference_price
    if deviation > config.max_deviation_from_signal_price_pct:
        logger.debug(
            "⚠️ 价格偏离触发风控 | 代码=%s 定价模式=%s 基准价=%.2f 参考价=%.2f 偏离=%.2f%% 阈值=%.2f%%",
            signal.code,
            mode_label,
            base_price,
            signal.reference_price,
            deviation * 100,
            config.max_deviation_from_signal_price_pct * 100,
        )
        raise PriceDeviationError(
            f"market price {base_price} deviates {deviation:.4%} from reference {signal.reference_price}"
        )

    logger.debug(
        "💹 定价计算 | 代码=%s 方向=%s 模式=%s 最新价=%.3f 卖一=%s 买一=%s 委托价=%.3f",
        signal.code,
        signal.action.value,
        mode_label,
        quote.last_price,
        quote.ask1,
        quote.bid1,
        order_price,
    )
    # 按品种 tick size 取整到合法报价, 再消除浮点尾差。
    return round(round(order_price / tick) * tick, 3)


def _price_by_mode(
    action: Action, quote: Quote, config: ExecutionConfig, tick: float
) -> tuple[float, float, str]:
    """返回 (定价基准价, 委托价, 模式标签)。"""
    offset = config.book_tick_offset * tick

    if config.pricing_mode == "book":
        if action == Action.BUY and quote.ask1 and quote.ask1 > 0:
            return quote.ask1, quote.ask1 + offset, "book"
        if action == Action.SELL and quote.bid1 and quote.bid1 > 0:
            return quote.bid1, quote.bid1 - offset, "book"
        # 对手盘缺失(涨跌停单边、行情源无档位) → 回退 slippage
        logger.debug("💹 盘口缺失, 回退滑点定价 | 方向=%s 卖一=%s 买一=%s", action.value, quote.ask1, quote.bid1)

    if action == Action.BUY:
        return quote.last_price, quote.last_price * (1 + config.buy_slippage_pct), "slippage"
    return quote.last_price, quote.last_price * (1 - config.sell_slippage_pct), "slippage"

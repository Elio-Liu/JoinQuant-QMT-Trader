from __future__ import annotations

import logging

from qmt_follower.models import Action, ExecutionConfig, TradeSignal

logger = logging.getLogger(__name__)


class PriceDeviationError(ValueError):
    """最新行情相对策略参考价偏离过大, 触发风控保护。"""

    pass


def calculate_order_price(signal: TradeSignal, latest_price: float, config: ExecutionConfig) -> float:
    """按最新价和固定百分比滑点计算委托价。

    买入向上加滑点, 卖出向下减滑点。偏离保护用于防止行情剧烈跳变时盲目追单。
    """
    if latest_price <= 0:
        raise ValueError("latest_price must be positive")

    # 先和聚宽参考价比较, 超过阈值则拒绝执行, 避免极端行情或错误行情导致追价。
    deviation = abs(latest_price - signal.reference_price) / signal.reference_price
    if deviation > config.max_deviation_from_signal_price_pct:
        logger.warning(
            "⚠️ 价格偏离触发风控 | 代码=%s 最新价=%.2f 参考价=%.2f 偏离=%.2f%% 阈值=%.2f%%",
            signal.code,
            latest_price,
            signal.reference_price,
            deviation * 100,
            config.max_deviation_from_signal_price_pct * 100,
        )
        raise PriceDeviationError(
            f"latest price {latest_price} deviates {deviation:.4%} from reference {signal.reference_price}"
        )

    if signal.action == Action.BUY:
        price = latest_price * (1 + config.buy_slippage_pct)
        slippage_direction = "上浮"
        slippage_pct = config.buy_slippage_pct
    else:
        price = latest_price * (1 - config.sell_slippage_pct)
        slippage_direction = "下浮"
        slippage_pct = config.sell_slippage_pct

    logger.debug(
        "💹 定价计算 | 代码=%s 方向=%s 最新价=%.2f 滑点=%s%.2f%% 委托价=%.2f",
        signal.code,
        signal.action.value,
        latest_price,
        slippage_direction,
        slippage_pct * 100,
        price,
    )

    # A股通常按 0.01 元报价, 这里先统一保留两位小数。
    # 后续如支持可转债、ETF 或特殊证券, 可在这里接入 tick size 表。
    return round(price, 2)

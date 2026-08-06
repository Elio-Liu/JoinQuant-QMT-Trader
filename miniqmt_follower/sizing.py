"""意图型信号的数量计算（纯函数，无 I/O，便于单测）。

规则（与策略端语义保持一致）:
- sell_all: 真实可卖持仓全清。
- sell_half: 持仓的一半向下取整到 100 股；不足一手则全卖（与策略端"不足1手全卖"一致）。
- auto_buy: 等分可用资金，再受单票上限约束：min(可用资金÷N, 总资产×单票比例)，
  向下取整到 100 股。
"""

from __future__ import annotations

LOT_SIZE = 100


def resolve_sell_all(available_position: int) -> int:
    """清仓数量 = 真实可卖持仓（负值按 0）。"""
    return max(int(available_position), 0)


def resolve_sell_half(available_position: int) -> int:
    """卖一半: 向下取整到整手；不足一手全卖。"""
    position = max(int(available_position), 0)
    half = position // 2 // LOT_SIZE * LOT_SIZE
    if half >= LOT_SIZE:
        return half
    return position


def resolve_auto_buy(
    *,
    available_cash: float,
    total_assets: float,
    buy_count: int,
    price: float,
    max_single_position_pct: float,
) -> int:
    """自动买入数量: 等分可用资金并受单票上限约束, 向下取整到 100 股。"""
    if buy_count <= 0 or price <= 0:
        return 0
    per_stock_budget = min(
        available_cash / buy_count,
        total_assets * max_single_position_pct,
    )
    return int(per_stock_budget / price / LOT_SIZE) * LOT_SIZE

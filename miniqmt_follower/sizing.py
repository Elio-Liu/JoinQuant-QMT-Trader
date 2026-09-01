"""意图型信号的数量计算（纯函数，无 I/O，便于单测）。

将 sell_all / sell_half / auto_buy 三种意图从真实账户数据换算成委托股数，
规则与策略端语义保持一致。

本模块约定:
- sell_all: 真实可卖持仓全清。
- sell_half: 持仓的一半向下取整到 100 股；不足一手时按
  sell_half_insufficient_lot_mode 处理（sell_all=全卖 / skip=不卖）。
- auto_buy: 等分可用资金，再受单票上限约束：min(可用资金÷N, 总资产×单票比例)，
  向下取整到 100 股。
"""

from __future__ import annotations

LOT_SIZE = 100


def resolve_sell_all(available_position: int) -> int:
    """清仓数量 = 真实可卖持仓（负值按 0）。"""
    return max(int(available_position), 0)


def resolve_sell_half(
    available_position: int, insufficient_lot_mode: str = "sell_all"
) -> int:
    """卖一半: 向下取整到整手；不足一手按模式处理（sell_all=全卖 / skip=不卖）。"""
    position = max(int(available_position), 0)
    half = position // 2 // LOT_SIZE * LOT_SIZE
    if half >= LOT_SIZE:
        return half
    if insufficient_lot_mode == "skip":
        return 0
    return position


def resolve_auto_buy(
    *,
    available_cash: float,
    total_assets: float,
    buy_count: int,
    price: float,
    max_single_position_pct: float,
    fee_buffer_pct: float = 0.0,
) -> int:
    """自动买入数量: 等分可用资金并受单票上限约束, 向下取整到 100 股。

    注意: 并发执行多只买单时不要直接调用它 —— available_cash 会随兄弟报单
    递减而 buy_count 固定, 等分会退化成等比衰减。执行端改走
    OrderExecutionEngine._per_stock_budget() 的批次预算闩锁, 本函数保留给
    单发买单与单元测试。
    """
    if buy_count <= 0 or price <= 0:
        return 0
    per_stock_budget = min(
        available_cash / buy_count,
        total_assets * max_single_position_pct,
    )
    return shares_for_budget(
        budget=per_stock_budget, price=price, fee_buffer_pct=fee_buffer_pct,
    )


def shares_for_budget(
    *, budget: float, price: float, fee_buffer_pct: float = 0.0
) -> int:
    """把金额预算换算成整手股数, 预留手续费缓冲。

    缓冲是必要的: 柜台校验的是"委托金额 + 佣金/过户费 ≤ 可用资金"。按可用资金
    顶格算出的股数会被判定资金不足而废单, 而重试拿到的是同一个报价 ——
    max_attempts 次尝试会全部烧在同一笔必然失败的委托上。
    """
    if budget <= 0 or price <= 0:
        return 0
    usable = budget * (1.0 - max(0.0, fee_buffer_pct))
    return int(usable / price / LOT_SIZE) * LOT_SIZE

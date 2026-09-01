"""本地策略的纯决策规则：由持仓与行情快照给出下一步动作（卖出/阻塞/不动）。

本模块只做无副作用的规则判定，不接触账户、下单或落库；输入为持仓与行情快照、
对应规则配置，输出 StrategyDecision 或 None（无动作/无持仓）。此外提供开盘买入
等额预算计算。决策原因字符串由本模块生成，供上层落账与日志使用。

本模块约定:
- 所有函数纯计算、无副作用，便于单测与复用；
- 数据校验问题统一返回 BLOCK 决策而非抛异常。
"""

from __future__ import annotations

import datetime as dt

from miniqmt_follower.strategy_config import (
    CapitalAllocationConfig,
    DataSafetyConfig,
    IntradayHardStopConfig,
    LimitDetectionConfig,
    LowOpenExitConfig,
    ProfitReduceConfig,
    TrailingTakeProfitConfig,
)
from miniqmt_follower.strategy_models import (
    AccountSnapshot,
    MarketSnapshot,
    PositionSnapshot,
    StrategyAction,
    StrategyDecision,
)


# ---------------------------------------------------------------------------
# 决策辅助
# ---------------------------------------------------------------------------


def _blocked(reason: str) -> StrategyDecision:
    return StrategyDecision(StrategyAction.BLOCK, reason)


def _market_data_issue(
    market: MarketSnapshot,
    safety: DataSafetyConfig,
    now: dt.datetime,
    *,
    require_open_previous: bool = False,
    require_high_limit: bool = False,
    require_low_limit: bool = False,
) -> str | None:
    """校验行情数据有效性，返回问题描述字符串；无问题返回 None。"""
    if market.last_price <= 0:
        return "行情最新价无效"
    if market.quote_time is None:
        return "行情时间缺失"
    age = (now - market.quote_time).total_seconds()
    if age < -1 or age > safety.max_tick_age_sec:
        return f"行情已过期或时钟异常: age={age:.3f}s"
    if safety.require_current_trade_date and market.trading_date != now.date():
        return "行情交易日期不是当前日期"
    if require_open_previous and safety.require_open_and_previous_close:
        if not market.open_price or not market.previous_close:
            return "开盘价或昨收行情缺失"
    if require_high_limit and market.high_limit is None:
        return "涨停价行情缺失"
    if require_low_limit and market.low_limit is None:
        return "跌停价行情缺失"
    return None


def _is_limit_up(
    market: MarketSnapshot, config: LimitDetectionConfig
) -> bool | None:
    """按容差判定是否触及涨停；涨停价缺失时返回 None。"""
    if market.high_limit is None:
        return None
    return market.last_price >= market.high_limit * (1 - config.tolerance_pct)


def _is_limit_down(
    market: MarketSnapshot, config: LimitDetectionConfig
) -> bool | None:
    """按容差判定是否触及跌停；跌停价缺失时返回 None。"""
    if market.low_limit is None:
        return None
    return market.last_price <= market.low_limit * (1 + config.tolerance_pct)


# ---------------------------------------------------------------------------
# 规则判定
# ---------------------------------------------------------------------------


def decide_opening_exit(
    position: PositionSnapshot,
    market: MarketSnapshot,
    low_open: LowOpenExitConfig,
    limits: LimitDetectionConfig,
    safety: DataSafetyConfig,
    *,
    now: dt.datetime,
    limit_down_enabled: bool = True,
) -> StrategyDecision | None:
    """开盘卖出判定：跌停排队全清或低开阈值全清，数据问题返回 BLOCK，无动作返回 None。"""
    issue = _market_data_issue(market, safety, now)
    if issue:
        return _blocked(issue)
    if position.available_qty <= 0:
        return None
    if limit_down_enabled:
        if limits.require_limit_prices and market.low_limit is None:
            return _blocked("跌停价行情缺失")
        if _is_limit_down(market, limits):
            return StrategyDecision(StrategyAction.SELL_ALL, "开盘达到跌停，排队全清")
    if not low_open.enabled:
        return None
    if not market.open_price or not market.previous_close:
        return _blocked("开盘价或昨收行情缺失")
    issue = _market_data_issue(
        market, safety, now, require_open_previous=True
    )
    if issue:
        return _blocked(issue)
    open_return = market.open_price / market.previous_close - 1
    if open_return < low_open.threshold_pct:
        return StrategyDecision(
            StrategyAction.SELL_ALL,
            f"低开收益率 {open_return:.4%} 低于阈值 {low_open.threshold_pct:.4%}",
        )
    return None


def decide_hard_stop(
    position: PositionSnapshot,
    market: MarketSnapshot,
    config: IntradayHardStopConfig,
    limits: LimitDetectionConfig,
    safety: DataSafetyConfig,
    *,
    now: dt.datetime,
) -> StrategyDecision | None:
    """盘中固定止损判定：现价跌破成本价阈值即全清，涨停且配置跳过时不动作。"""
    if not config.enabled:
        return None
    issue = _market_data_issue(
        market,
        safety,
        now,
        require_high_limit=limits.require_limit_prices and config.skip_if_limit_up,
    )
    if issue:
        return _blocked(issue)
    if position.available_qty <= 0:
        return None
    if position.cost_price <= 0:
        return _blocked("QMT 持仓成本价无效")
    if config.skip_if_limit_up and _is_limit_up(market, limits):
        return None
    stop_price = position.cost_price * (1 - config.loss_pct)
    if market.last_price <= stop_price:
        loss = market.last_price / position.cost_price - 1
        return StrategyDecision(
            StrategyAction.SELL_ALL,
            f"浮动收益率 {loss:.4%} 触发固定止损 {-config.loss_pct:.4%}",
        )
    return None


def decide_trailing_exit(
    position: PositionSnapshot,
    market: MarketSnapshot,
    high_since_buy: float | None,
    config: TrailingTakeProfitConfig,
    safety: DataSafetyConfig,
    *,
    now: dt.datetime,
) -> StrategyDecision | None:
    """回落止盈判定：现价相对买入以来最高价回落达到阈值即全清。

    high_since_buy 由上层维护(当日最高播种 + 跨日 SQLite 记录), 缺失时 BLOCK
    封闭失败; 与成本无关, 买了没涨过的票由硬止损兜底, 本规则自然轮不到。
    """
    if not config.enabled:
        return None
    issue = _market_data_issue(market, safety, now)
    if issue:
        return _blocked(issue)
    if position.available_qty <= 0:
        return None
    if high_since_buy is None or high_since_buy <= 0:
        return _blocked("买入以来最高价缺失")
    trigger_price = high_since_buy * (1 - config.pullback_pct)
    if market.last_price <= trigger_price:
        pullback = market.last_price / high_since_buy - 1
        return StrategyDecision(
            StrategyAction.SELL_ALL,
            f"自最高 {high_since_buy:.2f} 回落 {pullback:.4%} "
            f"触发回落止盈 {-config.pullback_pct:.4%}",
        )
    return None


def decide_morning_exit(
    position: PositionSnapshot,
    market: MarketSnapshot,
    loss_exit: bool,
    profit_reduce: ProfitReduceConfig,
    require_not_limit_up: bool,
    limits: LimitDetectionConfig,
    safety: DataSafetyConfig,
    *,
    now: dt.datetime,
) -> StrategyDecision | None:
    """上午检查卖出判定：未涨停且亏损全清、盈利/持平减半，返回 None 表示不动。"""
    issue = _market_data_issue(
        market,
        safety,
        now,
        require_high_limit=limits.require_limit_prices and require_not_limit_up,
    )
    if issue:
        return _blocked(issue)
    if position.available_qty <= 0:
        return None
    if require_not_limit_up and _is_limit_up(market, limits):
        return None
    if position.cost_price <= 0:
        return _blocked("QMT 持仓成本价无效")
    profit = market.last_price / position.cost_price - 1
    if profit < 0:
        if loss_exit:
            return StrategyDecision(
                StrategyAction.SELL_ALL, f"上午检查未涨停且亏损 {profit:.4%}"
            )
        return None
    if profit_reduce.enabled:
        return StrategyDecision(
            StrategyAction.SELL_HALF,
            f"上午检查未涨停且盈利或持平 {profit:.4%}",
            sell_ratio=profit_reduce.sell_ratio,
        )
    return None


def decide_afternoon_exit(
    position: PositionSnapshot,
    market: MarketSnapshot,
    sell_if_not_limit_up: bool,
    hold_if_limit_up: bool,
    limits: LimitDetectionConfig,
    safety: DataSafetyConfig,
    *,
    now: dt.datetime,
) -> StrategyDecision | None:
    """下午检查卖出判定：未涨停全清，涨停且配置持有则不动作。"""
    issue = _market_data_issue(
        market,
        safety,
        now,
        require_high_limit=limits.require_limit_prices,
    )
    if issue:
        return _blocked(issue)
    if position.available_qty <= 0:
        return None
    if _is_limit_up(market, limits) and hold_if_limit_up:
        return None
    if sell_if_not_limit_up:
        return StrategyDecision(StrategyAction.SELL_ALL, "下午检查未涨停，全清")
    return None


# ---------------------------------------------------------------------------
# 预算计算
# ---------------------------------------------------------------------------


def calculate_equal_budgets(
    candidates: tuple[str, ...],
    account: AccountSnapshot,
    config: CapitalAllocationConfig,
    *,
    single_position_limit_pct: float,
) -> dict[str, float]:
    """按批次快照固定每票预算；后续失败不在兄弟候选间重新分配。

    single_position_limit_pct 即 config.strategy.yaml 的
    execution.max_single_position_pct(单一键): 这里把它当"单标的持仓上限"
    使用 —— 预算 = min(等分额, 总资产×比例 − 该标的现有持仓市值)。
    """
    if not config.enabled:
        return {}
    positions = {position.code: position for position in account.positions}
    effective = tuple(
        code
        for code in candidates
        if not (
            config.skip_existing_positions
            and (positions.get(code) is not None)
            and positions[code].total_qty > 0
        )
    )
    if not effective:
        return {}
    available = max(0.0, account.available_cash)
    if config.total_position_limit_pct > 0:
        remaining = (
            account.total_assets * config.total_position_limit_pct
            - account.market_value
        )
        available = min(available, max(0.0, remaining))
    if config.cash_reserve_pct > 0:
        available *= 1 - config.cash_reserve_pct
    equal_budget = available / len(effective)
    budgets: dict[str, float] = {}
    for code in effective:
        budget = equal_budget
        if single_position_limit_pct > 0:
            current_value = positions.get(code).market_value if code in positions else 0.0
            single_remaining = (
                account.total_assets * single_position_limit_pct
                - current_value
            )
            budget = min(budget, max(0.0, single_remaining))
        if budget > 0:
            budgets[code] = budget
    return budgets


def calculate_topup_budgets(
    candidates: tuple[str, ...],
    account: AccountSnapshot,
    pool: float,
    config: CapitalAllocationConfig,
    *,
    single_position_limit_pct: float,
) -> dict[str, float]:
    """把回笼卖款在候选间等分补仓：受单票仓位上限与现金预留约束。

    与第一波等额预算的区别: 不按可用资金全额分配、不应用总仓位上限 ——
    补仓池就是卖款回笼的增量, 总市值因卖单已下降, 总上限自然满足;
    skip_existing_positions 不适用(补仓对象本来就是第一波已买入的持仓)。
    single_position_limit_pct 语义同 calculate_equal_budgets, 扣除现有持仓
    市值计算剩余空间。
    """
    if not config.enabled or pool <= 0 or not candidates:
        return {}
    available = max(0.0, pool)
    if config.cash_reserve_pct > 0:
        available *= 1 - config.cash_reserve_pct
    if available <= 0:
        return {}
    positions = {position.code: position for position in account.positions}
    equal_budget = available / len(candidates)
    budgets: dict[str, float] = {}
    for code in candidates:
        budget = equal_budget
        if single_position_limit_pct > 0:
            current_value = (
                positions[code].market_value if code in positions else 0.0
            )
            room = max(
                0.0,
                account.total_assets * single_position_limit_pct
                - current_value,
            )
            budget = min(budget, room)
        if budget > 0:
            budgets[code] = budget
    return budgets


# 补仓波次的最小下单门槛余量: 委托价可上浮到最新价×(1+quote_band_pct)
# (示例 1.5%), 且执行端把预算按最新价向下取整到整手; 预算低于
# 一手市值×(1+余量)的候选本轮跳过, 资金留池下 tick 累积, 避免零股废单。
_TOPUP_WAVE_MIN_PRICE_MARGIN = 0.02
_TOPUP_WAVE_LOT_SIZE = 100


def filter_deployable_topup_budgets(
    budgets: dict[str, float],
    last_prices: dict[str, float],
    *,
    min_price_margin: float = _TOPUP_WAVE_MIN_PRICE_MARGIN,
    lot_size: int = _TOPUP_WAVE_LOT_SIZE,
) -> dict[str, float]:
    """过滤出本波至少能买到一手的补仓预算; 其余资金留池累积。

    纯函数: 输入预算与最新价, 输出可部署子集。多波次补仓用——波次内不足
    一手的候选不提交(执行端会把预算向下取整到 0 股白占一波信号), 而是把
    钱留在池子里等下一 tick 与新增回款合并后再分配。
    """
    deployable: dict[str, float] = {}
    for code, budget in budgets.items():
        price = last_prices.get(code, 0.0)
        if price <= 0:
            continue
        if budget >= price * lot_size * (1 + min_price_margin):
            deployable[code] = budget
    return deployable

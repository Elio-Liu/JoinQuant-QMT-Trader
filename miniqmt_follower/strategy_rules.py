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
import logging

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
from miniqmt_follower.pricing import tick_size_for

logger = logging.getLogger(__name__)

# 主板 10% 涨停价基准 (涨停豁免的"昨日封板"判定)。本策略候选只在沪深主板
# (聚宽侧已过滤创业板/科创板/ST), 与聚宽侧 check_auction_stop_loss 同口径。
_MAIN_BOARD_LIMIT_UP_RATIO = 1.1


# ---------------------------------------------------------------------------
# 决策辅助
# ---------------------------------------------------------------------------


def _blocked(reason: str) -> StrategyDecision:
    return StrategyDecision(StrategyAction.BLOCK, reason)


def price_sealed_at_limit(market: MarketSnapshot) -> bool:
    """价格是否钉死在涨跌停价且对方盘口为空(封板冻结)。

    涨停封死 = 价格贴在涨停价且卖一为空; 跌停封死 = 价格贴在跌停价且买一为空。
    封板期间没有成交、价格不会离开限价; 一旦开板, 首笔成交产生新 tick,
    时效自然恢复 —— 封板快照的"超龄"不携带信息量, 时效门控可放宽
    (2026-09-15 elio_hx 复盘: 涨停票 tick 节奏 10s+, 3s 门控下决策掷硬币)。

    判定要点:
    - 对方盘口为空是强信号: 普通清淡票价格不在限价上, 不会命中;
    - 价格容差取 0.5 tick: 吸收浮点噪声, 同时保证距限价 1 tick 的非封板价不豁免;
    - 涨跌停价缺失(静态信息查询失败)时返回 False, 门控走常规路径(fail-closed)。
    """
    half_tick = tick_size_for(market.code) / 2
    if (
        market.high_limit is not None
        and market.last_price >= market.high_limit - half_tick
        and market.ask1 is None
    ):
        return True
    if (
        market.low_limit is not None
        and market.last_price <= market.low_limit + half_tick
        and market.bid1 is None
    ):
        return True
    return False


def _market_data_issue(
    market: MarketSnapshot,
    safety: DataSafetyConfig,
    now: dt.datetime,
    *,
    require_open_previous: bool = False,
    require_high_limit: bool = False,
    require_low_limit: bool = False,
    max_age_override: float | None = None,
) -> str | None:
    """校验行情数据有效性，返回问题描述字符串；无问题返回 None。

    时效门控分层(2026-09-15 elio_hx 复盘):
    - 默认(普通规则) = max_tick_age_sec × normal_rule_age_slack —— 低成交票
      自然 tick 节奏约 3s, 基准 3s 恰好卡刀口, 普通规则给 2 倍余量防掷硬币;
    - max_age_override: 保护类规则(硬止损)用它放宽容忍度 —— 保命决策宁可用
      稍旧的价格判断, 也不能被"行情超龄"整体 BLOCK;
    - 封板快照(price_sealed_at_limit)在两者之上再取 max(sealed 档): 价格钉死
      在限价, 超龄无信息量, 涨停/跌停票的慢节奏不应让规则空转。
    """
    if market.last_price <= 0:
        return "行情最新价无效"
    if market.quote_time is None:
        return "行情时间缺失"
    age = (now - market.quote_time).total_seconds()
    if max_age_override is None:
        max_age = safety.max_tick_age_sec * safety.normal_rule_age_slack
    else:
        max_age = max_age_override
    if price_sealed_at_limit(market):
        max_age = max(max_age, safety.max_tick_age_sec * safety.sealed_quote_age_slack)
    if age < -1 or age > max_age:
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


def yesterday_sealed_limit_up(
    previous_close: float | None,
    prev_prev_close: float | None,
    tolerance_pct: float,
) -> bool | None:
    """昨日收盘是否封板: 昨收 ≥ round(T-2收盘×1.1, 2)×(1-容差)。

    prev_prev_close / previous_close 缺失返回 None(数据不足, 调用方 fail-closed)。
    封板判定用 T-2 收盘 ×1.1 —— 不能用当日 high_limit(它是今昨收×1.1, 恒 False,
    2026-09 已踩坑)。涨停豁免决策与盘前预热日志共用本函数, 保持单一公式。
    """
    if not prev_prev_close or not previous_close:
        return None
    return previous_close >= round(
        prev_prev_close * _MAIN_BOARD_LIMIT_UP_RATIO, 2
    ) * (1 - tolerance_pct)


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
    prev_prev_close: float | None = None,
) -> StrategyDecision | None:
    """开盘卖出判定：跌停排队全清或低开阈值全清，数据问题返回 BLOCK，无动作返回 None。

    prev_prev_close = 前日收盘 (T-2): 涨停豁免用它判定"昨日(T-1)是否封板"
    (昨收 ≥ 前日收盘×10%涨停价)。缺失时豁免不生效, 退化为普通低开卖出 (fail-closed)。
    """
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
        if low_open.sealed_exempt.enabled and prev_prev_close:
            # 涨停豁免 (与聚宽侧同规则, 2026-09 复盘: 昨日涨停+次日微低开
            # 竞价砍仓55笔全部卖飞, 豁免后净值+3.7%): 昨日收盘封板且低开
            # 幅度≤容忍度 → 不做低开卖出, 交10:00早盘规则与盘中硬止损。
            sealed_yesterday = yesterday_sealed_limit_up(
                market.previous_close, prev_prev_close, limits.tolerance_pct
            )
            if (
                sealed_yesterday
                and open_return >= -low_open.sealed_exempt.gap_tolerance_pct
            ):
                logger.info(
                    "【策略】🕊️ 竞价豁免｜%s | 昨日涨停且低开%.2f%%≤容忍度%.1f%%"
                    "｜不竞价止损, 交10:00规则",
                    market.code,
                    open_return * 100,
                    low_open.sealed_exempt.gap_tolerance_pct * 100,
                )
                return None
        return StrategyDecision(
            StrategyAction.SELL_ALL,
            f"低开收益率 {open_return:.4%} 低于阈值 {low_open.threshold_pct:.4%}",
        )
    return None


# 硬止损对行情超龄的放宽倍数(2026-09-09 复盘): 集泰股份的行情在双机上
# 稳定滞后 ~3.0s, 恰好卡在 max_tick_age_sec=3 的刀口上, 决策变成掷硬币;
# 硬止损是保命规则, 时效门控放宽到 5 倍(3s→15s), 宁可按稍旧价格判断,
# 也不能被系统性滞后 BLOCK 成裸奔。超过放宽线仍照旧 BLOCK。
_HARD_STOP_STALE_AGE_SLACK = 5.0


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
        max_age_override=safety.max_tick_age_sec * _HARD_STOP_STALE_AGE_SLACK,
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
    """补仓池等分，受实时现金、总仓位、单票仓位和现金预留约束。

    补仓允许已有持仓；单票预算扣除其当前市值，未分配金额继续留池。
    """
    if not config.enabled or pool <= 0 or not candidates:
        return {}
    available = min(max(0.0, pool), max(0.0, account.available_cash))
    if config.total_position_limit_pct > 0:
        remaining = account.total_assets * config.total_position_limit_pct - account.market_value
        available = min(available, max(0.0, remaining))
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
    min_lots: int = 1,
) -> dict[str, float]:
    """过滤出本波至少能买到 min_lots 手的补仓预算; 其余资金留池累积。

    纯函数: 输入预算与最新价, 输出可部署子集。多波次补仓用——波次内不足
    一手(或 min_lots 手)的候选不提交(执行端会把预算向下取整到 0 股白占
    一波信号), 而是把钱留在池子里等下一 tick 与新增回款合并后再分配。
    2026-09-09 复盘: min_lots>1 时, 单票上限接近饱和的小账户不再每个
    tick 买 100 股碎单(见缝插针), 留池安静等待, 直到凑够一个像样的波次。
    """
    deployable: dict[str, float] = {}
    for code, budget in budgets.items():
        price = last_prices.get(code, 0.0)
        if price <= 0:
            continue
        if budget >= price * lot_size * min_lots * (1 + min_price_margin):
            deployable[code] = budget
    return deployable

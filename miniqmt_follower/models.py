from __future__ import annotations

import hashlib
from dataclasses import dataclass, replace
from enum import StrEnum


class Action(StrEnum):
    """聚宽侧传来的交易方向。"""

    BUY = "buy"
    SELL = "sell"


class BrokerOrderStatus(StrEnum):
    """券商/miniQMT 订单状态的内部统一表达。"""

    OPEN = "open"
    PARTIALLY_FILLED = "partially_filled"
    FILLED = "filled"
    CANCELED = "canceled"
    REJECTED = "rejected"


class BrokerRejectionKind(StrEnum):
    """已确认不会成交的券商废单原因分类。"""

    PRICE = "price"
    RESOURCE = "resource"
    TRANSIENT = "transient"
    UNKNOWN = "unknown"
    HARD_STOP = "hard_stop"


class BrokerOrderRejected(RuntimeError):
    """同步下单明确未受理，可按拒单分类决定是否刷新重试。"""

    def __init__(
        self,
        reason: str,
        *,
        error_code: str | None = None,
        kind: BrokerRejectionKind = BrokerRejectionKind.UNKNOWN,
    ):
        super().__init__(reason)
        self.reason = reason
        self.error_code = error_code
        self.kind = kind


class BrokerSubmissionUncertain(RuntimeError):
    """下单调用异常，无法确认券商是否已经受理委托。"""


class ExecutionStatus(StrEnum):
    """执行引擎落库的信号级状态, 用于幂等、恢复和盘后审计。"""

    RECEIVED = "received"
    ACCEPTED = "accepted"
    DUPLICATE_IGNORED = "duplicate_ignored"
    ORDER_SUBMITTED = "order_submitted"
    FILLED = "filled"
    EXPIRED = "expired"
    FAILED_TIMEOUT = "failed_timeout"
    PARTIALLY_FILLED_TIMEOUT = "partially_filled_timeout"
    FAILED_RISK = "failed_risk"
    FAILED_BROKER = "failed_broker"
    SKIPPED_NO_POSITION = "skipped_no_position"
    # sell_half 意图信号半仓不足一手、且配置为 skip 时的终态（与无持仓区分开）。
    SKIPPED_SMALL_POSITION = "skipped_small_position"
    # 涨跌停无对手盘时的快速跳过终态: 挂单只会排进涨跌停价的巨量队列,
    # 占满超时预算和同方向 worker, 不如直接跳过并留痕供盘后对账。
    SKIPPED_LIMIT_DOWN = "skipped_limit_down"
    SKIPPED_LIMIT_UP = "skipped_limit_up"
    # 跌停排队卖出 (limit_down_sell_mode=queue): 挂跌停价等待开板。
    # QUEUED_LIMIT_DOWN 是中间态(排队中, 供盘中观测), LIMIT_DOWN_QUEUE_EXPIRED
    # 是终态(排到截止时间仍一股未成)。部分成交后到期仍走 PARTIALLY_FILLED_TIMEOUT。
    QUEUED_LIMIT_DOWN = "queued_limit_down"
    LIMIT_DOWN_QUEUE_EXPIRED = "limit_down_queue_expired"
    QUEUED_LIMIT_UP = "queued_limit_up"
    LIMIT_UP_QUEUE_EXPIRED = "limit_up_queue_expired"
    # 重启核对时无法确认券商是否已有订单：保留 Redis 待办并阻止自动重下。
    RECOVERY_REQUIRED = "recovery_required"


TERMINAL_EXECUTION_STATUSES = frozenset(
    {
        ExecutionStatus.FILLED,
        ExecutionStatus.EXPIRED,
        ExecutionStatus.FAILED_TIMEOUT,
        ExecutionStatus.PARTIALLY_FILLED_TIMEOUT,
        ExecutionStatus.FAILED_RISK,
        ExecutionStatus.FAILED_BROKER,
        ExecutionStatus.SKIPPED_NO_POSITION,
        ExecutionStatus.SKIPPED_SMALL_POSITION,
        ExecutionStatus.SKIPPED_LIMIT_DOWN,
        ExecutionStatus.SKIPPED_LIMIT_UP,
        ExecutionStatus.LIMIT_DOWN_QUEUE_EXPIRED,
        ExecutionStatus.LIMIT_UP_QUEUE_EXPIRED,
    }
)


def is_terminal_execution_status(status: ExecutionStatus) -> bool:
    """只有明确不会再触达券商的状态，才允许确认对应 Redis 消息。"""
    return status in TERMINAL_EXECUTION_STATUSES


_CONSOLE_EVENT_EMOJIS = {
    "重复": "⏭️",
    "停止": "🛑",
    "风控": "⚠️",
    "竞价": "⏳",
    "超时": "⏰",
    "过期": "⌛",
    "失败": "❌",
    "成交": "✅",
    "重试": "🔁",
    "跳过": "⛔",
}

# 买卖方向的终端 emoji: 买入向上(📈), 卖出向下(📉), 一眼区分进出方向。
_ACTION_EMOJIS: dict[Action, str] = {
    Action.BUY: "📈",
    Action.SELL: "📉",
}


def format_stock_label(code: str, stock_name: str | None = None) -> str:
    """日志展示用证券标识：中文名(六码)，取不到名称时回退六码。"""
    short_code = str(code).split(".", 1)[0]
    return f"{stock_name}({short_code})" if stock_name else short_code


@dataclass(frozen=True)
class TradeSignal:
    """一条来自 Redis Stream 的订单信号。

    reference_price 是策略参考价, 真实委托价由执行端根据最新行情和滑点配置计算。
    sent_at_ms 是聚宽侧发送时刻的毫秒时间戳, 仅用于延迟测量, 缺省时相关日志跳过。
    """

    signal_id: str
    strategy_id: str
    action: Action
    code: str
    amount: int
    reference_price: float
    created_at: str
    mode: str = "live"
    sent_at_ms: int | None = None
    expire_at: str | None = None
    # 意图型信号的数量模式: exact=amount 精确(旧协议) / auto_buy=按真实资金计算 /
    # sell_half=按真实持仓卖一半 / sell_all=按真实持仓全清。非 exact 时 amount 为 0 占位,
    # 由执行端在执行前解析成具体股数。
    quantity_mode: str = "exact"
    # auto_buy 的等分基数（待买只数）；派生信号由 PlanExecutor 填充，缺省按 1 处理。
    budget_group_size: int | None = None
    # 证券中文名, 由执行端启动/收单时从行情适配器解析一次后填充;
    # 为空时日志只显示代码。不参与信号契约, from_dict 不会解析它。
    stock_name: str = ""

    @property
    def label(self) -> str:
        """日志用简短标识: 中文名(六码)+方向+数量, 比完整 signal_id 更易扫读。

        完整 signal_id 仍以 DEBUG 级别记录（只进文件不上控制台), 需要精确核对
        SQLite 记录或排查幂等去重时可以从文件日志里找。
        """
        return f"{self.display_code} {self.action.value} {self.amount}股"

    @property
    def action_label(self) -> str:
        """终端展示用买卖方向。"""
        return "买单" if self.action == Action.BUY else "卖单"

    @property
    def task_id(self) -> str:
        """完整 signal_id 的稳定四位短标识，便于终端和文件日志串联。"""
        return hashlib.blake2s(self.signal_id.encode("utf-8"), digest_size=2).hexdigest().upper()

    @property
    def display_code(self) -> str:
        """日志用展示标识: 有中文名时显示 名称(代码), 否则仅显示代码。"""
        return format_stock_label(self.code, self.stock_name)

    def with_stock_name(self, stock_name: str | None) -> "TradeSignal":
        """返回补上中文名的新实例; 名字为空或解析失败时保持原样。"""
        if not stock_name or stock_name == self.stock_name:
            return self
        return replace(self, stock_name=stock_name)

    @property
    def console_prefix(self) -> str:
        return f"【{self.action_label}】{_ACTION_EMOJIS[self.action]} 信号#{self.task_id}"

    def console_event(self, event: str) -> str:
        emoji = _CONSOLE_EVENT_EMOJIS.get(event, "ℹ️")
        return f"【{self.action_label}】{emoji} 信号#{self.task_id}"

    @classmethod
    def from_dict(cls, raw: dict) -> "TradeSignal":
        # 兼容旧字段 price, 便于从早期 publish 版本平滑迁移。
        reference_price = raw.get("reference_price", raw.get("price"))
        if reference_price is None:
            raise ValueError("signal requires reference_price or price")

        action_raw = str(raw["action"]).lower()
        quantity_mode = str(raw.get("quantity_mode") or "exact").lower()
        if quantity_mode not in ("exact", "auto_buy", "sell_half", "sell_all"):
            raise ValueError(f"invalid quantity_mode: {quantity_mode}")
        # 协议映射: sell_half/sell_all → SELL + 对应模式；buy 无 amount → auto_buy。
        if action_raw in ("sell_half", "sell_all"):
            action = Action.SELL
            quantity_mode = action_raw
        elif action_raw == "buy" and raw.get("amount") is None:
            action = Action.BUY
            quantity_mode = "auto_buy"
        else:
            action = Action(action_raw)

        amount_raw = raw.get("amount")
        if quantity_mode == "exact":
            if amount_raw is None:
                raise ValueError("signal requires amount for exact quantity")
            amount = int(amount_raw)
        else:
            amount = int(amount_raw) if amount_raw is not None else 0

        sent_at_ms = raw.get("sent_at_ms")
        expire_at = raw.get("expire_at")
        budget_raw = raw.get("budget_group_size")
        return cls(
            signal_id=str(raw["signal_id"]),
            strategy_id=str(raw["strategy_id"]),
            action=action,
            code=str(raw["code"]),
            amount=amount,
            reference_price=float(reference_price),
            created_at=str(raw.get("created_at") or raw.get("timestamp") or ""),
            mode=str(raw.get("mode", "live")),
            sent_at_ms=int(sent_at_ms) if sent_at_ms is not None else None,
            expire_at=str(expire_at) if expire_at else None,
            quantity_mode=quantity_mode,
            budget_group_size=int(budget_raw) if budget_raw is not None else None,
        )


@dataclass(frozen=True)
class Quote:
    """一次行情快照: 最新成交价 + 买一/卖一。

    ask1/bid1 为 None 表示该侧盘口不可得(涨跌停单边无档、行情源未提供等),
    定价时应回退到 last_price 滑点模式。
    high_limit/low_limit 是当日涨跌停价(来自静态合约信息, QMT tick 不含),
    为 None 表示行情源取不到 —— 跌停排队卖出等依赖该价格的功能应回退保守路径。
    """

    last_price: float
    ask1: float | None = None
    bid1: float | None = None
    high_limit: float | None = None
    low_limit: float | None = None


@dataclass(frozen=True)
class ExecutionConfig:
    """执行参数, 全部来自配置文件, 方便盘中调参后重启生效。

    pricing_mode:
    - "slippage": 最新成交价 ± 固定百分比滑点(原有行为)。
    - "book": 盘口价定价, 买入=卖一价+book_tick_offset个tick, 卖出=买一价-offset,
      追求首次挂单即成交; 对应盘口缺失时自动回退 slippage 模式。
    """

    buy_slippage_pct: float = 0.003
    sell_slippage_pct: float = 0.003
    order_timeout_sec: float = 3.0
    max_attempts: int = 3
    max_total_duration_sec: float = 15.0
    # 撤单后等待券商回报真实终态的最长时间, 独立于 max_total_duration_sec。
    # 必须独立: 重挂节奏由 order_timeout_sec 控制, 最后一次尝试必然贴着总预算边界,
    # 若撤单确认也从总预算里扣, 它一开始就是超时的 —— 会把一次普通的未成交撤单
    # 误判成"撤单终态不明", 触发全局停止交易。撤单确认必须等到终态才能知道成交了
    # 多少, 否则无法安全重挂, 所以本就该豁免总时长限制。
    cancel_confirm_timeout_sec: float = 30.0
    poll_interval_sec: float = 0.2
    pricing_mode: str = "slippage"
    book_tick_offset: int = 2
    # 9:15~9:30 排队委托的激进报价幅度 (买 = 最新价×(1+pct), 卖 = 最新价×(1-pct))。
    # 这些委托不参与 9:25 的开盘集合竞价 (开盘价那时已定), 而是排队等 9:30 连续
    # 竞价开撮 —— 成交价按对手方挂单价逐档确定, 所以激进报价买的是"时间优先排位",
    # 不是更差的成交价。风险只在吃不完的剩余会留在报价上, 故幅度要远小于涨跌停带,
    # 且结果强制夹进 [跌停价, 涨停价]。设为 0 关闭该行为, 回退常规 book/slippage。
    auction_aggressive_pct: float = 0.02
    # 跌停无买盘的卖单 / 涨停无卖盘的买单直接跳过 (SKIPPED_* 终态),
    # 防止排队委托占满超时预算和同方向 worker。默认关闭以兼容无盘口行情源。
    skip_sell_when_limit_down: bool = False
    skip_buy_when_limit_up: bool = False
    # 跌停锁盘时 SELL 的处理模式:
    #   ""(缺省) — 由旧开关推导: skip_sell_when_limit_down=true → "skip", 否则 "none"
    #   "skip"  — 直接 SKIPPED_LIMIT_DOWN 终态 (等价旧开关行为)
    #   "queue" — 挂跌停价排队至截止时间, 开板即按价格优先成交; 不撤不重挂
    #             (重挂丢队列位置), 豁免 order_timeout/max_attempts/总时长与偏离度守卫
    #   "none"  — 旧回退路径: 照常按滑点定价下单
    limit_down_sell_mode: str = ""
    queue_sell_poll_interval_sec: float = 3.0
    queue_sell_deadline: str = "14:56:30"
    # 排队单会占用 worker 线程直到成交或截止; 超过该并发数的新排队请求降级为 skip,
    # 确保始终有 worker 留给正常信号。建议 ≤ --workers 减 2。
    max_concurrent_queue_sells: int = 2
    # 涨停锁盘时 BUY 的处理模式；留空时由旧 skip_buy_when_limit_up 开关推导。
    # queue 模式只在行情同时确认涨停价与卖一空档时启用，挂涨停价保留队列位置。
    limit_up_buy_mode: str = ""
    queue_buy_deadline: str = "14:56:30"
    max_concurrent_queue_buys: int = 5
    # 意图型信号（日计划）参数。
    plan_enabled: bool = True       # 是否启用日计划执行（无 plan 消息时不影响旧协议）
    plan_execute_at: str = "09:30:00"  # plan 执行时刻（HH:MM:SS，交易机本地时间）
    # 单票买入金额上限 = 账户总资产（现金+股票市值）× 该比例；取值范围 (0, 1]。
    #
    # 这是单票集中度风控, 不是等分逻辑的一部分。等分负责"把资金摊到几只票上",
    # 它负责"无论摊给几只, 单票都不能太重"。目标股 ≥3 只时 可用资金/N 天然
    # 低于它、不会生效; 只有 1~2 只时才真正咬合 —— 而那恰恰是最需要它的场景:
    # 满仓押一只连板股, 当天高开跳水就是一次不可接受的回撤。
    #
    # 0.5 = 单票市值不超过总资产一半。调这个值等于调"最坏情况下单票能伤多深"。
    max_single_position_pct: float = 0.5
    # 买入可用资金的手续费缓冲: 委托金额上限按 可用资金×(1-该值) 计算。
    # 不留缓冲时"用满可用资金"的委托会因佣金/过户费被柜台判定资金不足而废单,
    # 然后把 max_attempts 次重试全烧在同一个必然失败的报价上。
    cash_fee_buffer_pct: float = 0.003
    # sell_half 意图信号在半仓取整不足一手(<200股)时的处理:
    #   "sell_all" — 全卖当前持仓(默认, 原行为)
    #   "skip"     — 不卖, 记 SKIPPED_SMALL_POSITION 终态
    sell_half_insufficient_lot_mode: str = "sell_all"
    # 信号过期秒数: 执行端按 sent_at_ms(发送时刻毫秒) + 该值判断是否已过期;
    # 0 = 不过期。旧协议 expire_at 绝对时间字段仍优先兼容。
    signal_expire_seconds: int = 600

    def effective_limit_down_sell_mode(self) -> str:
        """归一化跌停卖出模式; 缺省时由旧开关 skip_sell_when_limit_down 推导。"""
        if self.limit_down_sell_mode:
            return self.limit_down_sell_mode
        return "skip" if self.skip_sell_when_limit_down else "none"

    def effective_limit_up_buy_mode(self) -> str:
        """归一化涨停买入模式；缺省时保留旧开关语义。"""
        if self.limit_up_buy_mode:
            return self.limit_up_buy_mode
        return "skip" if self.skip_buy_when_limit_up else "none"


@dataclass(frozen=True)
class OrderSnapshot:
    """某个券商订单在一次查询时的快照。filled_qty 是该订单自身的累计成交量。"""

    order_id: str
    status: BrokerOrderStatus
    filled_qty: int = 0
    rejection_reason: str | None = None
    rejection_code: str | None = None
    rejection_kind: BrokerRejectionKind | None = None
    quantity: int = 0
    price: float = 0.0


@dataclass(frozen=True)
class ExecutionResult:
    """一条信号最终执行结果, 用于日志、测试和未来执行回报。"""

    signal_id: str
    status: ExecutionStatus
    requested_qty: int
    filled_qty: int
    attempts: int
    message: str = ""


@dataclass(frozen=True)
class StoredSignal:
    """SQLite 中已接收信号的最小读取模型。"""

    signal_id: str
    status: ExecutionStatus
    filled_qty: int


@dataclass(frozen=True)
class StoredAttempt:
    """重启核对所需的单笔委托最小记录。"""

    attempt_no: int
    broker_order_id: str
    quantity: int
    price: float
    status: str
    filled_qty: int


@dataclass(frozen=True)
class DailyPlan:
    """日计划: 开盘清仓清单 + 今日待买清单（signal_id 按日期幂等）。"""

    signal_id: str
    strategy_id: str
    codes_to_sell: tuple[str, ...]
    codes_to_buy: tuple[str, ...]
    created_at: str
    mode: str = "live"
    sent_at_ms: int | None = None

    @classmethod
    def from_dict(cls, raw: dict) -> "DailyPlan":
        action = str(raw.get("action", "")).lower()
        if action != "plan":
            raise ValueError(f"not a plan message: action={action!r}")
        raw_sell = raw.get("codes_to_sell", [])
        raw_buy = raw.get("codes_to_buy", [])
        if not isinstance(raw_sell, (list, tuple)):
            raise ValueError("codes_to_sell 必须是列表")
        if not isinstance(raw_buy, (list, tuple)):
            raise ValueError("codes_to_buy 必须是列表")
        sent_at_ms = raw.get("sent_at_ms")
        return cls(
            signal_id=str(raw["signal_id"]),
            strategy_id=str(raw["strategy_id"]),
            codes_to_sell=tuple(str(code) for code in raw_sell),
            codes_to_buy=tuple(str(code) for code in raw_buy),
            created_at=str(raw.get("created_at") or ""),
            mode=str(raw.get("mode", "live")),
            sent_at_ms=int(sent_at_ms) if sent_at_ms is not None else None,
        )

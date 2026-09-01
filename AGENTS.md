# AGENTS.md

This file provides guidance to Codex (Codex.ai/code) when working with code in this repository.

## Project Overview

JoinQuant miniQMT Live Follower — a two-sided system where a JoinQuant cloud strategy sends trade signals via Redis Stream, and a Windows-side service consumes them to execute real orders through miniQMT/xtquant.

## Commands

```bash
# Run all tests (tests/ is a local-only suite, not tracked in this repo)
python -m unittest discover -v

# Compile-check runtime and tests (catches ImportErrors before runtime; tests/ is local-only)
python -m compileall miniqmt_follower bigqmt_follower tests

# Run the Windows execution service
python main.py
python main.py --config config.yaml --workers 8

# Run a single test module
python -m unittest tests.test_executor -v
```

## Architecture

### Two-side split

This project contains both sides of the system. Local strategy deployment copies are intentionally kept outside version control because they may embed environment-specific Redis credentials.

1. **JoinQuant side** (cloud strategy): the sender functions are documented in the standalone `joinquant_signal_sender.py` (placeholder Redis config, no imports from `miniqmt_follower/`). Copy that file into the JoinQuant strategy and fill in the real Redis config before deployment; private strategy deployment copies embed these functions with real credentials and stay outside version control. They serialize a signal as JSON, write it to Redis Stream via `XADD`, and return immediately (fire-and-forget). The five-argument `publish_trade_signal_to_redis(context, action, code, amount, price)` is frozen — exact buy/sell call sites stay untouched. The file also provides `publish_watchlist_to_redis(context, codes)`, `publish_daily_plan_to_redis(context, codes_to_sell, codes_to_buy)`, `publish_sell_half_to_redis(context, code, price)`, and `publish_sell_all_to_redis(context, code, price)`.

2. **Windows side** (this repo's runtime): `miniqmt_follower/` — a long-running service (`python main.py`) that consumes Redis Stream via consumer group with `XREADGROUP`, executes orders through miniQMT, and ACKs messages only after the order reaches a terminal state.

Two run modes are selected by the presence of `config.strategy.yaml` next to `main.py`:

- **Local strategy engine mode** (file present, the development mainline): strict single-strategy contract (see below), `candidate_plan` driven; `load_runtime_with_strategy` returns the `StrategyEngineConfig` and rejects migrated execution keys in the machine config.
- **Pure follower mode** (file absent): `load_runtime_with_strategy` returns `strategy=None`; no local engine is built, `local_strategy_id=None`, `_dispatch_message` skips the local-strategy rejection and ACK-drops `candidate_plan` (no consumer); whitelisted strategies' ordinary `plan/buy/sell/sell_half/sell_all/watchlist` messages execute normally, the strategy whitelist may contain multiple ids, and the migrated execution keys may live in the machine config again (machine defaults otherwise).

`strategies/` deployment copies are candidate-plan-first on the wire: they keep their full paper-trading logic (auction stop-loss / opening buys / morning & afternoon exits / fixed stops / intraday top-exit checks) and publish three kinds of messages — `candidate_plan` right after selection (with timed retries of the same frozen list), plus two kinds of `sell_all` exit signals whose `purpose` labels are decided by the strategy side (both retried every minute from a pending queue until XADD succeeds). The primary fixed stop is decided by the JoinQuant paper cost basis; the trading box's local engine runs its own hard stop only as a disaster fallback with a widened threshold configured in `config.strategy.yaml` (`intraday_hard_stop.enabled/loss_pct`) that takes over when the signal chain is lost — the two mechanisms coexist safely (first sell wins, the later one ends `SKIPPED_NO_POSITION`). In engine mode, `app._reject_local_strategy_ordinary_message` whitelists only `sell_all` whose `purpose` is listed in `config.strategy.yaml`'s `strategy_engine.external_signals.allowed_sell_purposes` from the local strategy id, everything else is ACK-rejected. All other buy/sell decisions are made by the trading box's local strategy engine per `config.strategy.yaml`.

### Execution pipeline (Windows side)

```
Redis Stream → RedisStreamClient.read_forever()
    → app.py filters by redis.allowed_strategy_ids (non-empty ⇒ unknown strategies are logged + ACKed, never executed)
    → plan messages route to PlanExecutor: derived sell_all/auto_buy signals enter the same pools
    → app.py routes each signal to a per-direction ThreadPoolExecutor (separate buy/sell pools, --workers threads each, default 8)
    → pre-open SELL signals from `machine_schedule.market_session.preopen_sell_start_at` (example `09:25:00`) until `machine_schedule.market_session.continuous_trading_start_at` (example `09:30:00`) register in OpeningSellBarrier; BUY workers wait for its release
    → OrderExecutionEngine.execute()
        → SQLiteExecutionStore.try_accept_signal()  [idempotency gate: INSERT OR IGNORE on signal_id PK]
        → resolve intent quantities via sizing.py (sell_all / sell_half / auto_buy) or use the exact amount
        → MarketDataAdapter.latest_quote()  [last price + ask1/bid1]
        → pricing.calculate_order_price()  [auction-queue / order-book / slippage pricing, plus the A-share price cage]
        → query fresh resources: BUY → available cash, SELL → available position
        → BrokerAdapter.submit_order()
        → poll BrokerAdapter.get_order_snapshot() until terminal or timeout
        → if non-terminal: request cancel → wait for confirmed terminal state → reconcile final fill
        → if quantity remains: fetch fresh quote/resources → re-submit remaining qty
        → SQLiteExecutionStore.update_signal_status()  [terminal state]
    → RedisStreamClient.ack()  [only after the worker's future completes]
```

Key design decisions in this pipeline:
- **Idempotency**: SQLite `signals` table primary key is `signal_id`. Duplicate signals return `DUPLICATE_IGNORED` with the existing filled quantity — no second order ever reaches the broker.
- **Resource capping fails closed**: if the fresh cash/position query fails, no order is submitted. BUY cash query, 100-share-lot capping, and broker submission are serialized as one compound operation; order polling remains parallel. SELL quantities cap to the latest closeable position, and zero live position ends as `SKIPPED_NO_POSITION` without an order.
- **Each attempt refreshes state**: every broker submission uses a fresh quote and fresh MiniQMT cash/position query.
- **Cancel confirmation**: a cancel request is not treated as completion. The engine waits for `FILLED`, `CANCELED`, or `REJECTED`, then reconciles fills that arrive during cancellation before re-submitting. If the cancel state remains uncertain, the process latches a trading halt and later queued signals cannot submit.
- **Adaptive polling**: `_wait_for_terminal_or_timeout` polls at ≤50ms for the first second, then falls back to `poll_interval_sec`.
- **Opening SELL barrier**: pre-open SELL signals are submitted in parallel, while BUY execution waits until `machine_schedule.market_session.continuous_trading_start_at` (example `09:30:00`) and every ordinary pre-open SELL has completed or explicitly terminated. After that configured boundary, the first ordinary SELL attempt receives only a 0.5-second report grace, then uses the existing cancel-confirm/reconcile/reprice flow.
- **Limit-down exception to the barrier**: once a limit-down SELL has a positive broker order ID and both its attempt and `QUEUED_LIMIT_DOWN` state are persisted, it releases the opening barrier but keeps that one low-limit order queued until fill or `machine_schedule.order_guard.limit_down_queue_cancel_at` (example `14:56:30`). It is not cancelled or re-submitted just to unblock BUYs.
- **Auction queue pricing**: from `machine_schedule.market_session.call_auction_start_at` (example `09:15:00`) until `machine_schedule.market_session.continuous_trading_start_at` (example `09:30:00`), `pricing._auction_queue_price()` overrides the normal book/slippage price with `last_price × (1 ± auction_aggressive_pct)` (default 2%), clamped into `[low_limit, high_limit]`. Pre-open signals arriving from `machine_schedule.market_session.preopen_sell_start_at` (example `09:25:00`, the opening SELL barrier's recognition start) have missed the example opening call-auction cutoff — the open price is already fixed — so they queue for the continuous session starting at `machine_schedule.market_session.continuous_trading_start_at`, where fills take the *counterparty's* price level by level. Aggressive quoting therefore buys time priority, not a worse fill. **Do not "just quote the limit price"** for ordinary orders: the unfilled part stays resting at the limit price. Dedicated locked-limit queue modes are the explicit exception. The clamp is mandatory, and missing `high_limit`/`low_limit` falls back to normal pricing rather than quoting uncapped. Set `auction_aggressive_pct: 0` to disable. Tests decouple from the clock by monkeypatching `pricing._in_call_auction` in `setUpModule`.
- **Cancel confirmation is budgeted separately** (`cancel_confirm_timeout_sec`, default 30s) and deliberately **not** charged to `max_total_duration_sec`. Re-quotes run back to back, so the final attempt always butts up against the total budget and its cancel necessarily lands after it — funding the cancel wait from the same budget makes it time out on entry, and a routine unfilled cancel then trips the `_trading_halt_reason` kill switch that stops every later signal for the rest of the process's life (stop-losses included). The engine also cannot re-quote until the cancel reaches a terminal state, since the filled quantity is unknown until then; waiting past the budget is correct.
- **Concurrency**: signals execute in parallel worker threads. The BUY and SELL pools are separate, but opening BUYs deliberately wait for the pre-open SELL barrier. Both sides' limit-queue orders (limit-down sells and limit-up buys) hand off to the dedicated `qmt-queue` pool once the resting order is persisted, so buy/sell workers are never parked all day; the queue pool is sized `max_concurrent_queue_sells + max_concurrent_queue_buys`. Same-code duplicates are prevented by signal-id idempotency, not FIFO ordering.
- **Tick-size aware pricing**: `pricing.tick_size_for()` returns 0.001 for funds/ETFs (SH `5xxxxx`, SZ `1xxxxx`) and 0.01 for stocks; order prices round to the instrument's tick. Without this, ETF buy orders would round down to the cent and miss the book.
- **Daily plan expansion is idempotent**: a `plan` message is recorded in the `plans` table, then expanded deterministically into derived signals (`{plan_id}-sell-{code}` / `{plan_id}-buy-{code}`). Replays re-derive the same ids and hit the existing `signals` idempotency gate; codes already held are filtered out with a fresh position query before auto-buy derivation.
- **Intent quantities resolve from the live account**: `sizing.py` computes `sell_all` (all closeable), `sell_half` (half floored to a 100-share lot; <1 lot sells everything), and `auto_buy` (min(cash ÷ N, total assets × `max_single_position_pct`), floored to a lot). Exact `buy`/`sell` with `amount` keep the legacy path.
- **A-share price cage**: `pricing._apply_stock_dynamic_price_cage()` clamps stock candidates into the dynamic effective range (2% or ±10 ticks from the live book reference, whichever is wider) and the daily limit band; ETFs/funds skip the cage.
- **ACK after terminal state**: The Redis message is only acknowledged after the engine writes the final status to SQLite. If the process crashes mid-execution, the message remains pending in the consumer group and will be redelivered (idempotency protects against double-execution).
- **Pending recovery checks QMT first**: on restart, messages previously owned by the same stable consumer are reclaimed immediately; stale messages from another consumer in the same account group wait for `redis.pending_claim_idle_ms`. New MiniQMT submissions use a deterministic 24-ASCII-character order remark derived from `signal_id` (short IDs remain unchanged) because MiniQMT truncates longer remarks. Recovered signals query QMT by that exact remark before any new submission. An uncertain result persists `RECOVERY_REQUIRED`, halts that account, and stays unacknowledged until an operator stops the service, reconciles QMT, and runs `python -m miniqmt_follower.recovery_cli`.
- **No implied FIFO within a side**: each side uses a multi-worker pool so several opening orders can reach the broker promptly. The explicit opening barrier controls SELL-before-BUY ordering; `signal_id` idempotency controls duplicates. Position queries are never cached; order snapshots retain only a short (100ms) shared polling cache plus per-order callback freshness (`_orders_updated_mono`) that serves recently-updated orders without a full `query_stock_orders` scan.
- **2026-08 提速与可靠性加固（七项）**: ① 聚宽发送端 XADD 带 2s 读写超时 + 3 次重试（signal_id 不变，重复消息由幂等去重）；② 执行侧行情取数失败退避重试 3 次；③ 连续竞价时段行情快照时效门控 `quote_max_age_sec`（超龄有界重取 ~1s，仍超龄带告警提交不判死）；④ 开盘首挂强化 `opening_aggressive_window_sec` + `opening_order_timeout_sec`（默认 0=沿用 `order_timeout_sec`，按机器差异配置：慢撤单券商首挂可放宽到 10s，快撤单券商首挂 2s；`max_total_duration_sec` 按机器放开，示例 90s）；⑤ 收信主循环重活后台化（watchlist 订阅/中文名/plan 展开），收单取名走只读缓存 `cached_instrument_name`；⑥ 跌停排队卖出挂单落库后移交 `qmt-queue` 专用线程池（`queue_future_for` 挂接 ACK 补发，未配置时行为与旧版一致），卖出 worker 不再被全天占用；⑦ 热路径 ⏱️ 耗时日志 20ms 阈值门控 + `log_file_level` 文件级别可配。复盘脚本 `tools/analyze_attempts.py`（终态/重挂分布）与 `tools/analyze_cancel_latency.py`（撤单确认耗时）支撑分机调参。
- **2026-08 第二轮提速与可靠性加固（八项）**: ① 排队单移交后 pending 扫描不再重投：`recover()` 对本进程仍跟踪的 `QUEUED_*` 信号短路返回占位结果，app 层 `_MessageIdRegistry` 把排队待 ACK 与后台展开中的消息 id 并入 `active_message_ids_provider` 排除集（修掉"每 5 秒重投 → 恢复等待以排队截止为限阻塞卖出 worker → 数十秒占满卖出池"的隐患）；② 优雅退出可打断排队慢轮询：引擎接收 `stop_event`，排队轮询命中即按既有撤单收口路径落终态，进程退出上界 ≈ `cancel_confirm_timeout_sec` + 轮询间隔（硬杀语义不变，重启按 QMT 备注恢复）；③ 订单快照缓存按单笔回调新鲜度直返（`_orders_updated_mono`）+ 全局 TTL 50ms→100ms，9:30 全量 `query_stock_orders` 扫描频率下降、报单与查单不再在同一把 `_trading_lock` 上互堵（撤单路径 `_invalidate_order_cache` 强制穿透，撤单确认时效不回退）；④ plan 展开持仓过滤与 auto_buy 批次预算改用单次 `query_account_snapshot`（快照缺失/失败回退逐票查询），盘前少 N-1 次全量持仓扫描、预算少一次资产查询；⑤ 涨停排队买单同样移交 `qmt-queue`（全额接纳时），续跑（非硬拒单/部分成交剩余）在队列线程内迭代闭环，开板后余量转 `_execute_main_loop` 常规定价；⑥ 本地策略引擎把 `QUEUED_*` 占位结果视为受管中间态：不熔断策略日、事件保持 SUBMITTED 且活动卖单占位转挂排队 future，到排队终态才收口；⑦ 过期判断前置于意图解析、幂等闸前置于中文名解析，重复/过期信号零行情与账户开销；⑧ QMT 适配器账本（owned orders/trades/errors）日期翻转清空防数月内存泄漏、全量刷新不吞刚预填的新单、合约静态信息查询单飞。
- **2026-08 开盘首挂价格感知等待**（2026-08-24 复盘）：开盘窗口内 BUY 首笔委托超时未成交时不再一律撤单追价 —— 用新鲜行情按偏离挂单价判断：偏离 ≤ `opening_price_gap_wait_pct` 则每秒重判继续等（最多 `opening_price_gap_wait_max_sec` 秒，受 `max_total_duration_sec` 总预算约束），价格甩开阈值或等待耗尽才走撤单追价；行情取不到宁等不撤，查单异常仍沿既有熔断路径。等待只是给"回报迟到/被更高价买盘短暂插队"一个确认窗口，不改变成交价保护（挂单价仍是价格包络）。任一配置为 0 = 关闭、维持"超时即撤"旧行为。默认 `opening_price_gap_wait_pct: 0.01` / `opening_price_gap_wait_max_sec: 6`；预算核对需保证 首挂超时 + 等待 + 一次追单 ≤ 总预算。**2026-08-27 复盘后升级**：① 重判节奏改为 `opening_price_gap_wait_poll_sec`（默认 0.2s、每秒 5 判，1.0=旧每秒一轮；行情重判与查单阻塞解耦，等待预算按墙钟计入含行情取数耗时）——价格甩开最多 0.2s 内反应；② 行情仍停在竞价旧快照（开盘宽限内冻结时间戳）时不下价格结论（旧价盲判曾让死单白等 6 秒）；③ 买单价格在挂单价下方却零成交、或旧价快照场景，每等待窗口至多触发一次幽灵单三重校验——健康单直查可见继续等（毫秒级成本），死单立即抛 `_GhostOrderSuspected` 交主循环重挂，不用等满窗口。
- **2026-08 幽灵单检测与自动重挂**（2026-08-27 复盘）：`order_stock` 秒回单号但订单从未出现在 QMT 账户委托清单（幽灵单）时，旧代码靠"预填 OPEN/0 + 保留分支 + 兜底 OPEN/0"把幽灵单伪装成健康排队单、傻等数分钟。新机制：适配器记录每笔自有单的报单时刻，超过 `execution.ghost_order_detect_grace_sec`（0=关闭，建议 3）仍不出现在 QMT 全量清单时改报内部合成状态 `BrokerOrderStatus.NOT_VISIBLE`（终态快照不受影响）；引擎轮询遇 NOT_VISIBLE 抛 `_GhostOrderSuspected` 并做三重校验——① `query_orders_by_signal_id` 直查（可见=回报滞后，恢复轮询原单）、② `AccountSnapshot.frozen_cash`（>0=在途委托）、③ 持仓成交痕迹（买：total_qty>0；卖：可用持仓<委托量）——全空才确认幽灵单，按 `execution.ghost_order_auto_resubmit` 直接重挂剩余数量（无单可撤、走新 attempt、signal_id 幂等键不变；连续幽灵重挂超 2 次熔断防通道坏死空转刷单）；任一有疑维持熔断+RECOVERY_REQUIRED 人工对账。自动模式最坏=现状（误判→熔断），最好=自动恢复。残留风险：冻结资金回传延迟的理论窗口与买单方向"账户本已持有该代码"的误判（均 fail-closed）。`QmtBrokerAdapter` 构造新增关键字参数 `ghost_order_detect_grace_sec`，由 app.py 从 `config.execution` 注入。
- **2026-08 陈旧 Redis 消息收信侧日期门控**（某交易机隔天开机重放昨日候选计划复盘）：消费组里昨天未读的消息在隔天开机时会被 `XREADGROUP >` 当新消息重放。candidate_plan 有 trading_date 门控（落 LATE 审计、不执行），但 ① 日计划(plan)消息完全无日期门控、只靠派生信号的 600s 过期兜底（sent_at_ms 缺失的旧协议消息会真执行）；② 普通信号同样只有过期兜底。新增 app 层 `_message_trade_date()`（sent_at_ms 本地时区优先，其次 created_at 前缀，推导不出返回 None 不误杀）与 `_local_today()`：`_dispatch_message` 对 plan 与普通信号在派发前检查"消息日期 < 本地今天 → 记日志 + ACK，不落库不展开不下单"；`_handle_candidate_plan` 对 LATE/REJECTED 用警告级文案"⏳ 候选计划未采纳｜仅审计落库, 不执行不订阅"，避免被误读为正常采纳。
- **2026-08-31 复盘后（补仓多波次 + 价格拒单强制刷新）**: ① 开盘回款补仓由"卖单屏障清空后一次性触发"改为**多波次**——窗口 [start_at, admit_until) 内每 tick 检查回款池（可用资金 − 第一波基线），池子够一手就发一波；买单提交即冻结资金、池自然归零，下一笔卖款到账再起下一波（含窗口内硬止损/开盘止损等一切卖款）。基线随第一波接纳落库新增 KV 表 `strategy_state`（key=wave1_cash_baseline）；波次编号从落库信号数重建（signal_id=`{sid}-{date}-topup{NN}-{code}`），事件规则名带波次后缀 `opening_buy_topup_{NN}`（事件表主键含 rule_name），重启可续跑不重发旧波；恢复路径按规则名后缀解析波次重建信号，避免"恢复 id 不一致"假熔断。单波内不足一手的预算留池累积（`filter_deployable_topup_budgets`，1 手门槛含 2% 报价余量，防止顶格一手被 1.5% 报价包络顶成废单）。`admit_until` 可按策略放宽到开盘后 10 分钟收口，波次提交后失效账户快照缓存防双花。② 价格类拒单（分类=price）重挂不再沿用旧行情——重挂前强制刷新（开盘宽限豁免不再适用，快照时间戳不前进则持续重取，上限 2s；`_refresh_quote_after_price_rejection`），且改**盘口锚定报价**（`calculate_order_price(prefer_book=True)`：卖挂买一/买挂卖一，不再用"最新价×激进幅度"），退避下限 0.2s。修复某票 20 连拒（"20009:委托价不正"）：冻结竞价快照 + 激进价双双脱离交易所实时价格笼子（卖单不得低于实时买一×98% 与买一−10tick 孰低），盘口锚定价天然合法。

### Adapter isolation (Protocol pattern)

`miniqmt_follower/executor.py` defines two `Protocol` classes that the execution engine depends on:

- `MarketDataAdapter` — `latest_quote()` returning a `Quote` (last price + ask1/bid1, `None` when that book side is empty)
- `BrokerAdapter` — `query_available_cash()`, `query_available_position()`, `submit_order()`, `get_order_snapshot()`, `cancel_order()`

The concrete QMT implementations live in `miniqmt_follower/adapters/qmt.py`; tests inject `FakeMarketData` and `FakeBroker` without touching QMT. Both real adapters are implemented:

- `QmtMarketDataAdapter` uses `xtdata.get_full_tick()` for quotes. It subscribes codes via `xtdata.subscribe_quote()` — the `market_data.pre_subscribe_codes` config list at startup, plus lazily on first sight of a new code — because unsubscribed `get_full_tick` calls may hit the quote server (tens of ms) while subscribed ones read local memory.
- `QmtBrokerAdapter` wires `XtQuantTrader` (connect → subscribe → `order_stock` / `query_stock_orders` / `cancel_order_stock`). Its constructor is a **safety gate**: it raises `QmtAdapterNotConfigured` unless `trading.enabled=true` and `account_id`/`miniqmt_path` are set and `xtquant` imports — so the service cannot accidentally live-trade with the default config.
- Code mapping lives in `jq_code_to_qmt_code()` (`000001.XSHE → 000001.SZ`, `510300.XSHG → 510300.SH`); QMT order-status constants map to internal `BrokerOrderStatus` in `_qmt_order_status_to_broker_status()` (unknown statuses conservatively map to `OPEN` so the timeout logic handles them).

Do not alter `OrderExecutionEngine` to work around broker-specific behavior — fix the adapter instead.

### Key modules

| Module | Role |
|---|---|
| `miniqmt_follower/models.py` | All dataclasses and enums: `TradeSignal`, `ExecutionConfig`, `BrokerOrderStatus`, `ExecutionStatus`, `OrderSnapshot`, `ExecutionResult` |
| `miniqmt_follower/config.py` | YAML config loading with `${ENV_VAR}` password resolution; `RedisConfig` / `TradingConfig` / `RuntimeConfig` |
| `miniqmt_follower/store.py` | SQLite execution ledger: WAL mode, `signals` table (PK=signal_id), `order_attempts` table (one row per broker submission), `plans` table (daily-plan receipt) |
| `miniqmt_follower/plan_executor.py` | Daily-plan expansion: records the plan, derives `sell_all`/`auto_buy` signals with deterministic ids, filters out held codes, submits a combined future |
| `miniqmt_follower/sizing.py` | Pure intent quantity math: `sell_all` / `sell_half` / `auto_buy` |
| `miniqmt_follower/pricing.py` | Order pricing: auction-queue quoting, order-book/slippage modes, tick rounding |
| `miniqmt_follower/executor.py` | Core state machine — depends only on the two Protocol adapters and the store |
| `miniqmt_follower/redis_stream.py` | Redis Stream consumer group wrapper: `read_forever()`, `ack()`, `publish_signal()` |
| `miniqmt_follower/app.py` | DI wiring + BUY/SELL worker pools + opening SELL barrier; graceful shutdown via a shared `Event` |
| `miniqmt_follower/opening.py` | Pre-open SELL recognition and the thread-safe BUY release barrier |
| `miniqmt_follower/logging_config.py` | Console + `TimedRotatingFileHandler` file logging (emoji-style Chinese log lines are the project convention) |
| `miniqmt_follower/adapters/qmt.py` | Concrete QMT adapters, code/status mapping, trading lock + short order cache |
| `joinquant_signal_sender.py` | JoinQuant-side sender functions (`publish_trade_signal_to_redis` / `publish_watchlist_to_redis` / `publish_daily_plan_to_redis` / `publish_sell_half_to_redis` / `publish_sell_all_to_redis`); placeholder config, copied into the strategy at deploy time |

### Signal contract

Signals are JSON objects written to Redis Stream (message field `payload`). The `signal_id` field is the idempotency key:

```json
{
  "signal_id": "strategy-a-20260608093001-000001XSHE-buy-1000",
  "strategy_id": "strategy_a",
  "mode": "live",
  "action": "buy",
  "code": "000001.XSHE",
  "amount": 1000,
  "reference_price": 10.0,
  "created_at": "2026-06-08 09:30:01",
  "expire_at": "2026-06-08 09:30:21",
  "nonce": "a1b2c3d4"
}
```

- `reference_price` is the strategy's opinion, **not** the final order price — the Windows side always reprices from the live quote. Nothing gates on it: the deviation guard that used to reject orders straying too far from it was removed, because a rejected order forks live holdings away from the JoinQuant paper portfolio permanently and nothing re-sends it. It survives purely as an audit/log field.
- `mode` distinguishes live vs backtest; backtest signals are silently dropped by the sender.
- Signal expiry is driven by `sent_at_ms + execution.signal_expire_seconds` (default 600s / 10 minutes, `0` disables), checked when a signal reaches the head of its side-specific execution queue / starts executing; expired or malformed signals are finalized without broker submission. The legacy `expire_at` absolute-time field is still honored first when present. `nonce` remains an audit-only field. `from_dict` also accepts legacy field names `price` and `timestamp`.
- `execute_at` is no longer part of the live contract. Historical payloads containing it remain parseable because unknown keys are ignored, but they execute immediately when otherwise valid and unexpired.
- `sent_at_ms` (epoch milliseconds at send time) feeds the latency instrumentation: the consumer logs transport latency on receipt and end-to-end latency at terminal state. Optional — old signals without it just skip those log fields. Only meaningful when both machines are NTP-synced.
- Default `signal_id` = `strategy_id + time + code + action + amount`, so two identical orders in the same second collide — a deliberate dedupe property.

Besides trade signals, the stream carries **watchlist commands** (`{"action": "subscribe", "codes": [...], "strategy_id": ...}`): strategies push their stock pool immediately after selection via `publish_watchlist_to_redis()`, and the consumer subscribes those codes' quotes immediately (no order is placed). `redis_stream._parse_message()` routes them to `StreamMessage.watchlist`; `app.py` handles them inline and ACKs without entering the engine.

The stream also carries **daily plans** (`{"action": "plan", "signal_id": ..., "codes_to_sell": [...], "codes_to_buy": [...], ...}`) and **intent signals** (`action: sell_half` / `sell_all`, or `buy` without `amount`). Plans are expanded by `PlanExecutor` into derived signals; intent quantities resolve from the live account via `sizing.py`. The exact five-argument buy/sell protocol remains the legacy-compatible path.

### SQLite schema

Three tables:
- `signals` — one row per unique `signal_id`, tracks terminal `status` and cumulative `filled_qty`
- `order_attempts` — one row per broker submission (attempt 1, 2, 3…), linked to `signals.signal_id` via FK, records `broker_order_id`, `quantity`, `price`, per-attempt `filled_qty` and `status`
- `plans` — one row per daily-plan `plan_id` (audit receipt; replay dedupe happens through the derived `signals.signal_id`s)

WAL mode + `synchronous=NORMAL` reduce write latency on the hot path.

## Config

Copy `config.example.yaml` to `config.yaml`. Password supports `${ENV_VAR}` syntax (e.g., `"${REDIS_PASSWORD}"` reads from the environment).

`execution` tunables:
- `buy_slippage_pct` / `sell_slippage_pct` — fixed-percentage price adjustment per order side
- `order_timeout_sec` — per-attempt poll deadline before cancel+retry
- `max_attempts` / `max_total_duration_sec` — hard stops on the retry loop
- `cancel_confirm_timeout_sec` — how long to wait for the broker to report a cancelled order's true terminal state (default 30s). Exceeding it is what declares the cancel state uncertain and halts trading, so do not shrink it toward `max_total_duration_sec`; see the pipeline note above
- `auction_aggressive_pct` — how far queue orders quote past the last price from `machine_schedule.market_session.call_auction_start_at` (example `09:15:00`) until `machine_schedule.market_session.continuous_trading_start_at` (example `09:30:00`; `0.02` by default, `0` disables). It buys time priority for that configured continuous-session boundary; the result is clamped into the daily limit band. See "Auction queue pricing" above for why this must never be the limit price itself
- `limit_down_sell_mode` / `limit_up_buy_mode` — **moved to `config.strategy.yaml` `execution` node** (strategy-layer policy: `"queue"` parks at the limit price until `machine_schedule.order_guard.limit_down_queue_cancel_at` / `limit_up_queue_cancel_at`, example `14:56:30`; `"skip"`; `"none"`). Machine configs carrying these keys are rejected on the strategy path; `max_concurrent_queue_sells` / `max_concurrent_queue_buys` (queue-pool capacity) stay machine-side.
- `plan_enabled` / `plan_execute_at` — daily-plan handling and a compatibility-only legacy time field that does not delay execution
- `sell_half_insufficient_lot_mode` — **moved to `config.strategy.yaml` `execution` node**: `"sell_all"` (a sell-half whose rounded half is below one lot sells the whole position) or `"skip"` (resolves to 0 and ends as `SKIPPED_SMALL_POSITION` without an order)
- `max_single_position_pct` — **moved to `config.strategy.yaml` `execution` node**: the single-stock concentration cap (total assets × pct), now the merged single key that replaced `strategy_engine.opening_buy.capital_allocation.single_position_limit_pct`. The strategy engine applies it as a position-level cap (budget = total assets × pct − existing position market value) in `calculate_equal_budgets` / `calculate_topup_budgets`; the execution layer applies it as a per-order cap in `_per_stock_budget()` for `auto_buy` signals. The strategy YAML `execution` node has exactly four required keys (`limit_down_sell_mode`, `limit_up_buy_mode`, `sell_half_insufficient_lot_mode`, `max_single_position_pct`), validated by the same validators as the machine config, and is injected into `ExecutionConfig` by `load_runtime_with_strategy`; `LocalStrategyEngine` receives the same value as `single_position_limit_pct`.
- `signal_expire_seconds` — seconds after `sent_at_ms` after which a not-yet-started signal is finalized as `EXPIRED` (default 600 / 10 minutes; `0` disables expiry; legacy `expire_at` still wins when present)
- `poll_interval_sec` — steady-state order polling interval
- `pricing_mode` — `"slippage"` (default: last price ± slippage) or `"book"` (buy at ask1 + `book_tick_offset` ticks, sell at bid1 − offset; falls back to slippage when that book side is empty, e.g. limit-up/down)

`redis` section:
- `allowed_strategy_ids` — non-empty strategy whitelist; messages from any other `strategy_id` are logged and ACKed without execution (empty = no filtering, legacy behavior)

`market_data` section:
- `pre_subscribe_codes` — JoinQuant-format codes to subscribe at startup so the first signal of the day prices from local memory

`trading` section (all consumed by `QmtBrokerAdapter.__init__`):
- `enabled` — **defaults to `false`**; the live-trading kill switch
- `account_id` / `miniqmt_path` / `session_id` / `strategy_name`

Top-level: `state_db` (SQLite path), `log_level`, `log_dir`.

Note: `redis.stream` must match the stream name hard-coded inside the copy of `publish_trade_signal_to_redis` embedded in the JoinQuant strategy — the two sides don't share config, so keep them in sync manually.

Opening-flow deployment has two independent artifacts: update `miniqmt_follower/` and restart `qmt-trader` on the Windows trading box, then paste the updated strategy deployment copy into the JoinQuant live simulation. In engine mode the two YAML layers are strict and fully required: every machine config must provide all 9 `machine_schedule` fields, and `config.strategy.yaml` must provide all 13 `strategy_engine.schedule` fields, the `external_signals.allowed_sell_purposes` whitelist, and the four `execution` keys (`limit_down_sell_mode` / `limit_up_buy_mode` / `sell_half_insufficient_lot_mode` / `max_single_position_pct`); no code defaults or automatic migration fill omissions. In pure follower mode only the machine config is required. Before deployment manually confirm both limit modes are `queue` (in `config.strategy.yaml`), both configured queue-cancel values are the intended example `14:56:30`, and both queue-capacity limits match the intended example values. The public `config.strategy.example.yaml` template uses fictional example values only — never copy real thresholds into the public repo.

## Testing

`tests/` is intentionally kept outside version control as a local-only suite (it includes
strategy-specific contract tests that read private deployment copies). The commands above
work on machines that have the local `tests/` copy; a fresh clone has no tests.

Tests use only stdlib `unittest` and inject fakes:
- `FakeMarketData` with a pre-loaded price queue
- `FakeBroker` with pre-loaded `OrderSnapshot` sequences per order ID
- `FakeRedis` modules monkeypatched into `sys.modules["redis"]`
- `SQLiteExecutionStore` backed by `tempfile.TemporaryDirectory`

No real Redis, QMT, or network is needed to run the full test suite. `xtquant` only imports on Windows with miniQMT installed, so anything touching real QMT APIs must stay behind the adapter boundary or lazy imports for tests to keep passing on macOS/Linux.

A local-only contract test may read a private strategy deployment copy when present and skips when it is absent.

## QMT Broker Adapter Status

`QmtBrokerAdapter` is implemented and has been exercised by the user with a MiniQMT simulation account. Treat that as environment-specific evidence, not universal broker compatibility: re-validate per target MiniQMT/broker build using the simulation-account steps in `README.md` ("上线检查" / "本地验证") and keep `trading.enabled=false` until the target account passes. The old `docs/windows-qmt-adapter-handoff.md` checklist is no longer maintained in this repo.

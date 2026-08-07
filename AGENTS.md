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

### Execution pipeline (Windows side)

```
Redis Stream → RedisStreamClient.read_forever()
    → app.py filters by redis.allowed_strategy_ids (non-empty ⇒ unknown strategies are logged + ACKed, never executed)
    → plan messages route to PlanExecutor: derived sell_all/auto_buy signals enter the same pools
    → app.py routes each signal to a per-direction ThreadPoolExecutor (separate buy/sell pools, --workers threads each, default 8)
    → 09:25–09:30 pre-open SELL signals register in OpeningSellBarrier; BUY workers wait for its release
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
- **Opening SELL barrier**: the strategy publishes SELLs at 09:27 and publishes no BUY at 09:29. MiniQMT submits those SELLs in parallel. After 09:30, the first ordinary SELL attempt receives only a 0.5-second report grace, then uses the existing cancel-confirm/reconcile/reprice flow. BUY execution waits until every ordinary pre-open SELL has completed or explicitly terminated.
- **Limit-down exception to the barrier**: once a limit-down SELL has a positive broker order ID and both its attempt and `QUEUED_LIMIT_DOWN` state are persisted, it releases the opening barrier but keeps that one low-limit order queued until fill or 14:56:30. It is not cancelled or re-submitted just to unblock BUYs.
- **Auction queue pricing**: during 9:15–9:30, `pricing._auction_queue_price()` overrides the normal book/slippage price with `last_price × (1 ± auction_aggressive_pct)` (default 2%), clamped into `[low_limit, high_limit]`. Pre-open signals **miss the 9:25 opening call auction entirely** — the open price is already fixed — so they queue for the 9:30 continuous session, where fills take the *counterparty's* price level by level. Aggressive quoting therefore buys time priority, not a worse fill. **Do not "just quote the limit price"** for ordinary orders: the unfilled part stays resting at the limit price. Dedicated locked-limit queue modes are the explicit exception. The clamp is mandatory, and missing `high_limit`/`low_limit` falls back to normal pricing rather than quoting uncapped. Set `auction_aggressive_pct: 0` to disable. Tests decouple from the clock by monkeypatching `pricing._in_call_auction` in `setUpModule`.
- **Cancel confirmation is budgeted separately** (`cancel_confirm_timeout_sec`, default 30s) and deliberately **not** charged to `max_total_duration_sec`. Re-quotes run back to back, so the final attempt always butts up against the total budget and its cancel necessarily lands after it — funding the cancel wait from the same budget makes it time out on entry, and a routine unfilled cancel then trips the `_trading_halt_reason` kill switch that stops every later signal for the rest of the process's life (stop-losses included). The engine also cannot re-quote until the cancel reaches a terminal state, since the filled quantity is unknown until then; waiting past the budget is correct.
- **Concurrency**: signals execute in parallel worker threads. The BUY and SELL pools are separate, but opening BUYs deliberately wait for the pre-open SELL barrier. Keep `--workers` above each side's configured limit-queue capacity so ordinary work retains at least two workers. Same-code duplicates are prevented by signal-id idempotency, not FIFO ordering.
- **Tick-size aware pricing**: `pricing.tick_size_for()` returns 0.001 for funds/ETFs (SH `5xxxxx`, SZ `1xxxxx`) and 0.01 for stocks; order prices round to the instrument's tick. Without this, ETF buy orders would round down to the cent and miss the book.
- **Daily plan expansion is idempotent**: a `plan` message is recorded in the `plans` table, then expanded deterministically into derived signals (`{plan_id}-sell-{code}` / `{plan_id}-buy-{code}`). Replays re-derive the same ids and hit the existing `signals` idempotency gate; codes already held are filtered out with a fresh position query before auto-buy derivation.
- **Intent quantities resolve from the live account**: `sizing.py` computes `sell_all` (all closeable), `sell_half` (half floored to a 100-share lot; <1 lot sells everything), and `auto_buy` (min(cash ÷ N, total assets × `max_single_position_pct`), floored to a lot). Exact `buy`/`sell` with `amount` keep the legacy path.
- **A-share price cage**: `pricing._apply_stock_dynamic_price_cage()` clamps stock candidates into the dynamic effective range (2% or ±10 ticks from the live book reference, whichever is wider) and the daily limit band; ETFs/funds skip the cage.
- **ACK after terminal state**: The Redis message is only acknowledged after the engine writes the final status to SQLite. If the process crashes mid-execution, the message remains pending in the consumer group and will be redelivered (idempotency protects against double-execution).
- **No implied FIFO within a side**: each side uses a multi-worker pool so several opening orders can reach the broker promptly. The explicit opening barrier controls SELL-before-BUY ordering; `signal_id` idempotency controls duplicates. Position queries are never cached; order snapshots retain only a 50ms polling cache.

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
  "signal_id": "hunter-20260608093001-000001XSHE-buy-1000",
  "strategy_id": "hunter",
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

Besides trade signals, the stream carries **watchlist commands** (`{"action": "subscribe", "codes": [...], "strategy_id": ...}`): strategies push their stock pool right after selection (~9:25–9:26) via `publish_watchlist_to_redis()`, and the consumer subscribes those codes' quotes immediately (no order is placed). `redis_stream._parse_message()` routes them to `StreamMessage.watchlist`; `app.py` handles them inline and ACKs without entering the engine.

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
- `auction_aggressive_pct` — how far 9:15–9:30 queue orders quote past the last price (default `0.02`; `0` disables). Buys time priority for the 9:30 open; result is clamped into the daily limit band. See "Auction queue pricing" above for why this must never be the limit price itself
- `limit_down_sell_mode` / `limit_up_buy_mode` — `"queue"` (park at the limit price until `queue_sell_deadline` / `queue_buy_deadline`, default 14:56:30), `"skip"`, or `"none"`; `"queue"` holds one of the `max_concurrent_queue_sells` / `max_concurrent_queue_buys` slots, which must stay below the worker count
- `plan_enabled` / `plan_execute_at` / `max_single_position_pct` — daily-plan handling (default enabled, executes at 09:30:00 local time) and the per-stock auto-buy cap (total assets × pct, default 0.2)
- `sell_half_insufficient_lot_mode` — `"sell_all"` (default; a sell-half whose rounded half is below one lot sells the whole position) or `"skip"` (resolves to 0 and ends as `SKIPPED_SMALL_POSITION` without an order)
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

Opening-flow deployment has two independent artifacts: update `miniqmt_follower/` and restart `qmt-trader` on the Windows trading box, then paste the updated strategy deployment copy into the JoinQuant live simulation. No new YAML key is required; before deployment manually confirm both limit modes are `queue`, both deadlines are `14:56:30`, and both queue-capacity limits are 5.

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

`tests/test_harvester_strategy_contract.py` reads the local strategy deployment copy when present and skips when it is absent.

## QMT Broker Adapter Status

`QmtBrokerAdapter` is implemented and has been exercised by the user with a MiniQMT simulation account. Treat that as environment-specific evidence, not universal broker compatibility: re-validate per target MiniQMT/broker build using the simulation-account steps in `README.md` ("上线检查" / "本地验证") and keep `trading.enabled=false` until the target account passes. The old `docs/windows-qmt-adapter-handoff.md` checklist is no longer maintained in this repo.

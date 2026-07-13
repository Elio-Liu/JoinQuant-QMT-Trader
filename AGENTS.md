# AGENTS.md

This file provides guidance to Codex (Codex.ai/code) when working with code in this repository.

## Project Overview

JoinQuant miniQMT Live Follower — a two-sided system where a JoinQuant cloud strategy sends trade signals via Redis Stream, and a Windows-side service consumes them to execute real orders through miniQMT/xtquant.

## Commands

```bash
# Run all tests
python -m unittest discover -v

# Compile-check runtime, scripts, and tests (catches ImportErrors before runtime)
python -m compileall qmt_follower scripts tests

# Run the Windows execution service
python main.py
python main.py --config config.yaml --workers 1

# Run a single test module
python -m unittest tests.test_executor -v
```

## Architecture

### Two-side split

This project contains both sides of the system. Local deployment copies under `strategies/` are intentionally ignored because they may embed environment-specific Redis credentials.

1. **JoinQuant side** (cloud strategy): `joinquant_signal_sender.py` — a single function `publish_trade_signal_to_redis(context, action, code, amount, price)` that embeds into JoinQuant strategies. It serializes a signal as JSON, writes it to Redis Stream via `XADD`, and returns immediately (fire-and-forget). The function signature is frozen — strategy call sites stay untouched.

2. **Windows side** (this repo's runtime): `qmt_follower/` — a long-running service (`python main.py`) that consumes Redis Stream via consumer group with `XREADGROUP`, executes orders through miniQMT, and ACKs messages only after the order reaches a terminal state.

### Execution pipeline (Windows side)

```
Redis Stream → RedisStreamClient.read_forever()
    → app.py queues trade messages in a single-worker ThreadPoolExecutor (global FIFO)
    → OrderExecutionEngine.execute()
        → SQLiteExecutionStore.try_accept_signal()  [idempotency gate: INSERT OR IGNORE on signal_id PK]
        → MarketDataAdapter.latest_quote()  [last price + ask1/bid1]
        → pricing.calculate_order_price()  [deviation guard + slippage or order-book pricing]
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
- **Resource capping fails closed**: if the fresh cash/position query fails, no order is submitted. BUY quantities use the current calculated order price and round down to a 100-share lot; SELL quantities cap to the latest closeable position.
- **Each attempt refreshes state**: every broker submission uses a fresh quote and fresh MiniQMT cash/position query.
- **Cancel confirmation**: a cancel request is not treated as completion. The engine waits for `FILLED`, `CANCELED`, or `REJECTED`, then reconciles fills that arrive during cancellation before re-submitting. If the cancel state remains uncertain, the process latches a trading halt and later queued signals cannot submit.
- **Adaptive polling**: `_wait_for_terminal_or_timeout` polls at ≤50ms for the first second, then falls back to `poll_interval_sec`.
- **Auction protection**: signals received during the 9:15–9:30 call auction extend both the per-attempt poll deadline and the total-duration budget until after the 9:30 open (`_seconds_until_market_open()` in `executor.py`), so orders queue at the broker for the opening match instead of being cancelled by `order_timeout_sec`. Strategies deliberately send sell signals at 9:27 to get queue priority. Tests disable this via `setUpModule` monkeypatching.
- **Tick-size aware pricing**: `pricing.tick_size_for()` returns 0.001 for funds/ETFs (SH `5xxxxx`, SZ `1xxxxx`) and 0.01 for stocks; order prices round to the instrument's tick. Without this, ETF buy orders would round down to the cent and miss the book.
- **ACK after terminal state**: The Redis message is only acknowledged after the engine writes the final status to SQLite. If the process crashes mid-execution, the message remains pending in the consumer group and will be redelivered (idempotency protects against double-execution).
- **Global FIFO**: only one trade signal executes at a time. Redis intake and watchlist handling remain responsive, but a slow order intentionally blocks later trades from reaching MiniQMT. Position queries are never cached; order snapshots retain only a 50ms polling cache.

### Adapter isolation (Protocol pattern)

`qmt_follower/executor.py` defines two `Protocol` classes that the execution engine depends on:

- `MarketDataAdapter` — `latest_quote()` returning a `Quote` (last price + ask1/bid1, `None` when that book side is empty)
- `BrokerAdapter` — `query_available_cash()`, `query_available_position()`, `submit_order()`, `get_order_snapshot()`, `cancel_order()`

The concrete QMT implementations live in `qmt_follower/adapters/qmt.py`; tests inject `FakeMarketData` and `FakeBroker` without touching QMT. Both real adapters are implemented:

- `QmtMarketDataAdapter` uses `xtdata.get_full_tick()` for quotes. It subscribes codes via `xtdata.subscribe_quote()` — the `market_data.pre_subscribe_codes` config list at startup, plus lazily on first sight of a new code — because unsubscribed `get_full_tick` calls may hit the quote server (tens of ms) while subscribed ones read local memory.
- `QmtBrokerAdapter` wires `XtQuantTrader` (connect → subscribe → `order_stock` / `query_stock_orders` / `cancel_order_stock`). Its constructor is a **safety gate**: it raises `QmtAdapterNotConfigured` unless `trading.enabled=true` and `account_id`/`miniqmt_path` are set and `xtquant` imports — so the service cannot accidentally live-trade with the default config.
- Code mapping lives in `jq_code_to_qmt_code()` (`000001.XSHE → 000001.SZ`, `510300.XSHG → 510300.SH`); QMT order-status constants map to internal `BrokerOrderStatus` in `_qmt_order_status_to_broker_status()` (unknown statuses conservatively map to `OPEN` so the timeout logic handles them).

Do not alter `OrderExecutionEngine` to work around broker-specific behavior — fix the adapter instead.

### Key modules

| Module | Role |
|---|---|
| `qmt_follower/models.py` | All dataclasses and enums: `TradeSignal`, `ExecutionConfig`, `BrokerOrderStatus`, `ExecutionStatus`, `OrderSnapshot`, `ExecutionResult` |
| `qmt_follower/config.py` | YAML config loading with `${ENV_VAR}` password resolution; `RedisConfig` / `TradingConfig` / `RuntimeConfig` |
| `qmt_follower/store.py` | SQLite execution ledger: WAL mode, `signals` table (PK=signal_id), `order_attempts` table (one row per broker submission) |
| `qmt_follower/pricing.py` | Slippage calculation and price deviation guard (`PriceDeviationError`) |
| `qmt_follower/executor.py` | Core state machine — depends only on the two Protocol adapters and the store |
| `qmt_follower/redis_stream.py` | Redis Stream consumer group wrapper: `read_forever()`, `ack()`, `publish_signal()` |
| `qmt_follower/app.py` | DI wiring + single-worker FIFO trade queue; graceful shutdown via a shared `Event` |
| `qmt_follower/logging_config.py` | Console + `TimedRotatingFileHandler` file logging (emoji-style Chinese log lines are the project convention) |
| `qmt_follower/adapters/qmt.py` | Concrete QMT adapters, code/status mapping, trading lock + short order cache |
| `joinquant_signal_sender.py` | Standalone JoinQuant-side functions (`publish_trade_signal_to_redis`, `publish_watchlist_to_redis`); no imports from `qmt_follower/` |
| `scripts/send_manual_signal.py` | Manual signal tool; remote Redis connection comes from `QMT_REDIS_*` environment variables |
| `scripts/send_batch_signals.py` | Ordered stress-batch sender with configurable message interval and sell-limit validation |

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

- `reference_price` is the strategy's opinion, **not** the final order price. It is the deviation-guard reference; BUY cash capping uses the newly calculated live order price.
- `mode` distinguishes live vs backtest; backtest signals are silently dropped by the sender.
- `expire_at` and `nonce` are advisory/audit fields — the consumer's `TradeSignal.from_dict()` ignores them. `from_dict` also accepts legacy field names `price` and `timestamp`.
- `sent_at_ms` (epoch milliseconds at send time) feeds the latency instrumentation: the consumer logs transport latency on receipt and end-to-end latency at terminal state. Optional — old signals without it just skip those log fields. Only meaningful when both machines are NTP-synced.
- Default `signal_id` = `strategy_id + time + code + action + amount`, so two identical orders in the same second collide — a deliberate dedupe property.

Besides trade signals, the stream carries **watchlist commands** (`{"action": "subscribe", "codes": [...], "strategy_id": ...}`): strategies push their stock pool right after selection (~9:25–9:26) via `publish_watchlist_to_redis()`, and the consumer subscribes those codes' quotes immediately (no order is placed). `redis_stream._parse_message()` routes them to `StreamMessage.watchlist`; `app.py` handles them inline and ACKs without entering the engine.

### SQLite schema

Two tables:
- `signals` — one row per unique `signal_id`, tracks terminal `status` and cumulative `filled_qty`
- `order_attempts` — one row per broker submission (attempt 1, 2, 3…), linked to `signals.signal_id` via FK, records `broker_order_id`, `quantity`, `price`, per-attempt `filled_qty` and `status`

WAL mode + `synchronous=NORMAL` reduce write latency on the hot path.

## Config

Copy `config.example.yaml` to `config.yaml`. Password supports `${ENV_VAR}` syntax (e.g., `"${REDIS_PASSWORD}"` reads from the environment).

`execution` tunables:
- `buy_slippage_pct` / `sell_slippage_pct` — fixed-percentage price adjustment per order side
- `order_timeout_sec` — per-attempt poll deadline before cancel+retry
- `max_attempts` / `max_total_duration_sec` — hard stops on the retry loop
- `max_deviation_from_signal_price_pct` — risk gate: refuse to trade if the pricing base (last price in slippage mode, opposite book side in book mode) deviates beyond this from the JoinQuant reference price
- `poll_interval_sec` — steady-state order polling interval
- `pricing_mode` — `"slippage"` (default: last price ± slippage) or `"book"` (buy at ask1 + `book_tick_offset` ticks, sell at bid1 − offset; falls back to slippage when that book side is empty, e.g. limit-up/down)

`market_data` section:
- `pre_subscribe_codes` — JoinQuant-format codes to subscribe at startup so the first signal of the day prices from local memory

`trading` section (all consumed by `QmtBrokerAdapter.__init__`):
- `enabled` — **defaults to `false`**; the live-trading kill switch
- `account_id` / `miniqmt_path` / `session_id` / `strategy_name`

Top-level: `state_db` (SQLite path), `log_level`, `log_dir`.

Note: `redis.stream` must match the stream name hard-coded inside the copy of `publish_trade_signal_to_redis` embedded in the JoinQuant strategy — the two sides don't share config, so keep them in sync manually.

## Testing

Tests use only stdlib `unittest` and inject fakes:
- `FakeMarketData` with a pre-loaded price queue
- `FakeBroker` with pre-loaded `OrderSnapshot` sequences per order ID
- `FakeRedis` modules monkeypatched into `sys.modules["redis"]`
- `SQLiteExecutionStore` backed by `tempfile.TemporaryDirectory`

No real Redis, QMT, or network is needed to run the full test suite. `xtquant` only imports on Windows with miniQMT installed, so anything touching real QMT APIs must stay behind the adapter boundary or lazy imports for tests to keep passing on macOS/Linux.

## QMT Broker Adapter Status

`QmtBrokerAdapter` is implemented and has been exercised by the user with a MiniQMT simulation account. Treat that as environment-specific evidence, not universal broker compatibility: run the checklist in [docs/windows-qmt-adapter-handoff.md](docs/windows-qmt-adapter-handoff.md) for every target MiniQMT/broker build and keep `trading.enabled=false` until the target account passes.

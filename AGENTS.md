# AGENTS.md

This file provides guidance to Codex (Codex.ai/code) when working with code in this repository.

## Project Overview

JoinQuant miniQMT Live Follower — a two-sided system where a JoinQuant cloud strategy sends trade signals via Redis Stream, and a Windows-side service consumes them to execute real orders through miniQMT/xtquant.

## Commands

```bash
# Run all tests
python -m unittest discover -v

# Compile-check both packages (catches ImportErrors before runtime)
python -m compileall qmt_follower tests

# Run the Windows execution service
python main.py
python main.py --config config.prod.json

# Run a single test module
python -m unittest tests.test_executor -v
```

## Architecture

### Two-side split

This repo contains **both** sides of the system:

1. **JoinQuant side** (cloud strategy): `joinquant_signal_sender.py` — a single function `publish_trade_signal_to_redis(context, action, code, amount, price)` that embed into JoinQuant strategies. It serializes a signal as JSON, writes it to Redis Stream via `XADD`, and returns immediately (fire-and-forget). The function signature is frozen — strategy call sites stay untouched.

2. **Windows side** (this repo's runtime): `qmt_follower/` — a long-running service (`python main.py`) that consumes Redis Stream via consumer group with `XREADGROUP`, executes orders through miniQMT, and ACKs messages only after the order reaches a terminal state.

### Execution pipeline (Windows side)

```
Redis Stream → RedisStreamClient.read_forever()
    → OrderExecutionEngine.execute()
        → SQLiteExecutionStore.try_accept_signal()  [idempotency gate: INSERT OR IGNORE on signal_id PK]
        → MarketDataAdapter.latest_price()
        → pricing.calculate_order_price()  [deviation guard + slippage]
        → BrokerAdapter.submit_order()
        → poll BrokerAdapter.get_order_snapshot() until terminal or timeout
        → if partial fill: BrokerAdapter.cancel_order() → re-price with latest quote → re-submit remaining qty
        → SQLiteExecutionStore.update_signal_status()  [terminal state]
    → RedisStreamClient.ack()  [only after terminal state]
```

Key design decisions in this pipeline:
- **Idempotency**: SQLite `signals` table primary key is `signal_id`. Duplicate signals return `DUPLICATE_IGNORED` with the existing filled quantity — no second order ever reaches the broker.
- **Each attempt re-prices**: After a timeout/cancel, the engine fetches a fresh `latest_price` and recalculates the order price with slippage. This means attempt 2 can have a different price than attempt 1.
- **ACK after terminal state**: The Redis message is only acknowledged after the engine writes the final status to SQLite. If the process crashes mid-execution, the message remains pending in the consumer group and will be redelivered (idempotency protects against double-execution).

### Adapter isolation (Protocol pattern)

`qmt_follower/executor.py` defines two `Protocol` classes — `MarketDataAdapter` and `BrokerAdapter` — that the execution engine depends on. The concrete QMT implementations live in `qmt_follower/adapters/qmt.py`. This means:

- Tests inject `FakeMarketData` and `FakeBroker` without touching QMT.
- `QmtMarketDataAdapter` is implemented and uses `xtdata.get_full_tick()` for latest prices.
- **`QmtBrokerAdapter.__init__()` deliberately raises `QmtAdapterNotConfigured`** — it is a placeholder. Real `submit_order`/`get_order_snapshot`/`cancel_order` must be wired to xttrader on a Windows machine with miniQMT installed. See `docs/windows-qmt-adapter-handoff.md` for the full implementation guide.

### Key modules

| Module | Role |
|---|---|
| `qmt_follower/models.py` | All dataclasses and enums: `TradeSignal`, `ExecutionConfig`, `BrokerOrderStatus`, `ExecutionStatus`, `OrderSnapshot`, `ExecutionResult` |
| `qmt_follower/config.py` | JSON config loading with `${ENV_VAR}` password resolution |
| `qmt_follower/store.py` | SQLite execution ledger: WAL mode, `signals` table (PK=signal_id), `order_attempts` table (one row per broker submission) |
| `qmt_follower/pricing.py` | Slippage calculation and price deviation guard (`PriceDeviationError`) |
| `qmt_follower/executor.py` | Core state machine — depends only on the two Protocol adapters and the store |
| `qmt_follower/redis_stream.py` | Redis Stream consumer group wrapper: `read_forever()`, `ack()`, `publish_signal()` |
| `qmt_follower/app.py` | DI wiring: connects config → Redis → SQLite → adapters → engine, then runs the `read_forever` → execute → ack loop |
| `qmt_follower/adapters/qmt.py` | Concrete QMT adapters (market data ready, broker placeholder) |
| `joinquant_signal_sender.py` | Standalone JoinQuant-side function; no imports from `qmt_follower/` |
| `strategies/etf_discount_live_signal_strategy.py` | Reference JoinQuant strategy with the sender function embedded |

### Signal contract

Signals are JSON objects written to Redis Stream. The `signal_id` field is the idempotency key:

```json
{
  "signal_id": "hunter-20260608093001-000001XSHE-buy-1000",
  "strategy_id": "hunter",
  "mode": "live",
  "action": "buy",
  "code": "000001.XSHE",
  "amount": 1000,
  "reference_price": 10.0,
  "created_at": "2026-06-08 09:30:01"
}
```

`reference_price` is the strategy's opinion, **not** the final order price — the Windows side always reprices with live market data and configured slippage. The `mode` field distinguishes live vs backtest; backtest signals are silently dropped by the sender.

### SQLite schema

Two tables:
- `signals` — one row per unique `signal_id`, tracks terminal `status` and cumulative `filled_qty`
- `order_attempts` — one row per broker submission (attempt 1, 2, 3…), linked to `signals.signal_id` via FK, records `broker_order_id`, `quantity`, `price`, per-attempt `filled_qty` and `status`

WAL mode + `synchronous=NORMAL` reduce write latency on the hot path.

## Config

Copy `config.example.json` to `config.json`. Password supports `${ENV_VAR}` syntax (e.g., `"${REDIS_PASSWORD}"` reads from the environment). Key tunables live under `execution`:
- `buy_slippage_pct` / `sell_slippage_pct` — fixed-percentage price adjustment per order side
- `order_timeout_sec` — per-attempt poll deadline before cancel+retry
- `max_attempts` / `max_total_duration_sec` — hard stops on the retry loop
- `max_deviation_from_signal_price_pct` — risk gate: refuse to trade if live price deviates beyond this from the JoinQuant reference price

## Testing

Tests use only stdlib `unittest` and inject fakes:
- `FakeMarketData` with a pre-loaded price queue
- `FakeBroker` with pre-loaded `OrderSnapshot` sequences per order ID
- `FakeRedis` modules monkeypatched into `sys.modules["redis"]`
- `SQLiteExecutionStore` backed by `tempfile.TemporaryDirectory`

No real Redis, QMT, or network is needed to run the full test suite.

## QMT Broker Adapter Status

`QmtBrokerAdapter` is **not** implemented. Its constructor raises `QmtAdapterNotConfigured` to prevent accidental live trading. The full implementation guide, including code-to-code mapping (XSHG→SH, XSHE→SZ), xttrader API discovery steps, state mapping requirements, and verification sequence, is in [docs/windows-qmt-adapter-handoff.md](docs/windows-qmt-adapter-handoff.md). Do not alter `OrderExecutionEngine` to work around missing broker functionality — implement the adapter instead.

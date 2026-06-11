# QMT Live Follower Design

## Goal

Build a Windows-side live execution service that consumes JoinQuant order signals from Redis Stream and executes them through miniQMT with strict idempotency, fixed-percent slippage pricing, timeout cancellation, and automatic re-ordering for remaining quantity.

## Confirmed Decisions

- JoinQuant sends order instructions, not target positions.
- `signal_id` is the idempotency key. One signal may be executed once only.
- The execution price is calculated on the Windows side from miniQMT/xtquant latest price.
- Slippage uses fixed percentages per side.
- On timeout, the service cancels the active order, fetches the latest price, applies slippage again, and submits a new order for the remaining quantity.
- Partial fills continue automatically for the remaining quantity.
- Slippage, timeout, retry count, and total duration are config driven.

## Architecture

The service is split into a testable execution core and IO adapters. The core owns signal validation, idempotency, price calculation, state transitions, and retry behavior. Redis and miniQMT are adapters around that core.

```text
JoinQuant realtime simulation
  -> Redis Stream: tidal_quant_signals
  -> Windows service
     -> RedisStreamReceiver
     -> SQLiteExecutionStore
     -> OrderExecutionEngine
     -> MarketDataAdapter
     -> BrokerAdapter
     -> execution reports and logs
```

## Components

- `qmt_follower.models`: dataclasses and enums for signals, config, broker orders, and execution results.
- `qmt_follower.pricing`: fixed-percent slippage calculation and deviation guard.
- `qmt_follower.store`: SQLite-backed idempotency and execution state records.
- `qmt_follower.executor`: order execution state machine.
- `qmt_follower.redis_stream`: Redis Stream consumer and producer helpers.
- `qmt_follower.adapters.qmt`: miniQMT adapter boundary. The first implementation keeps QMT-specific code isolated behind interfaces.

## State Machine

```text
RECEIVED -> ACCEPTED -> PRICE_CALCULATED -> ORDER_SUBMITTED -> WAITING_FILL -> FILLED
WAITING_FILL -> PARTIALLY_FILLED -> CANCELING -> CANCELED -> REPRICE -> ORDER_SUBMITTED
```

Terminal failure states:

- `DUPLICATE_IGNORED`
- `FAILED_RISK`
- `FAILED_TIMEOUT`
- `PARTIALLY_FILLED_TIMEOUT`
- `FAILED_BROKER`

## Redis Stream Semantics

JoinQuant writes signals with `XADD`. A successful `XADD` means the signal has been durably accepted by Redis. It does not mean the Windows service has received, submitted, or filled the order.

The Windows service consumes with a consumer group and acknowledges with `XACK` only after the signal reaches a terminal local state. Execution reports can be written to a separate stream later.

## First MVP Scope

- Single strategy, single account, single Windows executor.
- Redis Stream consumption.
- SQLite idempotency and execution records.
- Fixed-percent slippage pricing.
- Retry state machine for timeout and partial fill.
- File/stdout logs.
- Fake adapters for automated tests.

## Out Of Scope For MVP

- Web admin console.
- Multi-account allocation.
- Target-position reconciliation.
- Human approval workflow.
- Full production monitoring dashboard.

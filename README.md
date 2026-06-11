# JoinQuant miniQMT Live Follower

This project is a Windows-side execution service for JoinQuant live simulation signals. JoinQuant writes order instructions into Redis Stream, and the Windows service executes them through a broker adapter such as miniQMT.

## Signal Contract

Use a strict idempotency key. The same `signal_id` is accepted once only.

```json
{
  "signal_id": "hunter-20260608-093001-000001XSHE-buy-001",
  "strategy_id": "hunter",
  "mode": "live",
  "action": "buy",
  "code": "000001.XSHE",
  "amount": 1000,
  "reference_price": 10.0,
  "created_at": "2026-06-08 09:30:01"
}
```

JoinQuant should write the signal with Redis `XADD`. A successful `XADD` confirms Redis accepted the signal; it does not confirm Windows received it, submitted it, or filled it.

## JoinQuant Sender

The optimized JoinQuant-side function lives in `joinquant_signal_sender.py`. It keeps the original function signature and replaces only the internals: Redis `PUBLISH` becomes Redis Stream `XADD`, the payload gets `signal_id`, the execution anchor price is named `reference_price`, and old backtest signals are skipped.

The integrated ETF strategy with this function embedded is saved at `strategies/etf_discount_live_signal_strategy.py`.

Minimal usage inside a JoinQuant strategy:

```python
from joinquant_signal_sender import publish_trade_signal_to_redis

publish_trade_signal_to_redis(context, "buy", "000001.XSHE", 1000, 10.0)
```

Keep your existing strategy call sites unchanged. Edit Redis host, password, stream, and `strategy_id` inside `publish_trade_signal_to_redis`.

The generated `signal_id` is deterministic for the same strategy, timestamp, symbol, side, and amount:

```text
hunter-20260608093001-000001XSHE-buy-1000
```

Because the public function signature remains unchanged, two intentional orders with the same strategy, `context.current_dt`, symbol, side, and amount will share one `signal_id` and the Windows side will treat the second one as a duplicate. If that case matters later, add an internal sequence suffix when constructing `signal_id` inside the function.

Do not make JoinQuant wait for the Windows execution result. JoinQuant only needs to know whether Redis accepted the signal. The Windows service records execution status locally and can publish execution reports separately later.

## Execution Behavior

- The Windows service gets the latest price from miniQMT/xtquant.
- Buy price is `latest_price * (1 + buy_slippage_pct)`.
- Sell price is `latest_price * (1 - sell_slippage_pct)`.
- If an order times out, the service cancels it, keeps filled shares, fetches a fresh latest price, and reorders only the remaining quantity.
- Attempts stop at `max_attempts` or `max_total_duration_sec`.

## Low-Latency Settings

- JoinQuant Redis client is cached inside `publish_trade_signal_to_redis`, so repeated open-time signals do not create a new Redis connection each time.
- Redis Stream consumer uses `redis.block_ms` from config. The example value is `20`, which means the reader wakes quickly while still avoiding a tight busy loop.
- SQLite uses WAL mode and `synchronous=NORMAL` to reduce local state-write latency.
- miniQMT/xttrader live trading still requires account-specific `QmtBrokerAdapter` wiring before real orders can be submitted.

## Local Verification

```bash
python -m unittest discover -v
python -m compileall qmt_follower tests
```

## Runtime

Copy `config.example.json` to `config.json`, set `REDIS_PASSWORD`, and run:

```bash
python main.py
```

To use a non-default config path:

```bash
python main.py --config config.prod.json
```

The QMT broker adapter is intentionally isolated. The execution core is ready for testing, but account-specific `xttrader` wiring must be completed before live trading.

Windows miniQMT implementation handoff: `docs/windows-qmt-adapter-handoff.md`.

# Redis Signal Expiration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make both the independent miniQMT service and the single-file big QMT fallback reject and ACK Redis trade signals whose optional `expire_at` deadline has passed.

**Architecture:** Preserve the existing Redis payload and ACK flows. The miniQMT path stores `expire_at` on `TradeSignal` and performs the hard-deadline check inside `OrderExecutionEngine.execute()` after idempotency registration but before any market-data or broker access; the big QMT path performs the same check when a FIFO message reaches the head of `pending`, immediately before creating an active order. Missing deadlines retain legacy behavior, while malformed deadlines fail closed with explicit terminal logging.

**Tech Stack:** Python 3, stdlib `datetime`, `time`, `unittest`, SQLite, Redis Streams, miniQMT/xtquant adapter boundary.

## Global Constraints

- Use the existing `expire_at` string format exactly: `YYYY-MM-DD HH:MM:SS`.
- Do not add configuration options, dependencies, schema migrations, retries, fallbacks, or Redis pending recovery.
- Do not modify sender-side expiry generation or the `execute_at` scheduling rule.
- Missing or empty `expire_at` keeps existing execution behavior.
- Expired and malformed signals must not query market data, funds, positions, or submit an order.
- Expired signals are terminal and ACKed; malformed deadlines fail closed, are logged explicitly, and are ACKed.
- Preserve existing user changes and leave the unrelated untracked `output/` directory untouched.

---

### Task 1: Enforce expiration in the independent miniQMT service

**Files:**
- Modify: `qmt_follower/models.py:25-117`
- Modify: `qmt_follower/store.py:120-155`
- Modify: `qmt_follower/executor.py:65-207`
- Test: `tests/test_runtime.py`
- Test: `tests/test_executor.py`
- Test: `tests/test_store.py`

**Interfaces:**
- Consumes: payload field `expire_at: str | None` using `%Y-%m-%d %H:%M:%S` local time.
- Produces: `TradeSignal.expire_at: str | None`, `ExecutionStatus.EXPIRED`, and terminal `ExecutionResult(status=ExecutionStatus.EXPIRED, filled_qty=0, attempts=0)`.

- [ ] **Step 1: Write failing parsing and persistence tests**

Add an `expire_at` value to the Redis payload in `tests/test_runtime.py` and assert that it survives parsing:

```python
"expire_at": "2026-06-08 09:30:20",
```

```python
self.assertEqual(message.signal.expire_at, "2026-06-08 09:30:20")
```

In `tests/test_store.py`, construct a signal with `expire_at="2026-07-10 09:30:20"`, read its `raw_json`, and assert:

```python
raw = json.loads(row["raw_json"])
self.assertEqual(raw["expire_at"], "2026-07-10 09:30:20")
```

- [ ] **Step 2: Write failing execution tests for expired and malformed deadlines**

Add two cases to `ExecutorTests` using empty fake adapters so any unintended query fails visibly:

```python
def test_expired_signal_is_recorded_without_market_or_broker_access(self):
    with tempfile.TemporaryDirectory() as tmpdir:
        store = SQLiteExecutionStore(Path(tmpdir) / "state.db")
        signal = TradeSignal(
            signal_id="sig-expired",
            strategy_id="hunter",
            action=Action.BUY,
            code="000001.XSHE",
            amount=1000,
            reference_price=10.0,
            created_at="2000-01-01 09:30:00",
            expire_at="2000-01-01 09:30:20",
        )
        market_data = FakeMarketData([])
        broker = FakeBroker({})
        engine = OrderExecutionEngine(store, market_data, broker, ExecutionConfig())

        result = engine.execute(signal)

        self.assertEqual(result.status, ExecutionStatus.EXPIRED)
        self.assertEqual(result.filled_qty, 0)
        self.assertEqual(result.attempts, 0)
        self.assertEqual(store.get_signal(signal.signal_id).status, ExecutionStatus.EXPIRED)
        self.assertEqual(market_data.queries, [])
        self.assertEqual(broker.submitted, [])
        self.assertEqual(broker.cash_queries, 0)

def test_invalid_expire_at_fails_closed_without_market_or_broker_access(self):
    with tempfile.TemporaryDirectory() as tmpdir:
        store = SQLiteExecutionStore(Path(tmpdir) / "state.db")
        signal = TradeSignal(
            signal_id="sig-invalid-expiry",
            strategy_id="hunter",
            action=Action.BUY,
            code="000001.XSHE",
            amount=1000,
            reference_price=10.0,
            created_at="2026-07-14 09:30:00",
            expire_at="09:30:20",
        )
        market_data = FakeMarketData([])
        broker = FakeBroker({})
        engine = OrderExecutionEngine(store, market_data, broker, ExecutionConfig())

        result = engine.execute(signal)

        self.assertEqual(result.status, ExecutionStatus.FAILED_RISK)
        self.assertIn("invalid expire_at", result.message)
        self.assertEqual(market_data.queries, [])
        self.assertEqual(broker.submitted, [])
        self.assertEqual(broker.cash_queries, 0)
```

- [ ] **Step 3: Run the focused tests and confirm the new expectations fail**

Run:

```bash
python -m unittest tests.test_runtime tests.test_store tests.test_executor -v
```

Expected: the new assertions fail because `TradeSignal` has no `expire_at`, `ExecutionStatus` has no `EXPIRED`, and the engine has no pre-broker expiry guard. Existing tests should remain green up to those failures.

- [ ] **Step 4: Add the optional field and terminal status**

In `qmt_follower/models.py`, add the enum member, console event, dataclass field, and parser assignment:

```python
class ExecutionStatus(StrEnum):
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
```

```python
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
}
```

```python
sent_at_ms: int | None = None
execute_at: str | None = None
expire_at: str | None = None
```

```python
expire_at = raw.get("expire_at")
```

```python
expire_at=str(expire_at) if expire_at else None,
```

- [ ] **Step 5: Preserve the deadline in the SQLite audit payload**

Add the optional field to the existing JSON dictionary in `SQLiteExecutionStore.try_accept_signal()`:

```python
"mode": signal.mode,
"expire_at": signal.expire_at,
```

Do not alter the SQLite schema.

- [ ] **Step 6: Add the miniQMT hard-deadline guard**

Add the status label in `qmt_follower/executor.py`:

```python
ExecutionStatus.EXPIRED: "信号过期",
```

Immediately after the existing “信号已登记” debug log and before the trading-halt check, add:

```python
if signal.expire_at:
    try:
        expire_at = dt.datetime.strptime(signal.expire_at, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        logger.error(
            "%s | %s | 过期时间非法 %r | 未下单",
            signal.console_event("失败"), short_code, signal.expire_at,
        )
        return self._finish(
            signal,
            ExecutionStatus.FAILED_RISK,
            0,
            0,
            "invalid expire_at: %s" % signal.expire_at,
        )
    now = dt.datetime.now()
    if now > expire_at:
        overdue_sec = (now - expire_at).total_seconds()
        logger.warning(
            "%s | %s | 截止 %s | 已过期 %.1fs | 未下单",
            signal.console_event("过期"), short_code, signal.expire_at, overdue_sec,
        )
        return self._finish(
            signal,
            ExecutionStatus.EXPIRED,
            0,
            0,
            "signal expired at %s" % signal.expire_at,
        )
```

This location is the required FIFO safeguard: the comparison runs when the single worker actually starts the signal, not when Redis first yields it.

- [ ] **Step 7: Run focused miniQMT tests**

Run:

```bash
python -m unittest tests.test_runtime tests.test_store tests.test_executor tests.test_main_entrypoint -v
```

Expected: all tests pass; the expired and malformed cases show zero market-data and broker calls, and legacy signals without `expire_at` remain green.

- [ ] **Step 8: Commit the miniQMT slice**

```bash
git add qmt_follower/models.py qmt_follower/store.py qmt_follower/executor.py tests/test_runtime.py tests/test_store.py tests/test_executor.py
git commit -m "feat: expire stale miniQMT signals"
```

### Task 2: Enforce expiration in the single-file big QMT fallback

**Files:**
- Modify: `bigqmt_follower/bigqmt_redis_follower.py:58-88`
- Modify: `bigqmt_follower/bigqmt_redis_follower.py:412-439`
- Test: `tests/test_bigqmt_redis_follower.py`

**Interfaces:**
- Consumes: `signal["expire_at"]: str | None` and the existing injectable `BigQmtRuntime.clock() -> float` epoch seconds.
- Produces: big QMT terminal log statuses `EXPIRED` and `FAILED_INVALID_SIGNAL`, with the original Redis message ID placed in `ack_queue` and no active order created.

- [ ] **Step 1: Write failing parser and runtime tests**

Extend `trade_message()` with an optional deadline:

```python
def trade_message(
    message_id, signal_id, action="buy", code="000001.XSHE", amount=1000,
    expire_at=None,
):
    message = {
        "kind": "trade",
        "message_id": message_id,
        "signal": {
            "signal_id": signal_id,
            "strategy_id": "hunter",
            "mode": "live",
            "action": action,
            "code": code,
            "amount": amount,
            "reference_price": 10.0,
            "created_at": "2026-07-12 09:30:00",
            "sent_at_ms": None,
        },
    }
    if expire_at is not None:
        message["signal"]["expire_at"] = expire_at
    return message
```

Add a parser assertion using `expire_at="2026-07-12 09:30:20"` and these runtime cases:

```python
def test_expired_fifo_signal_is_acked_without_submission_and_next_signal_continues(self):
    expired_now = time.mktime(datetime.datetime(2026, 7, 12, 9, 30, 21).timetuple())
    self.runtime.clock = lambda: expired_now
    self.inbound.put(
        trade_message("310-0", "sig-expired", expire_at="2026-07-12 09:30:20")
    )
    self.inbound.put(trade_message("311-0", "sig-valid"))

    self.runtime.on_timer(self.context)

    self.assertEqual(self.acks.get_nowait(), "310-0")
    self.assertEqual(self.gateway.submissions, [])
    self.assertIsNone(self.runtime.active)

    self.runtime.on_timer(self.context)

    self.assertEqual(len(self.gateway.submissions), 1)
    self.assertEqual(self.runtime.active["signal"]["signal_id"], "sig-valid")

def test_invalid_expire_at_is_acked_without_submission(self):
    self.inbound.put(
        trade_message("312-0", "sig-invalid-expiry", expire_at="09:30:20")
    )

    self.runtime.on_timer(self.context)

    self.assertEqual(self.acks.get_nowait(), "312-0")
    self.assertEqual(self.gateway.submissions, [])
    self.assertIsNone(self.runtime.active)
```

The local-time conversion keeps the test aligned with the runtime's naive-local-time contract on every development machine.

- [ ] **Step 2: Run the big QMT tests and confirm the new expectations fail**

Run:

```bash
python -m unittest tests.test_bigqmt_redis_follower -v
```

Expected: the parser drops `expire_at`, and the expired/malformed signals currently reach `gateway.submit()`.

- [ ] **Step 3: Preserve and classify the deadline in the single-file runtime**

In `parse_stream_message()`, add:

```python
expire_at = raw.get("expire_at")
```

and preserve it in the trade signal:

```python
"expire_at": str(expire_at) if expire_at else None,
```

Add this module-level helper near the other payload/pricing helpers:

```python
def signal_expiration_status(signal, now_epoch):
    expire_at = signal.get("expire_at")
    if not expire_at:
        return None
    try:
        deadline = datetime.datetime.strptime(expire_at, "%Y-%m-%d %H:%M:%S")
    except (TypeError, ValueError):
        return "FAILED_INVALID_SIGNAL"
    if now_epoch > time.mktime(deadline.timetuple()):
        return "EXPIRED"
    return None
```

- [ ] **Step 4: Reject stale FIFO heads before active-order creation**

In `BigQmtRuntime.on_timer()`, immediately after `message = self.pending.popleft()` and before assigning `self.active`, add:

```python
expiration_status = signal_expiration_status(message["signal"], self.clock())
if expiration_status is not None:
    level = "ERROR" if expiration_status == "FAILED_INVALID_SIGNAL" else "WARNING"
    _log(
        level,
        "信号终态 %s status=%s expire_at=%r 未下单"
        % (
            message["signal"]["signal_id"],
            expiration_status,
            message["signal"].get("expire_at"),
        ),
    )
    self.worker.ack_queue.put(message["message_id"])
    return
```

Do not remove the signal from `seen_signal_ids`; `_drain_inbound()` has already registered it, preserving same-run deduplication.

- [ ] **Step 5: Run the focused big QMT tests**

Run:

```bash
python -m unittest tests.test_bigqmt_redis_follower -v
```

Expected: all tests pass; expired and malformed signals ACK without `gateway.submit()`, the following FIFO signal remains executable, and messages without `expire_at` behave unchanged.

- [ ] **Step 6: Commit the big QMT slice**

```bash
git add bigqmt_follower/bigqmt_redis_follower.py tests/test_bigqmt_redis_follower.py
git commit -m "feat: expire stale big QMT signals"
```

### Task 3: Align operator documentation and verify the complete change

**Files:**
- Modify: `README.md:65-75`
- Modify: `README.md:246-260`
- Modify: `bigqmt_follower/README.md:75-86`

**Interfaces:**
- Consumes: the completed miniQMT `expired` status and big QMT `EXPIRED`/`FAILED_INVALID_SIGNAL` behavior.
- Produces: operator documentation that distinguishes signal expiry from order timeout and Redis pending recovery.

- [ ] **Step 1: Update the root README**

Add an execution guarantee beside the existing scheduling and observability bullets:

```markdown
- **信号过期保护**：带 `expire_at` 的交易信号在真正开始执行时再次校验；过期或格式非法时不查询交易资源、不下单，并记录终态后 ACK。未携带该字段的旧信号保持兼容。
```

Replace the current field description that says the executor ignores `expire_at` with two rows:

```markdown
| `expire_at` | 可选，`YYYY-MM-DD HH:MM:SS`；两个执行端在真正下单前校验，过期后不执行并 ACK |
| `nonce` | 发送侧审计字段；当前执行端不据此去重 |
```

Keep the existing pending/`XCLAIM` warning unchanged.

- [ ] **Step 2: Update the big QMT fallback README**

Add this bullet before the consumer-group restart boundary:

```markdown
- 带 `expire_at` 的交易信号在到达 FIFO 队首时校验；过期或格式非法时不调用交易接口，记录终态并 ACK。没有该字段的旧信号继续执行。
```

- [ ] **Step 3: Run formatting and focused regression checks**

Run:

```bash
git diff --check
python -m unittest tests.test_runtime tests.test_store tests.test_executor tests.test_main_entrypoint tests.test_bigqmt_redis_follower -v
```

Expected: `git diff --check` exits 0 and all focused tests pass.

- [ ] **Step 4: Run repository-level verification**

Run:

```bash
python -m unittest discover -v
python -m compileall qmt_follower scripts tests bigqmt_follower
```

Expected: the full test suite passes and compileall reports no syntax or import errors. These checks do not replace Windows miniQMT or big QMT client validation.

- [ ] **Step 5: Review the final scope and sensitive-data boundary**

Run:

```bash
git status --short
git diff --stat HEAD~2
git diff --check HEAD~2
```

Expected: only the planned model, store, executor, big QMT fallback, tests, and README files are changed; `output/` remains untracked and untouched; no config, credential, lockfile, generated file, or unrelated strategy logic appears.

- [ ] **Step 6: Commit the documentation slice**

```bash
git add README.md bigqmt_follower/README.md
git commit -m "docs: explain Redis signal expiration"
```

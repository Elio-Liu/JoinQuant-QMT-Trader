# QMT Open Scheduled ETF Buy Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Publish the stable ETF discount strategy's rebalance intent after 09:25, queue old-position sells in the auction immediately, and release the target ETF buy to the existing QMT FIFO engine at local 09:30:00.

**Architecture:** Add an optional `execute_at` value to `TradeSignal`, then keep future-dated stream messages in the QMT application loop until due. The strategy computes a pre-open fixed-share plan from its shadow portfolio, records only successfully published code/direction pairs, and keeps the 09:30 shadow rebalance without sending duplicates.

**Tech Stack:** Python 3, stdlib `datetime`, `concurrent.futures`, pandas in the JoinQuant strategy, stdlib `unittest`.

## Global Constraints

- Keep `publish_trade_signal_to_redis(context, action, code, amount, price)` unchanged.
- Do not change the existing Redis Stream, idempotency, FIFO, pricing, resource-cap, cancel, or retry semantics.
- Do not change other strategies' timing.
- Do not add dependencies, configuration, database tables, or public APIs.
- Preserve backtest convergence validation.
- The worktree already contains unrelated tracked and untracked work. Preserve it and do not create implementation commits that would capture that baseline.

---

### Task 1: Parse and schedule optional execution times

**Files:**
- Modify: `qmt_follower/models.py`
- Modify: `qmt_follower/app.py`
- Test: `tests/test_runtime.py`
- Test: `tests/test_main_entrypoint.py`

**Interfaces:**
- Consumes: existing `TradeSignal.from_dict(raw: dict)`, `StreamMessage`, `OrderExecutionEngine.execute(signal)` and single-worker `ThreadPoolExecutor`.
- Produces: `TradeSignal.execute_at: str | None`, `_queue_or_submit_trade(...) -> None`, and `_submit_due_scheduled(...) -> None`.

- [ ] **Step 1: Write failing parsing and scheduling tests**

Add a runtime assertion for the optional value:

```python
self.assertEqual(signal.execute_at, "2026-07-13 09:30:00")
```

Add application tests that construct a future-dated `StreamMessage`, verify the pool is not called before 09:30, then verify one submission at 09:30. Add a second immediate message while the first is waiting and verify it is submitted without delay. Add an invalid-date test expecting `ValueError`.

- [ ] **Step 2: Run tests and verify RED**

Run:

```bash
python -m unittest tests.test_runtime tests.test_main_entrypoint -v
```

Expected: failures because `execute_at` and the scheduling helpers do not exist.

- [ ] **Step 3: Add the minimal model field**

Add to `TradeSignal` and `from_dict()`:

```python
execute_at: str | None = None

execute_at = raw.get("execute_at")
execute_at=str(execute_at) if execute_at else None,
```

- [ ] **Step 4: Add application-layer scheduling**

Use a sorted list of `(datetime, message_id, StreamMessage)` entries. Parse only the documented local timestamp format:

```python
dt.datetime.strptime(signal.execute_at, "%Y-%m-%d %H:%M:%S")
```

`_queue_or_submit_trade()` stores future messages without ACK or pool submission; due/past and unscheduled messages call the existing pool submission path. `_submit_due_scheduled()` releases all due entries in stable timestamp/message-ID order. Call it on every Redis loop iteration before processing the newly yielded item. Log and leave invalid messages unacknowledged.

- [ ] **Step 5: Run tests and verify GREEN**

Run:

```bash
python -m unittest tests.test_runtime tests.test_main_entrypoint -v
```

Expected: all tests pass.

### Task 2: Publish the 09:25 live rebalance plan

**Files:**
- Modify: `strategies/潮汐量化-ETF稳健折价猎手.py`
- Test: `tests/test_stable_discount_hunter_logging.py`

**Interfaces:**
- Consumes: the existing five-argument Redis publisher, `g.signal_candidates`, `context.portfolio`, and `get_current_data()`.
- Produces: `_publish_open_rebalance_plan(context, selected, current_data)`, plus `g.prepublished_open_signals: set[tuple[str, str]]`.

- [ ] **Step 1: Write failing strategy tests**

Add tests proving that a live 09:25 selection:

```python
expected_calls = [
    mock.call(context, "sell", "OLD.XSHG", 1000, 1.01),
    mock.call(context, "buy", "TARGET.XSHG", expected_lot_qty, 0.98),
]
```

Also test the real embedded publisher payload so the early buy contains `execute_at` at 09:30 and its `expire_at` is later, while an early sell has no `execute_at`. Add a 09:30 live-path test whose latest price would fail the old convergence check but still reaches local `order_target_value()`.

- [ ] **Step 2: Run test and verify RED**

Run:

```bash
python -m unittest tests.test_stable_discount_hunter_logging -v
```

Expected: failures because no pre-open trading plan or scheduled payload exists.

- [ ] **Step 3: Implement minimal pre-open plan calculation**

Initialize the success markers in `initialize()` and call one focused helper after explicit selection, including an explicitly empty selection. The helper:

```python
desired = set(selected.index)
weights = selected["premium"].abs()
target_qty = int(target_value / reference_price / 100) * 100
buy_qty = target_qty - existing_qty
```

It publishes non-target positions as immediate sells, publishes positive target deltas as early buys, and adds `(action, code)` to the marker set only when `result["sent"]` is true.

- [ ] **Step 4: Add the scheduled payload without changing the public signature**

Inside the existing publisher, only a live buy whose context time is in `[09:25, 09:30)` receives:

```python
execute_at = context_time.replace(hour=9, minute=30, second=0, microsecond=0)
signal["execute_at"] = execute_at.strftime("%Y-%m-%d %H:%M:%S")
```

Use `execute_at + SIGNAL_EXPIRE_SECONDS` for that signal's `expire_at`; keep all other signals unchanged.

- [ ] **Step 5: Preserve the shadow account and suppress only confirmed duplicates**

In live mode, use the 09:25 candidates directly instead of applying convergence filtering. `_emit_trade_signal()` checks the successful `(action, code)` markers before publishing and logs suppression. Clear the marker set in `validate_and_trade()` cleanup so 14:25 risk-control signals remain unaffected. Backtest mode continues through the current convergence block.

- [ ] **Step 6: Run test and verify GREEN**

Run:

```bash
python -m unittest tests.test_stable_discount_hunter_logging -v
```

Expected: all tests pass.

### Task 3: Scoped regression and compile verification

**Files:**
- Verify only; no additional production files unless a scoped failure identifies a direct regression.

**Interfaces:**
- Consumes: completed Tasks 1 and 2.
- Produces: fresh verification evidence and a scoped diff review.

- [ ] **Step 1: Run combined relevant tests**

```bash
python -m unittest tests.test_stable_discount_hunter_logging tests.test_runtime tests.test_main_entrypoint -v
```

Expected: all tests pass with zero failures and errors.

- [ ] **Step 2: Compile-check runtime, scripts, and tests**

```bash
python -m compileall qmt_follower scripts tests
```

Expected: exit code 0.

- [ ] **Step 3: Review only the scoped diff**

```bash
git diff --check -- qmt_follower/models.py qmt_follower/app.py tests/test_runtime.py tests/test_main_entrypoint.py tests/test_stable_discount_hunter_logging.py
git status --short
```

Also inspect the ignored strategy directly because it will not appear in ordinary status output. Confirm no unrelated file was edited during implementation.

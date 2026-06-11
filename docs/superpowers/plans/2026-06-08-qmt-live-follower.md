# QMT Live Follower Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a tested MVP for a Redis Stream to miniQMT live order follower.

**Architecture:** A pure Python execution core owns idempotency, pricing, and retry state transitions. Redis and miniQMT are replaceable adapters so the dangerous real-money integration can be tested separately from order logic.

**Tech Stack:** Python 3.11+, standard library `sqlite3`, `dataclasses`, `unittest`; optional runtime dependencies `redis` and `xtquant` for production adapters.

---

### Task 1: Core Models And Pricing

**Files:**
- Create: `qmt_follower/models.py`
- Create: `qmt_follower/pricing.py`
- Test: `tests/test_pricing.py`

- [ ] Write failing tests for buy/sell slippage and deviation rejection.
- [ ] Implement model dataclasses and pricing helpers.
- [ ] Run `python -m unittest tests.test_pricing -v`.

### Task 2: SQLite Idempotency Store

**Files:**
- Create: `qmt_follower/store.py`
- Test: `tests/test_store.py`

- [ ] Write failing tests proving duplicate `signal_id` cannot be accepted twice.
- [ ] Implement SQLite schema and signal status updates.
- [ ] Run `python -m unittest tests.test_store -v`.

### Task 3: Order Execution State Machine

**Files:**
- Create: `qmt_follower/executor.py`
- Test: `tests/test_executor.py`

- [ ] Write failing tests for partial-fill re-ordering of remaining quantity.
- [ ] Write failing tests for max-attempt timeout terminal state.
- [ ] Implement the execution loop against broker and market-data protocols.
- [ ] Run `python -m unittest tests.test_executor -v`.

### Task 4: Redis Stream And Runtime Entrypoint

**Files:**
- Create: `qmt_follower/redis_stream.py`
- Create: `qmt_follower/config.py`
- Create: `qmt_follower/app.py`
- Create: `config.example.json`
- Create: `README.md`

- [ ] Add config loading.
- [ ] Add Redis Stream consumer helper.
- [ ] Add executable app skeleton that wires config, store, Redis, and adapters.
- [ ] Document JoinQuant signal format and runtime usage.

### Task 5: Full Verification

**Files:**
- Modify as needed.

- [ ] Run `python -m unittest discover -v`.
- [ ] Run `python -m compileall qmt_follower tests`.
- [ ] Review final file tree and summarize implementation status.

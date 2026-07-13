# Terminal Logging and Repository Cleanup Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement the approved single-tag Emoji console format, make tests/scripts reproducible, remove stale references, and create one verified commit.

**Architecture:** Keep event semantics in `TradeSignal` so app/executor call sites share one format. Keep the logging formatter responsible only for fallback level tags. Clean repository metadata and documentation without changing the trading state machine.

**Tech Stack:** Python 3, stdlib logging/unittest, Ruff, Git.

## Global Constraints

- Preserve FIFO execution, MiniQMT resource refresh, cancel confirmation, Redis contracts, SQLite schema, and pricing behavior.
- Keep every retry as one INFO line with complete attempt details.
- Never stage local configuration, strategy credentials, runtime data, or logs.
- Create exactly one final commit after all checks pass.

---

### Task 1: Trade log tags and Emoji semantics

**Files:**
- Modify: `qmt_follower/models.py`
- Modify: `qmt_follower/app.py`
- Modify: `qmt_follower/executor.py`
- Test: `tests/test_terminal_logging.py`

- [ ] Write failing tests asserting `【卖单】📥 信号#XXXX`, `【卖单】🔁 信号#XXXX`, and `【卖单】✅ 信号#XXXX`, with no `【信号】` or `【重试】`.
- [ ] Run `python -m unittest tests.test_terminal_logging -v` and confirm the format assertions fail against the old stacked tags.
- [ ] Replace `TradeSignal.console_prefix/console_event` output with one category tag plus the fixed event Emoji map; add queue depth to receipt logs and live quote/book values to retry lines.
- [ ] Re-run `python -m unittest tests.test_terminal_logging -v` and confirm all logging tests pass.

### Task 2: System-line Emoji fallback

**Files:**
- Modify: `qmt_follower/logging_config.py`
- Modify: `qmt_follower/app.py`
- Modify: `qmt_follower/redis_stream.py`
- Modify: `qmt_follower/adapters/qmt.py`
- Test: `tests/test_terminal_logging.py`

- [ ] Write failing formatter tests for `【警告】⚠️` fallback and single-tag `【系统】🚀`, `【QMT】🔌`, `【Redis】📡`, `【行情】📡` messages.
- [ ] Run the focused tests and confirm missing Emoji assertions fail.
- [ ] Add level Emoji fallback and normalize visible system messages without changing their log levels or exception behavior.
- [ ] Re-run focused tests and verify pass.

### Task 3: Reproducible scripts and tests

**Files:**
- Modify: `.gitignore`
- Modify: `scripts/send_manual_signal.py`
- Modify: `scripts/send_batch_signals.py`
- Modify: `tests/test_batch_signal_sender.py`
- Delete: `tests/test_strategy_integration.py`
- Create: `tests/test_manual_signal_sender.py`

- [ ] Write failing tests proving Redis targets come from environment variables and the current batch totals are 5,000 sell / 500 buy shares.
- [ ] Run sender tests and confirm the old hard-coded target/stale total assertions fail.
- [ ] Replace hard-coded Redis host/password with `QMT_REDIS_HOST`, `QMT_REDIS_PORT`, `QMT_REDIS_PASSWORD`, and `QMT_REDIS_STREAM`; validate missing remote host before sending.
- [ ] Stop ignoring `scripts/` and `tests/`, keep credential-bearing `strategies/` ignored, and remove the obsolete nonexistent-strategy test.
- [ ] Run all sender/runtime tests and verify pass.

### Task 4: Reference and source cleanup

**Files:**
- Modify: `README.md`
- Modify: `AGENTS.md`
- Modify: `docs/windows-qmt-adapter-handoff.md`
- Modify: `qmt_follower/logging_config.py`
- Delete: `.DS_Store`
- Delete: `images/.DS_Store`

- [ ] Update documentation to current FIFO/resource/cancel behavior and actual file paths.
- [ ] Replace the stale handoff implementation plan with a concise Windows deployment verification checklist.
- [ ] Remove unused imports and the unused `get_logger` helper; delete generated Finder metadata.
- [ ] Run `ruff check --select F401,F821,F841 .` and correct only proven issues in project Python files.

### Task 5: Verify and commit once

**Files:** all intended project changes.

- [ ] Run focused logging, executor, adapter, store, sender, and runtime tests.
- [ ] Run `python -m unittest discover -v` and require zero failures/errors.
- [ ] Run `python -m compileall qmt_follower scripts tests`.
- [ ] Run Ruff, `git diff --check`, reference scans, and a staged secret scan.
- [ ] Stage only reviewed source, tests, safe scripts, and documentation.
- [ ] Create one commit describing FIFO safety, structured Emoji logs, and repository cleanup.

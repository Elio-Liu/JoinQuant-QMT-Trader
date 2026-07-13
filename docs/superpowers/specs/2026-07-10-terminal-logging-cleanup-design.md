# Terminal Logging and Repository Cleanup Design

## Goal

Make the Windows trading console easy to scan by giving every visible line one semantic emoji, while keeping trade tags compact (`【买单】` or `【卖单】`) and preserving one complete line for every retry. Then make the repository reproducible and internally consistent before creating one commit.

## Console format

Trade lines use this fixed shape:

```text
HH:MM:SS 【买单|卖单】<emoji> 信号#<4位短编号> | details
```

The redundant `【信号】` prefix and event tags such as `【重试】` are removed. The event is represented by exactly one emoji:

- `📥` signal received and queued
- `🔁` retry after a confirmed terminal result for the previous order
- `✅` fully filled
- `⚠️` risk adjustment or insufficient resources
- `❌` broker/order failure
- `🛑` trading halted because cancel state is uncertain
- `⏭️` duplicate ignored
- `⏳` auction or queue waiting

System domains use one tag and one emoji, for example `【系统】🚀`, `【QMT】🔌`, `【Redis】📡`, and `【行情】📡`.

Every retry remains one INFO line containing attempt number, live last/bid/ask prices, submitted price and quantity, final fill for that attempt, confirmed cancel result, remaining quantity, and elapsed duration. Full signal IDs, QMT order IDs, SQLite writes, and poll details remain in DEBUG file logs.

## Formatter behavior

Structured messages beginning with `【` pass through unchanged. Unstructured visible messages receive a single level tag and emoji (`【信息】ℹ️`, `【警告】⚠️`, `【错误】❌`, `【严重】🛑`). The default INFO console therefore has one emoji per line without adding ANSI color dependencies or changing file-log detail.

## Repository cleanup

- Track `tests/` so the commit includes its regression evidence.
- Track `scripts/` after replacing hard-coded Redis connection values with environment variables.
- Keep `strategies/` ignored because deployment copies contain environment-specific Redis credentials.
- Remove the obsolete integration test that references the nonexistent `strategies/etf_discount_live_signal_strategy.py`.
- Update the batch-script expectation to the current 5,000-share sell / 500-share buy stress batch.
- Rewrite stale README, AGENTS, and Windows handoff statements so they describe FIFO execution, live resource queries, cancel confirmation, and current scripts.
- Remove only proven-unused imports/helpers and generated `.DS_Store` files.
- Keep the project promotion document and referenced donation images.

## Safety boundaries

No change is made to pricing, FIFO ordering, resource capping, Redis payloads, SQLite schema, or MiniQMT order behavior. Local `config.json`, strategy deployment files, data, logs, and credentials are not staged. The final commit is created only after focused tests, the complete test suite, compile checks, Ruff checks, secret scans, and diff review pass.

## Acceptance criteria

1. `【信号】` and stacked event tags no longer appear in trade console messages.
2. Every default-console line contains one semantic emoji after its single category tag.
3. Every failed/partial attempt still emits exactly one detailed `🔁` line.
4. Tests and safe signal scripts are present in git status and work on a clean checkout.
5. The full test suite passes with no missing-file or stale stress-total failures.
6. No hard-coded Redis password or production host is staged.

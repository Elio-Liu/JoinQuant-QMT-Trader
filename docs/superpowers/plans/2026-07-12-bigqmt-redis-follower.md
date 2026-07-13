# 大 QMT Redis 信号执行端实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 新增一个可直接导入大 QMT 的单 Python 文件，在单次运行期间可靠消费现有 Redis Stream 信号并完成 FIFO 下单、成交跟踪、撤单和剩余量重试。

**Architecture:** Redis 守护线程只负责收信和 ACK 队列，大 QMT 的 500ms 定时回调在策略线程内推进内存状态机。所有大 QMT API 都通过单文件内的网关类调用，以便 macOS 测试注入 fake API；不使用 SQLite，也不恢复历史 pending。

**Tech Stack:** Python 标准库、redis-py、大 QMT `ContextInfo`/`passorder`/交易回调、stdlib `unittest`

## Global Constraints

- 不修改 `joinquant_signal_sender.py`、`qmt_follower/`、`main.py` 或现有配置。
- 唯一部署代码文件是 `bigqmt_follower/bigqmt_redis_follower.py`。
- 只支持普通股票账户，买卖操作类型固定为 `23/24`。
- `trading_enabled` 默认 `False`，账号或 Redis 地址为空时拒绝启动。
- 不使用 SQLite，不恢复历史 pending，所有状态只存在于单次运行内存中。
- 大 QMT 交易 API 只能从策略线程调用，Redis 线程不得调用 `ContextInfo` 或交易函数。
- 完整测试当前允许保留两个既有 `tests.test_manual_signal_sender` 失败，不修改该模块。

---

### Task 1: 信号解析与定价风控

**Files:**
- Create: `bigqmt_follower/bigqmt_redis_follower.py`
- Create: `tests/test_bigqmt_redis_follower.py`

**Interfaces:**
- Produces: `parse_stream_message(message_id, fields) -> dict`
- Produces: `jq_code_to_qmt_code(code) -> str`
- Produces: `tick_size_for(code) -> float`
- Produces: `calculate_order_price(signal, tick, config) -> float`

- [ ] **Step 1: 写解析和定价失败测试**

覆盖 payload JSON、旧字段、watchlist、代码转换、ETF tick、book 定价、盘口缺失回退和偏差拒绝。

- [ ] **Step 2: 运行测试确认 RED**

Run: `python -m unittest tests.test_bigqmt_redis_follower.BigQmtParsingAndPricingTests -v`

Expected: FAIL，因为部署模块或函数尚不存在。

- [ ] **Step 3: 写最小解析和定价实现**

在单文件中加入安全默认 `CONFIG`、`PriceDeviationError`、字段校验、聚宽代码转换和价格计算。使用 `Decimal` 按 tick 取整，买入向上、卖出向下。

- [ ] **Step 4: 运行测试确认 GREEN**

Run: `python -m unittest tests.test_bigqmt_redis_follower.BigQmtParsingAndPricingTests -v`

Expected: PASS。

### Task 2: Redis Stream 后台线程

**Files:**
- Modify: `bigqmt_follower/bigqmt_redis_follower.py`
- Modify: `tests/test_bigqmt_redis_follower.py`

**Interfaces:**
- Produces: `RedisStreamWorker(config, inbound_queue, ack_queue, redis_factory=None)`
- Produces: `RedisStreamWorker.ensure_group()`
- Produces: `RedisStreamWorker.process_ack_queue()`
- Produces: `RedisStreamWorker.run()` and `stop()`

- [ ] **Step 1: 写 Redis 行为失败测试**

验证新组以 `$` 创建、`BUSYGROUP` 被视为正常、`XREADGROUP` 固定使用 `>`、不读取 pending、主线程提交的 message ID 才会 `XACK`。

- [ ] **Step 2: 运行测试确认 RED**

Run: `python -m unittest tests.test_bigqmt_redis_follower.RedisStreamWorkerTests -v`

Expected: FAIL，因为 worker 尚不存在。

- [ ] **Step 3: 实现后台 worker**

worker 使用独立 Redis 连接、守护线程、线程安全收信队列和 ACK 队列。连接失败记录错误并固定间隔重连；任何日志都不包含密码。

- [ ] **Step 4: 运行测试确认 GREEN**

Run: `python -m unittest tests.test_bigqmt_redis_follower.RedisStreamWorkerTests -v`

Expected: PASS。

### Task 3: FIFO、资源校验和 passorder

**Files:**
- Modify: `bigqmt_follower/bigqmt_redis_follower.py`
- Modify: `tests/test_bigqmt_redis_follower.py`

**Interfaces:**
- Produces: `BigQmtGateway(context, config)`，封装行情、账户、持仓、委托查询、下单与撤单。
- Produces: `BigQmtRuntime(config, worker, gateway_factory=BigQmtGateway, clock=None)`。
- Produces: `BigQmtRuntime.on_timer(ContextInfo)`。

- [ ] **Step 1: 写 FIFO 和提交失败测试**

验证 watchlist 更新股票池并 ACK、重复 `signal_id` 不重下、单活动信号 FIFO、买入按资金缩量、卖出按可用持仓缩量，以及 `passorder(23/24, 1101, ..., 11, price, qty, strategy, 1, remark, ContextInfo)` 参数。

- [ ] **Step 2: 运行测试确认 RED**

Run: `python -m unittest tests.test_bigqmt_redis_follower.BigQmtSubmissionTests -v`

Expected: FAIL，因为 runtime 和 gateway 尚不存在。

- [ ] **Step 3: 实现运行期队列和首次下单**

在 `on_timer` 中先处理收信队列，再推进最多一个活动信号。每次提交重新查行情和资源；查询异常时明确失败，存在未知提交时熔断。`user_order_id` 使用 `signal_id` SHA-1 短摘要和 attempt 序号。

- [ ] **Step 4: 运行测试确认 GREEN**

Run: `python -m unittest tests.test_bigqmt_redis_follower.BigQmtSubmissionTests -v`

Expected: PASS。

### Task 4: 委托回调、撤单和剩余量重试

**Files:**
- Modify: `bigqmt_follower/bigqmt_redis_follower.py`
- Modify: `tests/test_bigqmt_redis_follower.py`

**Interfaces:**
- Produces: `BigQmtRuntime.on_order(order_info)`。
- Produces: `BigQmtRuntime.on_deal(deal_info)`。
- Produces: `BigQmtRuntime.halted` and terminal ACK behavior。

- [ ] **Step 1: 写执行状态机失败测试**

验证状态 `48/49/50/51/52/55/86/255/0` 保持在途，`53/54` 已撤，`56` 全成，`57` 废单；验证部分成交超时撤单、撤单期间晚到成交、只重报剩余数量、最大尝试终止、撤单不确定熔断和终态后 ACK。

- [ ] **Step 2: 运行测试确认 RED**

Run: `python -m unittest tests.test_bigqmt_redis_follower.BigQmtExecutionStateTests -v`

Expected: FAIL，因为回调和撤单推进尚未实现。

- [ ] **Step 3: 实现非阻塞执行状态机**

每次 timer 扫描当前策略委托以弥补回调延迟。`passorder` 调用后先进入 `WAITING_ORDER_ID`；看不到委托超过可见性超时则熔断。撤单请求后必须等待明确终态，随后把本次累计成交合并，再刷新行情和资源重报剩余量。

- [ ] **Step 4: 运行测试确认 GREEN**

Run: `python -m unittest tests.test_bigqmt_redis_follower.BigQmtExecutionStateTests -v`

Expected: PASS。

### Task 5: 大 QMT 生命周期入口和使用说明

**Files:**
- Modify: `bigqmt_follower/bigqmt_redis_follower.py`
- Create: `bigqmt_follower/README.md`
- Modify: `tests/test_bigqmt_redis_follower.py`

**Interfaces:**
- Produces: `init(ContextInfo)`、`handlebar(ContextInfo)`、`qmt_timer(ContextInfo)`、`order_callback(ContextInfo, orderInfo)`、`deal_callback(ContextInfo, dealInfo)`、`stop(ContextInfo)`。

- [ ] **Step 1: 写生命周期失败测试**

验证交易开关关闭或配置缺失时拒绝启动；有效配置调用 `set_account`、以 `500nMilliSecond` 注册定时器并启动 worker；全局回调转发给同一个 runtime。

- [ ] **Step 2: 运行测试确认 RED**

Run: `python -m unittest tests.test_bigqmt_redis_follower.BigQmtLifecycleTests -v`

Expected: FAIL，因为全局入口尚未接线。

- [ ] **Step 3: 实现入口并写 README**

README 说明单文件导入、顶部配置项、redis-py 安装、安全切换、仿真验收、重启丢状态和禁止与 miniQMT 同时运行。

- [ ] **Step 4: 运行新增测试和编译检查**

Run: `python -m unittest tests.test_bigqmt_redis_follower -v`

Expected: 全部 PASS。

Run: `python -m compileall bigqmt_follower tests/test_bigqmt_redis_follower.py`

Expected: exit 0。

### Task 6: 回归验证与范围审计

**Files:**
- Inspect only: all changed files

- [ ] **Step 1: 运行完整测试**

Run: `python -m unittest discover -v`

Expected: 大 QMT 新测试全部通过；只允许保留基线确认的两个 `tests.test_manual_signal_sender` 失败。

- [ ] **Step 2: 检查 diff 和编码**

Run: `git diff --check`

Expected: 无空白错误。

Run: `git status --short`

Expected: 本任务只新增计划、`bigqmt_follower/` 和 `tests/test_bigqmt_redis_follower.py`；其他显示项均为进入任务前已有改动。

- [ ] **Step 3: 对照设计逐项审计**

确认没有 SQLite、没有 pending 恢复、没有修改现有 miniQMT 文件、交易开关默认关闭、后台线程不调用 QMT API、终态才 ACK、未知撤单会熔断。

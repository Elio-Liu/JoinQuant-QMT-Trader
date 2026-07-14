# Redis 交易信号过期机制设计

## 背景

聚宽发送端已经在交易信号中写入 `expire_at`，默认有效期为 20 秒。当前独立 miniQMT 执行端和大 QMT 备用执行端都没有使用该字段，因此信号即使在 Redis、预约列表或 FIFO 队列中滞留，之后仍可能触发下单。

本次改动让两个执行端统一执行发送端给出的过期时间，避免迟到的交易意图继续触达券商。Redis pending 恢复和跨 consumer 的 `XCLAIM` 不属于本次范围。

## 目标

- miniQMT 与大 QMT 备用执行端使用相同的过期语义。
- 信号真正准备执行时检查 `expire_at`，覆盖 Redis 延迟、预约等待和 FIFO 排队。
- 过期信号不查询行情、资金或持仓，不提交订单，并在明确记录后 ACK。
- 没有 `expire_at` 的旧信号保持现有行为。
- 不增加配置项，不修改发送端字段格式，不修改 Redis Stream 契约。

## 非目标

- 不实现 Redis pending 自动认领或进程重启恢复。
- 不调整订单提交后的 `order_timeout_sec`、`max_attempts` 或 `max_total_duration_sec`。
- 不修改信号发送端默认的 20 秒有效期。
- 不改变 `execute_at` 的预约调度规则。

## 信号语义

`expire_at` 是可选的本地时间字符串，格式固定为 `YYYY-MM-DD HH:MM:SS`。

- 缺少或为空：视为旧协议信号，继续执行。
- 当前时间小于或等于 `expire_at`：信号仍有效，继续执行。
- 当前时间大于 `expire_at`：信号过期，不执行。
- 格式非法：视为无效交易信号，失败关闭，不执行并记录错误。

聚宽、miniQMT 和大 QMT 所在机器必须保持 NTP 校时。现有 `sent_at_ms` 延迟日志也依赖这一前提。

## miniQMT 执行端

### 数据模型

在 `TradeSignal` 末尾增加可选字段 `expire_at`，`from_dict()` 保留原始字符串。未携带该字段的现有构造和调用保持兼容。

在 `ExecutionStatus` 增加 `EXPIRED = "expired"`，用于 SQLite 账本和终态日志。SQLite 的状态列为文本，不需要数据库迁移。

### 检查位置

检查放在 `OrderExecutionEngine.execute()` 内：

1. 先沿用 `try_accept_signal()` 完成幂等登记。
2. 重复 `signal_id` 仍按现有逻辑返回，不改变去重语义。
3. 新信号在触发交易熔断判断、参数校验、行情查询和券商调用前检查 `expire_at`。
4. 已过期信号通过现有 `_finish()` 写入 `expired` 终态，成交量为 0、尝试次数为 0。
5. 上层 Future 正常完成后沿用现有 `_reap_one()` 执行 `XACK`。

检查必须在执行引擎中完成，不能只放在 Redis 收信处，否则进入单工作线程后排队过久的信号仍可能过期后下单。

格式非法时，执行引擎记录包含 `signal_id` 和非法值的错误上下文，使用现有 `failed_risk` 终态，不调用券商，并由现有完成流程 ACK。错误不能静默降级为“未配置过期时间”。

SQLite `raw_json` 同步记录 `expire_at`，便于盘后审计。

## 大 QMT 备用执行端

`parse_stream_message()` 在交易信号字典中保留可选 `expire_at`。

大 QMT 的检查发生在 FIFO 队首消息准备成为 `active`、调用 `_submit_attempt()` 之前：

1. 收信阶段保持现有去重和排队逻辑。
2. 从 `pending` 取出队首消息后检查 `expire_at`。
3. 已过期时记录 `EXPIRED` 终态日志；格式非法时记录 `FAILED_INVALID_SIGNAL`。两者都把 Redis message ID 放入 ACK 队列，并且不创建活动委托、不调用任何 QMT 交易接口。
4. 信号在本次运行中仍保留于 `seen_signal_ids`，相同 `signal_id` 不会重复下单。
5. 未过期信号继续沿用现有内存状态机。

大 QMT 使用已有可注入时钟 `self.clock` 进行判断，便于确定性测试。

## 预约信号

`execute_at` 调度语义保持不变。信号未到预约时间时继续等待，不提前 ACK；预约时间到达后进入执行引擎，再检查 `expire_at`。

当前 ETF 策略对预约买单生成的 `expire_at` 是 `execute_at + 20 秒`，因此正常情况下信号在预约时间后的 20 秒内仍可执行。若 FIFO 阻塞超过该时间，信号将被跳过。

## 日志与终态

过期日志应包含：

- 信号短任务 ID 或完整 `signal_id`
- 证券代码与买卖方向
- `expire_at`
- 当前判断时间或超时秒数
- 明确的“未下单、已 ACK”结果

miniQMT 使用 `expired` 账本终态；大 QMT 使用内存日志状态 `EXPIRED`。格式非法分别使用 miniQMT 的 `failed_risk` 和大 QMT 日志状态 `FAILED_INVALID_SIGNAL`，不伪装成正常过期。

## 测试

### miniQMT

- `TradeSignal.from_dict()` 正确解析有、无 `expire_at` 的信号。
- 未过期信号保持现有执行行为。
- 已过期信号落库为 `expired`，不调用行情或券商，最终 ACK。
- 信号被消费时有效、在 FIFO 等待后过期时不会下单。
- `execute_at` 到期释放后若已经过期，不会下单。
- 非法 `expire_at` 失败关闭，不调用券商，错误可见并 ACK。

### 大 QMT

- 解析后保留 `expire_at`。
- 队首过期信号不调用 `passorder`，进入 ACK 队列，后续有效信号仍可继续。
- FIFO 等待期间过期的信号被拦截。
- 缺少 `expire_at` 的旧信号行为不变。
- 非法 `expire_at` 不调用 QMT API，并产生明确错误和 ACK。

## 兼容性与风险

- `expire_at` 保持可选，不影响旧发送端和手工信号工具。
- 不改变 Redis Stream、消费组或 ACK 时机的总体契约。
- 不需要 SQLite schema migration。
- 最大风险是机器时钟偏差导致误判，部署验收需要确认聚宽侧、Windows 主机和 Redis/QMT 环境时间一致。
- 过期信号 ACK 后不会被自动重放；这是防止陈旧交易意图再次执行的预期行为。

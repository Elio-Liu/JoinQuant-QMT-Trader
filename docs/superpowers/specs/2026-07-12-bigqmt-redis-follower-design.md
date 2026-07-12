# 大 QMT Redis 信号执行端设计

## 目标

在不修改聚宽信号发送端、Redis Stream 消息格式和现有 miniQMT 执行端的前提下，新增一个可直接导入大 QMT 的单文件策略。该策略消费现有交易信号，并在大 QMT 普通股票账户中完成限价下单、成交跟踪、超时撤单和剩余数量重试。

这是 miniQMT 不可用时的备用执行方案。它只保证单次大 QMT 运行期间的执行完整性，不提供跨重启恢复或持久化账本。

## 范围

本次新增 `bigqmt_follower/`，核心交付物是一个可导入大 QMT 的 Python 文件：

```text
bigqmt_follower/
├── bigqmt_redis_follower.py
└── README.md
```

测试代码放在仓库现有 `tests/` 目录。现有 `joinquant_signal_sender.py`、`qmt_follower/`、`main.py` 和配置文件均不修改。

第一版只支持普通股票账户：

- 买入操作类型：`23`
- 卖出操作类型：`24`
- 单股、单账号、按股数下单：`1101`
- 指定限价：报价类型 `11`
- 实时立即触发：`quickTrade=1`

不支持信用账户、期货、期权、多账户或算法单。

## 部署形态

`bigqmt_redis_follower.py` 是唯一需要导入大 QMT 的代码文件。文件内部按逻辑分区组织配置、信号模型、Redis 消费、定价风控、内存状态机、大 QMT 网关和策略回调，不依赖仓库中的其他 Python 模块。

配置集中放在文件顶部的 `CONFIG` 字典。仓库只提交空账号、空 Redis 地址和空密码等安全占位值；交易开关默认关闭。实际部署时，用户复制文件后在 Windows 端填写配置并显式启用交易。

为兼容不同券商分发的大 QMT Python 环境，部署文件避免 `dataclass`、`Protocol`、结构化模式匹配和较新的类型注解。第三方依赖仅使用 Redis Python 客户端；若大 QMT 内置环境缺少该包，README 提供对应解释器的安装方法。

## Redis 契约

策略继续消费现有 Redis Stream，消息字段和聚宽发送端保持不变。交易消息中的 `payload` 是 JSON 字符串，核心字段包括：

- `signal_id`
- `strategy_id`
- `mode`
- `action`
- `code`
- `amount`
- `reference_price`，并兼容旧字段 `price`
- `created_at`，并兼容旧字段 `timestamp`
- 可选的 `sent_at_ms`

`mode` 不是 `live` 的消息不执行。代码格式从聚宽的 `000001.XSHE`、`510300.XSHG` 转为大 QMT 的 `000001.SZ`、`510300.SH`。

策略也识别现有 `action=subscribe` 的 watchlist 消息。watchlist 只更新内存股票池并调用 `ContextInfo.set_universe()`，随后 ACK，不产生委托。

大 QMT 使用独立消费组，默认名为 `bigqmt_executors`。创建消费组时从 `$` 开始，避免第一次部署时重放历史 Stream。每次启动生成新的 consumer 名称，只通过 `XREADGROUP ... >` 消费新消息，不扫描、不认领历史 pending。

## 线程与调度模型

Redis 客户端运行在一个守护后台线程中。该线程只负责：

1. 创建或确认消费组；
2. 阻塞读取新消息；
3. 解析基本 JSON；
4. 将消息放入线程安全队列；
5. 接收主线程的 ACK 请求并执行 `XACK`。

后台线程不得调用 `ContextInfo`、`passorder`、交易查询或撤单函数。

`init(ContextInfo)` 完成以下工作：

1. 校验配置和交易开关；
2. 通过 `ContextInfo.set_account()` 绑定普通股票账户并订阅交易回报；
3. 初始化内存队列、运行期去重集合和当前执行状态；
4. 启动 Redis 守护线程；
5. 通过 `ContextInfo.run_time()` 注册 500ms 周期的主循环。

主循环在大 QMT 策略线程中运行，负责取出消息、查询行情与账户、下单、检查超时、撤单和推进状态机。任意时刻最多有一条交易信号处于活动状态，从而保持全账户 FIFO。

`handlebar(ContextInfo)` 保持为空，避免同一逻辑同时被行情回调和定时器驱动。

## 定价与下单前校验

每次提交或重试都重新调用 `ContextInfo.get_full_tick([code])` 获取：

- `lastPrice`
- `askPrice[0]`
- `bidPrice[0]`

支持两种定价方式：

- `book`：买入以卖一价加配置的 tick 偏移，卖出以买一价减配置的 tick 偏移；对应盘口为空时回退到滑点模式。
- `slippage`：买入按最新价上浮，卖出按最新价下浮。

ETF/基金代码使用 `0.001` 元 tick，普通股票使用 `0.01` 元 tick。最终价格按交易方向保守取整，并与聚宽 `reference_price` 比较；超过配置的最大偏差时拒绝该信号，不下单。

提交前调用 `get_trade_detail_data()`：

- 买入查询 `ACCOUNT` 的 `m_dAvailable`，按本次限价计算最大可买数量并向下取整到 100 股。
- 卖出查询 `POSITION` 的 `m_nCanUseVolume`，限制在当前可用持仓以内。

资金、持仓或行情查询失败时不使用默认值，也不提交订单。数量小于等于零时将信号置为失败终态并 ACK。

## 运行期状态机

每个活动信号在内存中保存：

- Redis message ID 和 `signal_id`
- 原始请求数量和累计成交数量
- 当前尝试次数
- 当前委托备注、委托号、委托状态和已成交量
- 提交时间、撤单请求时间和总执行起点
- 是否已经请求撤单

状态流转如下：

```text
QUEUED
  -> VALIDATING
  -> SUBMITTING
  -> WAITING_ORDER_ID
  -> WORKING
  -> CANCEL_REQUESTED
  -> RETRYING
  -> FILLED / PARTIAL_FINAL / FAILED / HALTED
```

下单使用：

```python
passorder(
    op_type,
    1101,
    account_id,
    qmt_code,
    11,
    limit_price,
    quantity,
    strategy_name,
    1,
    user_order_id,
    ContextInfo,
)
```

`user_order_id` 由当前 `signal_id` 的稳定短摘要和尝试次数组成，长度保持在大 QMT 投资备注可接受范围内。`passorder` 无返回值，因此调用成功后不能视为委托成功；状态必须等待委托回调或交易明细查询确认。

## 委托和成交回调

`order_callback(ContextInfo, orderInfo)` 只处理账号和策略备注匹配本执行端的委托。它从回调对象读取：

- `m_strRemark`
- `m_strOrderSysID`
- `m_nOrderStatus`
- `m_nVolumeTotalOriginal`
- `m_nVolumeTraded`
- `m_nVolumeTotal`
- `m_dTradedPrice`
- `m_strCancelInfo`

委托状态映射：

- 在途：`0`、`48`、`49`、`50`、`51`、`52`、`55`、`86`、`255`
- 已撤终态：`53`、`54`
- 全成终态：`56`
- 废单终态：`57`

未知状态按在途处理，不允许直接重报。

`deal_callback(ContextInfo, dealInfo)` 按 `m_strRemark` 和 `m_strOrderSysID` 关联活动委托。成交回调用于加速状态更新，但累计成交数量以委托对象的 `m_nVolumeTraded` 为主要依据，避免重复成交回调造成双重累计。

主循环也会按当前 `user_order_id` 扫描本策略当日 `ORDER` 记录，用于弥补回调延迟或丢失。该扫描只服务当前运行中的活动信号，不用于重启恢复。

## 超时撤单与重试

委托进入在途状态后，超过单次 `order_timeout_sec` 时：

1. 先刷新委托状态和累计成交量；
2. 若已全成则直接完成；
3. 调用 `can_cancel_order()`；
4. 可撤时调用 `cancel()` 并进入 `CANCEL_REQUESTED`；
5. 等待委托明确变成已撤、全成或废单。

发送撤单请求不等于撤单完成。撤单等待期间新到达的成交必须合并到累计成交数量。只有确认委托终态后，才能按剩余数量重新查询行情、资金或持仓并发起下一次尝试。

达到 `max_attempts` 或 `max_total_duration_sec` 后不再重试，将已成交数量记录为最终结果。若撤单请求失败、撤单等待超时或委托状态始终不确定，执行端进入运行期熔断：当前信号不 ACK，后续信号不再下单，日志明确要求人工检查。

集合竞价期间不实现额外跨重启保护，但沿用现有思路：9:15 至 9:30 收到的信号将单次和总执行截止时间延长至 9:30 之后，避免开盘前挂单被普通超时配置提前撤销。

## ACK、去重与重启语义

运行期间维护 `seen_signal_ids`：

- 同一 `signal_id` 再次出现时不重复下单，直接 ACK 重复消息。
- watchlist 消息处理完成后立即 ACK。
- 交易信号只在全成、部分最终、明确失败或明确拒绝后 ACK。
- 熔断状态下的活动消息不 ACK。

停止或崩溃后，所有内存状态丢失。新进程不读取历史 pending，也不恢复未完成委托；未 ACK 的旧消息允许遗失。`seen_signal_ids` 同样清空，因此跨重启不保证业务级幂等。这是本备用方案明确接受的边界。

## 日志与错误处理

大 QMT 策略环境使用其现有标准输出机制记录中文日志。日志至少覆盖：

- Redis 连接、消费组和 consumer
- 信号接收和 FIFO 排队数量
- 行情、资金、持仓校验结果
- 每次委托的代码、方向、数量、价格和尝试次数
- 委托号、状态、成交数量、撤单与重试
- ACK、失败和熔断原因

日志不得输出 Redis 密码、账号完整凭据或其他秘密。异常不会被静默吞掉：Redis 线程连接失败时记录错误并按固定间隔重连；交易相关异常使当前信号失败或触发熔断，具体取决于是否存在未确认在途委托。

## 安全约束

- `CONFIG["trading_enabled"]` 默认是 `False`。
- 账号和 Redis 地址为空时拒绝启动消费线程。
- 只有 `mode=live` 且 `action` 为 `buy` 或 `sell` 的合法信号可以进入执行队列。
- 不能同时运行现有 miniQMT 执行端和新的大 QMT 执行端。两者使用不同消费组时，同一信号会分别执行一次。
- 第一次启用前必须使用大 QMT 仿真账户完成验收，确认券商版本的回调字段、委托状态值和撤单行为。

## 测试与验收

macOS 单元测试通过注入 fake Redis、fake `ContextInfo` 和 fake 大 QMT 全局交易函数验证：

1. 现有 payload 和旧字段兼容解析；
2. 聚宽到大 QMT 代码转换；
3. watchlist 只更新股票池并 ACK；
4. 同一运行期的 `signal_id` 防重；
5. FIFO 串行执行；
6. 盘口与滑点定价、ETF tick 和偏差保护；
7. 买入资金缩量和卖出持仓缩量；
8. `passorder` 参数正确；
9. 委托回调状态映射；
10. 部分成交后撤单，只重报剩余数量；
11. 撤单等待期间新增成交正确合并；
12. 未确认撤单触发熔断；
13. 最终状态后才 ACK；
14. 重启不恢复 pending 的边界通过消费参数验证。

完成本地单元测试和编译检查后，还必须在 Windows 大 QMT 仿真账户执行以下实机检查：

1. 运行策略后能连接 Redis 并收到新信号；
2. `quickTrade=1` 在非历史 bar 上立即形成委托；
3. `m_strRemark`、委托号和状态字段能被正确读取；
4. 部分成交、撤单和剩余量重报符合预期；
5. 信号终态后 Redis pending 数量下降；
6. 停止 miniQMT 后再启用大 QMT，不发生双执行。

本地测试通过只能证明纯逻辑和参数组装正确，不能替代目标券商大 QMT 客户端的仿真验收。

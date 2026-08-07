# 大 QMT Redis 信号执行端

这是现有“聚宽 → Redis Stream → miniQMT”链路的备用大 QMT 执行端。聚宽发送函数、Redis Stream 名称和 payload 格式不需要修改。

大 QMT 实际只需要导入一个文件：

```text
bigqmt_redis_follower.py
```

该版本不使用 SQLite。买入、卖出各自的 FIFO 队列、`signal_id` 去重、委托状态和成交数量都只保存在本次大 QMT 运行的内存里；停止或重启后不恢复历史 pending 和未完成状态。

## 1. 安全前提

1. 先停止现有 miniQMT 执行端。
2. 第一次运行必须使用普通股票仿真账户。
3. 确认大 QMT 使用实时运行方式，并能收到交易回报回调。大 QMT 文档所说的“模拟运行模式”不执行交易函数；应以券商提供的仿真资金账号在实时策略运行环境测试。
4. 不要让 miniQMT 与大 QMT 执行端同时运行。两端使用不同消费组时，同一条信号会各执行一次。

## 2. 安装 Redis 客户端

代码只额外依赖 `redis-py`。需要用大 QMT 实际加载策略的 Python 解释器安装，而不是随便选择系统里的 Python：

```powershell
<大QMT实际Python路径> -m pip install redis
```

若券商客户端禁止给内置 Python 安装第三方包，应先向券商确认可用的第三方库目录；不要把其他 Python 环境的包直接复制进去。

## 3. 修改顶部配置

复制 `bigqmt_redis_follower.py` 到 Windows，再编辑文件顶部的 `CONFIG`：

```python
CONFIG = {
    "trading_enabled": False,
    "account_id": "",
    "account_type": "stock",
    "strategy_name": "bigqmt_redis_follower",
    "redis_host": "",
    "redis_port": 6379,
    "redis_password": "",
    "redis_stream": "tidal_quant_signals",
    "redis_group": "bigqmt_executors",
    # 策略白名单: 非空时只执行名单内策略的信号/日计划, 其余直接 ACK。
    "allowed_strategy_ids": [],
    # 意图型信号单票买入上限 = 总资产 × 该比例。
    "max_single_position_pct": 0.2,
    # sell_half 半仓不足一手(<200股): sell_all=全卖(默认) / skip=跳过不卖。
    "sell_half_insufficient_lot_mode": "sell_all",
    # 其余定价、超时和重试参数见源码顶部。
}
```

配置顺序：

1. 填普通股票资金账号 `account_id`。
2. 填 Redis 地址、端口和密码。
3. 确认 `redis_stream` 与聚宽端完全一致。
4. 保持 `account_type="stock"`；本版本不支持信用账户。
5. 仿真验收前最后一步才把 `trading_enabled` 改为 `True`。

仓库版本的账号、Redis 地址和密码均为空，交易开关默认关闭。

## 4. 导入和运行

1. 在大 QMT 新建 Python 策略。
2. 把 `bigqmt_redis_follower.py` 的完整内容作为策略代码导入。
3. 选择实时运行，并绑定与 `CONFIG["account_id"]` 相同的普通股票仿真账户。
4. 启动后检查日志是否出现：

```text
大QMT Redis执行端已启动
Redis监听已启动 stream=... group=... consumer=...
```

策略使用 `ContextInfo.run_time()` 每 500ms 推进订单。Redis 后台线程只收消息和执行 ACK，不调用 `ContextInfo`、`passorder` 或撤单接口。

## 5. 执行语义

- 只执行 `mode=live` 的消息；策略白名单 `allowed_strategy_ids` 非空时，名单外的信号/日计划/预订阅直接 ACK。
- 支持精确信号 `action=buy/sell`（带 `amount`）与意图型信号：`sell_half` / `sell_all`（数量按真实可卖持仓计算）、不带 `amount` 的 `buy`（按 `min(可用资金÷待买只数, 总资产×max_single_position_pct)` 自动买入）。
- `sell_half` 半仓取整不足一手（<200股）时按 `sell_half_insufficient_lot_mode` 处理：`sell_all`=全卖当前持仓（默认）/ `skip`=跳过不卖。
- 支持日计划 `action=plan`：展开为清仓 `sell_all` + 待买 `auto_buy` 派生信号（已持仓代码不再补买），全部派生信号终态后才 ACK plan；派生 `signal_id` 与 miniQMT 同规则，重放幂等。
- 普通股票买入使用 `passorder` 操作类型 `23`，卖出使用 `24`。
- 使用 `1101` 按股数下单、报价类型 `11` 指定限价、`quickTrade=1` 立即触发。
- `action=subscribe` 只更新大 QMT 股票池，不下单。
- 同一运行期内按 `signal_id` 去重；买入和卖出各自保持 FIFO，两个方向可以同时活动。09:25–09:30 创建的盘前卖单会挡住买单——买单须等盘前卖单全部终态后才提交（卖出回款计入可用资金，与 miniQMT 版开盘屏障一致）；跌停排队卖单确认挂单后立即放行买单（与 miniQMT 一致）。
- 每次提交和重试前重新查询行情、资金或可用持仓。
- 首次按真实资源缩量后冻结执行目标，之后只补足剩余数量，不因资金/持仓变化追买。
- 部分成交超时后先确认撤单终态，再重报剩余数量。
- 盘前卖单首笔在开盘后给 0.5 秒回报宽限，再进入撤单流程（与 miniQMT 一致）。
- 涨停排队买单被废单（非硬拒单）时刷新行情与资金后重新排队，最多到 `max_attempts`（与 miniQMT 一致）；硬拒单立即终止。
- 撤单状态无法确认时熔断；熔断后的活动单与后续信号都以失败终态记录并 ACK，恢复时凭 QMT 委托与日志人工对账。
- 交易信号达到明确终态后才 `XACK`。
- 带 `expire_at` 的交易信号在到达对应方向 FIFO 队首时校验；过期或格式非法时不调用交易接口，记录终态并 ACK。没有该字段的旧信号继续执行。
- 不再支持预约执行；历史消息中的 `execute_at` 会被忽略，未过期信号到达队首后立即提交。

09:25–09:30 收到的卖出信号会立即调用 `passorder`。该时段交易所不接收买卖申报，委托能否由大 QMT/券商柜台接收并暂存到 09:30，必须在目标券商仿真环境确认；买入信号在盘前窗口内排队等待，09:30 后才提交。

与 miniQMT 版一致的风控：
- 沪深 A 股委托价夹进动态价格笼子（基准 ±2% 或 ±10tick 取较宽，再夹涨跌停），ETF/基金不套用；
- 跌停无买盘的卖单 / 涨停确认锁盘的买单按 `limit_down_sell_mode` / `limit_up_buy_mode` 处理：`queue`=挂跌停/涨停价排队至截止（默认 14:56:30），不撤不重挂；`skip`=直接跳过；`none`=普通定价；
- 拒单按原因分类：停牌、权限不足、账户异常等永久性原因立即终止；价格/资源/瞬时原因刷新行情与资源后重试。

消费组第一次创建时从 `$` 开始，不重放 Stream 里的历史消息。每次启动使用新的 consumer，只读取消费组尚未投递的新消息，不认领旧 consumer 的 pending。

## 6. 仿真验收

按顺序验证：

1. 发送一条 100 股买入信号，确认收到 Redis 消息。
2. 核对委托代码、方向、数量、价格和投资备注 `BQR-...-01`。
3. 确认 `order_callback` 能读取 `m_strOrderSysID`、`m_nOrderStatus` 和 `m_nVolumeTraded`。
4. 人为使用不易成交的价格测试超时撤单。
5. 测试部分成交，确认第二次委托只包含剩余数量。
6. 检查信号终态后 Redis pending 数量下降。
7. 在 09:25–09:30 发送普通股票或 ETF 小额信号，确认立即调用 `passorder`，并从委托回报/状态判断柜台是暂存、报出还是拒绝；仅有本地返回值不能证明已进入交易所队列。
8. 停止并重启大 QMT，确认旧 pending 不会被新进程恢复。

目标券商的大 QMT 版本可能在回调字段或运行模式上存在差异。macOS 单元测试只能验证状态机和调用参数，不能替代 Windows 仿真账户验收。

大 QMT Python API 参考：<https://miniqmt.com/qmtapi/QMT_Python_API_Doc.html>

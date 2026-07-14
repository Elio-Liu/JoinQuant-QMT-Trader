# 大 QMT Redis 信号执行端

这是现有“聚宽 → Redis Stream → miniQMT”链路的备用大 QMT 执行端。聚宽发送函数、Redis Stream 名称和 payload 格式不需要修改。

大 QMT 实际只需要导入一个文件：

```text
bigqmt_redis_follower.py
```

该版本不使用 SQLite。FIFO 队列、`signal_id` 去重、委托状态和成交数量都只保存在本次大 QMT 运行的内存里；停止或重启后不恢复历史 pending 和未完成状态。

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

- 只执行 `mode=live` 且 `action=buy/sell` 的交易信号。
- 普通股票买入使用 `passorder` 操作类型 `23`，卖出使用 `24`。
- 使用 `1101` 按股数下单、报价类型 `11` 指定限价、`quickTrade=1` 立即触发。
- `action=subscribe` 只更新大 QMT 股票池，不下单。
- 同一运行期内按 `signal_id` 去重，并保持全账户 FIFO。
- 每次提交和重试前重新查询行情、资金或可用持仓。
- 部分成交超时后先确认撤单终态，再重报剩余数量。
- 撤单状态无法确认时熔断，不继续提交后续订单。
- 交易信号达到明确终态后才 `XACK`。
- 带 `expire_at` 的交易信号在到达 FIFO 队首时校验；过期或格式非法时不调用交易接口，记录终态并 ACK。没有该字段的旧信号继续执行。

消费组第一次创建时从 `$` 开始，不重放 Stream 里的历史消息。每次启动使用新的 consumer，只读取消费组尚未投递的新消息，不认领旧 consumer 的 pending。

## 6. 仿真验收

按顺序验证：

1. 发送一条 100 股买入信号，确认收到 Redis 消息。
2. 核对委托代码、方向、数量、价格和投资备注 `BQR-...-01`。
3. 确认 `order_callback` 能读取 `m_strOrderSysID`、`m_nOrderStatus` 和 `m_nVolumeTraded`。
4. 人为使用不易成交的价格测试超时撤单。
5. 测试部分成交，确认第二次委托只包含剩余数量。
6. 检查信号终态后 Redis pending 数量下降。
7. 停止并重启大 QMT，确认旧 pending 不会被新进程恢复。

目标券商的大 QMT 版本可能在回调字段或运行模式上存在差异。macOS 单元测试只能验证状态机和调用参数，不能替代 Windows 仿真账户验收。

大 QMT Python API 参考：<https://miniqmt.com/qmtapi/QMT_Python_API_Doc.html>

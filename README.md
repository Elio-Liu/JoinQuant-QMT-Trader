# JoinQuant miniQMT 跟单助手

一个用于连接聚宽云策略与 Windows 本地 miniQMT 的实盘跟单框架。

聚宽侧策略只负责把交易信号写入 Redis Stream；Windows 端服务负责消费信号、读取 miniQMT 行情、按配置滑点重新定价，并通过 miniQMT/xtquant 提交订单。系统用 SQLite 执行账本保证同一条信号只处理一次，并在订单进入终态后再确认 Redis 消息。

> 重要说明：本项目包含真实交易链路代码，但 miniQMT/xtquant 交易适配必须在 Windows + 已登录 miniQMT 的环境中完成验证。默认配置 `trading.enabled=false`，请勿在未验证前直接切换到实盘。

## 核心能力

- **聚宽侧低侵入接入**：保留 `publish_trade_signal_to_redis(context, action, code, amount, price)` 五参数签名，已有策略调用点无需改动。
- **Redis Stream 可靠投递**：相比 Redis Pub/Sub，Stream 可保留未消费消息，并支持消费组与 ACK。
- **SQLite 幂等账本**：`signal_id` 是唯一键，重复投递不会触发第二次下单。
- **执行端重新定价**：聚宽传入的是 `reference_price`，Windows 端会用 miniQMT 最新行情和滑点配置计算实际委托价。
- **超时撤单重挂**：单次委托超时后撤单，对剩余数量重新取价、重新挂单，直到达到次数或总时长上限。
- **适配器隔离**：核心状态机只依赖 `MarketDataAdapter` / `BrokerAdapter` 协议，便于用 fake adapter 做本地测试。

## 架构概览

```text
JoinQuant 云策略
  └─ publish_trade_signal_to_redis(...)
       └─ Redis Stream XADD
            └─ Windows 跟单服务 main.py
                 ├─ RedisStreamClient.read_forever()
                 ├─ OrderExecutionEngine.execute()
                 │    ├─ SQLiteExecutionStore 幂等检查
                 │    ├─ QmtMarketDataAdapter 获取最新价
                 │    ├─ pricing 计算滑点与偏离保护
                 │    └─ QmtBrokerAdapter 下单 / 查单 / 撤单
                 └─ 订单终态后 XACK
```

执行端遵循一个关键原则：**Redis ACK 只发生在本地执行结果已经写入 SQLite 之后**。如果进程在处理中崩溃，Redis 消息仍会留在 pending 状态；重启后即使消息被重新处理，SQLite 的 `signal_id` 主键也会阻止重复下单。

## 目录结构

```text
.
├── joinquant_signal_sender.py              # 聚宽侧信号发送函数，可复制到策略内
├── main.py                                 # Windows 端快捷入口
├── config.example.json                     # 配置模板，含 JSON-safe 中文说明
├── qmt_follower/
│   ├── app.py                              # 配置组装、主循环、线程池执行
│   ├── executor.py                         # 下单状态机
│   ├── redis_stream.py                     # Redis Stream 消费组封装
│   ├── store.py                            # SQLite 执行账本
│   ├── pricing.py                          # 滑点和价格偏离保护
│   ├── models.py                           # 数据模型与状态枚举
│   ├── config.py                           # 配置加载
│   ├── logging_config.py                   # 控制台与文件日志
│   └── adapters/qmt.py                     # miniQMT 行情与交易适配
├── strategies/
│   └── etf_discount_live_signal_strategy.py # 参考聚宽策略
├── tests/                                  # 标准库 unittest 测试
└── docs/
    └── windows-qmt-adapter-handoff.md      # Windows/QMT 对接与验证说明
```

## 快速开始

### 1. 准备 Windows 端环境

要求：

- Windows 10/11 x64
- Python 3.8+
- Redis 可访问
- miniQMT 已安装、已登录，且可手工下单 / 撤单 / 查委托
- Python 环境可导入 `xtquant`

安装依赖：

```bash
pip install redis xtquant
```

复制配置：

```bash
copy config.example.json config.json
```

编辑 `config.json`，至少确认：

| 配置项 | 说明 |
| --- | --- |
| `redis.host` / `redis.port` / `redis.password` | Redis 连接信息 |
| `redis.stream` | 必须与聚宽侧函数里的 stream 一致 |
| `redis.group` / `redis.consumer` | Windows 执行端消费组和消费者名称 |
| `execution.buy_slippage_pct` | 买入按最新价上浮的比例 |
| `execution.sell_slippage_pct` | 卖出按最新价下浮的比例 |
| `execution.order_timeout_sec` | 单次委托等待多久后撤单重挂 |
| `execution.max_attempts` | 单条信号最多下单尝试次数 |
| `execution.max_total_duration_sec` | 单条信号最大执行总时长 |
| `execution.max_deviation_from_signal_price_pct` | 最新价相对聚宽参考价的最大允许偏离 |
| `trading.enabled` | 实盘开关，默认 `false` |
| `trading.account_id` | miniQMT 资金账号 |
| `trading.miniqmt_path` | miniQMT 的 `userdata_mini` 目录 |

启动服务：

```bash
python main.py
```

指定配置文件或线程数：

```bash
python main.py --config config.prod.json --workers 8
```

### 2. 接入聚宽策略

把 `joinquant_signal_sender.py` 中的 `publish_trade_signal_to_redis(...)` 复制到聚宽策略文件里，然后修改函数内部的 Redis 配置和 `strategy_id`：

```python
redis_config = {
    "host": "你的Redis服务器IP",
    "port": 6379,
    "password": "你的Redis密码",
    "stream": "tidal_quant_signals",
    "maxlen": 10000,
    "socket_connect_timeout": 1,
}
strategy_id = "你的策略ID"
```

在策略需要发出交易信号的位置调用：

```python
publish_trade_signal_to_redis(context, "buy", "000001.XSHE", 1000, 10.0)
publish_trade_signal_to_redis(context, "sell", "600519.XSHG", 500, 1800.0)
```

函数行为：

| 场景 | 行为 |
| --- | --- |
| 实时运行 | 写入 Redis Stream，返回 `{"sent": True, ...}` |
| 回测 / 研究 / 补跑 | 跳过发送，返回 `{"sent": False, "mode": "backtest", ...}` |
| Redis 异常 | 记录错误并返回失败结果，不阻塞策略主流程 |

## 信号格式

Redis Stream 中的消息字段为 `payload`，内容是 JSON 字符串：

```json
{
  "signal_id": "hunter-20260608093001-000001XSHE-buy-1000",
  "strategy_id": "hunter",
  "mode": "live",
  "action": "buy",
  "code": "000001.XSHE",
  "amount": 1000,
  "reference_price": 10.0,
  "created_at": "2026-06-08 09:30:01",
  "expire_at": "2026-06-08 09:30:21",
  "nonce": "a1b2c3d4"
}
```

字段说明：

| 字段 | 说明 |
| --- | --- |
| `signal_id` | 幂等键；同一个 `signal_id` 在 Windows 端只会执行一次 |
| `strategy_id` | 策略来源标识 |
| `mode` | `live` 或 `backtest` |
| `action` | `buy` 或 `sell` |
| `code` | 聚宽证券代码，例如 `000001.XSHE` |
| `amount` | 委托数量 |
| `reference_price` | 策略参考价，不是最终委托价 |
| `created_at` | 聚宽策略时间 |
| `expire_at` | 信号建议过期时间，用于执行端或监控端判断 |
| `nonce` | 审计辅助字段，不参与幂等 |

默认 `signal_id` 由 `strategy_id + 时间 + 代码 + 方向 + 数量` 组成。如果同一秒内对同一证券、同一方向、同一数量连续发出两笔独立订单，第二笔会被视为重复信号；如确有这种需求，应在发送函数内部增加序号后缀。

## 执行流程

```text
收到 Redis Stream 消息
  -> 解析 TradeSignal
  -> SQLite INSERT OR IGNORE 做幂等检查
  -> 获取 miniQMT 最新行情
  -> 检查最新价是否偏离 reference_price 过多
  -> 按买入 / 卖出滑点计算限价
  -> 提交委托
  -> 轮询订单状态
       -> 全部成交：记录终态并 ACK
       -> 部分成交：撤单，对剩余数量重新定价再下
       -> 超时未成交：撤单，重新定价再下
       -> 达到次数或总时长上限：记录终态并 ACK
```

## 本地验证

测试使用标准库 `unittest`，并通过 fake Redis / fake broker / fake market data 隔离外部依赖。运行测试不需要真实 Redis、miniQMT 或券商账号。

```bash
python -m unittest discover -v
python -m compileall qmt_follower tests
```

也可以只跑单个模块：

```bash
python -m unittest tests.test_executor -v
python -m unittest tests.test_qmt_adapter_mapping -v
```

## miniQMT 适配状态

`qmt_follower/adapters/qmt.py` 已包含：

- 聚宽代码到 QMT 代码的转换：`000001.XSHE -> 000001.SZ`，`510300.XSHG -> 510300.SH`
- `xtdata.get_full_tick()` 最新价读取
- `XtQuantTrader` 连接、订阅、下单、查单、撤单的基础实现
- QMT 订单状态到内部 `BrokerOrderStatus` 的映射
- 交易 API 调用锁和短缓存，减少并发轮询下的重复 QMT 调用

仍需在真实 Windows 环境中逐项验证：

- 当前券商 miniQMT 版本的 `xtquant` API 签名和返回值
- 账号类型、资金账号格式、`userdata_mini` 路径
- 下单、撤单、查单字段是否与适配器假设一致
- 全成、部成、已撤、废单等状态映射是否准确
- 模拟盘长时间运行稳定性和异常恢复路径

详细交接清单见 `docs/windows-qmt-adapter-handoff.md`。

## 安全建议

- 不要将 Redis 6379 端口直接暴露到公网；优先使用 VPN、内网、白名单或安全组限制来源 IP。
- 实盘前保持 `trading.enabled=false`，先确认 Redis 收发、SQLite 记录、日志和 QMT 查询链路。
- 首次打开实盘开关时，使用小额、低风险标的，并人工盯盘验证每个状态。
- 保留 `logs/` 和 `data/qmt_follower.db`，它们是排查重复信号、部分成交和撤单重挂的关键证据。

## 免责声明

本项目仅用于技术研究和个人自动化实验，不构成投资建议。实盘交易有风险，使用前请确认符合券商、交易所及相关法律法规要求。任何交易损失由使用者自行承担。

## 打赏赞助

如果这个项目帮你节省了盯盘和手工操作时间，欢迎请作者喝杯咖啡。

<div align="center">

<table>
<tr>
  <td align="center"><img src="images/微信.png" alt="微信打赏" width="220"/><br/>微信</td>
  <td align="center"><img src="images/支付宝.png" alt="支付宝打赏" width="220"/><br/>支付宝</td>
</tr>
</table>

</div>

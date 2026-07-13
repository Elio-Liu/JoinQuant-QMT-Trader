# JoinQuant QMT 跟单助手

把聚宽云策略产生的交易信号，通过 Redis Stream 可靠地送到 Windows QMT 执行端。

聚宽侧只负责表达“买什么、卖什么、数量和参考价”；执行端负责读取实时行情、核对资金或持仓、计算委托价、下单、确认撤单与成交，并在订单进入明确终态后确认 Redis 消息。核心目标是：**尽量不改策略调用方式，同时让真实交易过程可追踪、可去重、可停止。**

> [!WARNING]
> 本项目包含真实交易链路。仓库默认关闭交易开关，macOS/Linux 单元测试也不能替代目标券商 Windows 客户端的仿真验证。首次启用必须使用仿真账户、小额订单，并由人工盯盘。

## 选择执行方式

仓库提供两条 Windows 执行路径，共用聚宽发送函数、Redis Stream 和交易信号格式，但运行状态与部署方式不同。

| 执行方式 | 适用场景 | 状态与可靠性 | 入口 |
| --- | --- | --- | --- |
| **独立 miniQMT 服务（推荐）** | 可以在 Windows 单独运行 Python 服务 | SQLite 持久账本、`signal_id` 幂等、全账户 FIFO、完整订单尝试记录 | `python main.py` |
| **大 QMT 单文件执行器（备用）** | 客户端要求策略只能放在一个 Python 文件中 | FIFO、去重和订单状态仅保存在本次运行的内存中，重启不恢复 | `bigqmt_follower/bigqmt_redis_follower.py` |

两个执行端不能同时连接同一批真实信号。尤其不要因为消费组不同就同时启动：Redis 会把同一条消息分别交给每个消费组，可能造成重复下单。

## 系统架构

```text
聚宽云策略
  ├─ publish_watchlist_to_redis(...)       盘前推送股票池，仅预订阅行情
  └─ publish_trade_signal_to_redis(...)    发送买卖信号
                  │
                  ▼
          Redis Stream（XADD）
                  │
        ┌─────────┴─────────┐
        ▼                   ▼
独立 miniQMT 服务       大 QMT 单文件执行器
Redis 消费组            Redis 消费组
单工作线程 FIFO         内存 FIFO
SQLite 幂等账本         运行期 signal_id 去重
xtquant 下单            passorder 下单
        │                   │
        └─────────┬─────────┘
                  ▼
          Windows QMT / 券商柜台
```

独立 miniQMT 服务的主链路：

```text
RedisStreamClient.read_forever()
  → 预订阅指令：立即订阅行情并 ACK，不进入交易队列
  → 交易信号：进入单工作线程 FIFO
  → SQLite INSERT OR IGNORE：signal_id 幂等门
  → 获取最新价、买一/卖一、可用资金或可卖持仓
  → 计算盘口价/滑点价并检查参考价偏离
  → 提交委托并轮询订单状态
  → 超时：申请撤单，等待真实终态，核对撤单期间成交
  → 仍有剩余：刷新行情和账户资源后重挂
  → 写入最终执行状态
  → XACK
```

## 核心保障

- **低侵入接入**：保留 `publish_trade_signal_to_redis(context, action, code, amount, price)` 五参数签名，策略原有调用点无需改造。
- **可靠传输**：使用 Redis Stream 和消费组；Windows 暂时离线时，新消息仍保留在 Stream 中。
- **持久幂等**：独立 miniQMT 服务以 `signal_id` 为 SQLite 主键，重复投递不会再次触达券商。
- **全账户 FIFO**：收信和行情预订阅保持响应，真实交易只由一个工作线程按到达顺序执行。
- **每次尝试都刷新状态**：重新读取行情、买入可用资金或卖出可用持仓，不使用旧资源快照继续下单。
- **资源查询失败即停止**：无法确认资金或持仓时不提交委托，不通过默认值继续交易。
- **撤单必须确认**：撤单请求成功不等于订单已撤；只有查询到已成、已撤或废单后，才处理剩余数量。
- **不确定状态熔断**：撤单终态长期无法确认时，当前进程锁停后续交易，避免新旧订单同时成交。
- **ETF tick 精度**：沪市 `5xxxxx`、深市 `1xxxxx` 基金按 `0.001` 报价，其余股票按 `0.01` 报价。
- **开盘集合竞价保护**：9:15–9:30 收到的信号会把委托等待预算延长到开盘后，避免 9:30 前被普通超时逻辑撤掉。
- **预约执行**：独立 miniQMT 服务支持可选 `execute_at` 字段；未到时间的消息不提前下单，也不提前 ACK。
- **可观测性**：控制台保留简洁中文交易进度，文件日志记录 DEBUG 细节；`sent_at_ms` 可用于估算传输和端到端延迟。

> Redis pending 消息当前不会由新进程自动 `XCLAIM`。进程在执行中崩溃时，消息不会被错误 ACK，但恢复前需要人工核对 QMT 委托、SQLite 账本和 Redis pending，不能直接假设重启后会自动续跑。

## 目录结构

```text
.
├── main.py                              # 独立 miniQMT 服务入口
├── joinquant_signal_sender.py           # 可复制到聚宽策略的发送函数
├── config.example.yaml                  # 独立服务 YAML 配置模板
├── qmt_follower/
│   ├── app.py                           # 依赖组装、收信、预约调度和全局 FIFO
│   ├── executor.py                      # 下单、查单、撤单确认、重试状态机
│   ├── redis_stream.py                  # Redis Stream 消费组与消息解析
│   ├── store.py                         # SQLite 信号和委托尝试账本
│   ├── pricing.py                       # 盘口/滑点定价、tick 和偏离保护
│   ├── models.py                        # 信号、行情、订单与执行状态模型
│   ├── config.py                        # UTF-8 YAML 配置加载
│   ├── logging_config.py                # 控制台与按天轮转文件日志
│   └── adapters/qmt.py                  # miniQMT 行情和交易适配器
├── scripts/
│   ├── redis_target_config.py           # 手动脚本的本机 Redis 目标加载器
│   ├── send_manual_signal.py            # 单笔人工确认发送
│   └── send_batch_signals.py            # 模拟盘 FIFO 压力信号
├── bigqmt_follower/
│   ├── bigqmt_redis_follower.py         # 可直接导入大 QMT 的单文件执行器
│   └── README.md                        # 大 QMT 部署与仿真验收说明
├── tests/                               # unittest 测试
└── docs/
    ├── windows-qmt-adapter-handoff.md   # Windows miniQMT 上线验证清单
    └── joinquant-community-promo.md     # 项目介绍与社区发布稿
```

`config.yaml`、`scripts/redis_targets.yaml`、`strategies/`、日志和运行数据库都属于本机部署内容，默认不进入版本控制。`strategies/` 中可能保存带环境配置的聚宽部署副本，不应强制提交。

## 快速开始：独立 miniQMT 服务

### 1. 准备环境

- Windows 10/11 x64
- 已安装并登录的 miniQMT，目标账号可以手工下单、撤单、查委托
- 能导入 `xtquant` 的 Python 环境
- 可从 Windows 和聚宽访问的 Redis
- Python 3.8+（以目标 miniQMT/xtquant 版本实际支持范围为准）

在准备运行服务的 Python 环境中安装公共依赖：

```powershell
python -m pip install redis PyYAML
```

`xtquant` 通常随 miniQMT 环境提供。先验证当前解释器，不要盲目从其他 Python 环境复制包：

```powershell
python -c "import xtquant; print('xtquant ok')"
python -c "from xtquant import xtdata; print('xtdata ok')"
python -c "from xtquant.xttrader import XtQuantTrader; print('xttrader ok')"
```

### 2. 创建本机配置

```powershell
Copy-Item config.example.yaml config.yaml
$env:REDIS_PASSWORD="你的 Redis 密码"
```

编辑 `config.yaml`，至少核对：

| 配置项 | 作用 |
| --- | --- |
| `redis.host` / `port` / `password` | Redis 连接；密码可写成 `${REDIS_PASSWORD}` |
| `redis.stream` | 必须与聚宽发送函数中的 Stream 完全一致 |
| `redis.group` / `consumer` | 执行端消费组和实例名称 |
| `execution.pricing_mode` | `slippage` 或 `book` |
| `execution.max_deviation_from_signal_price_pct` | 行情定价基准相对策略参考价的偏离上限 |
| `execution.order_timeout_sec` / `max_attempts` | 单次等待和最多委托次数 |
| `execution.max_total_duration_sec` | 一条信号的总执行预算 |
| `market_data.pre_subscribe_codes` | 启动时预订阅的聚宽格式代码 |
| `trading.account_id` / `miniqmt_path` | 资金账号和 `userdata_mini` 路径 |
| `trading.enabled` | 交易安全门，模板默认 `false` |
| `state_db` / `log_dir` | SQLite 账本和日志目录 |

首次运行先保持 `trading.enabled: false`，确认程序会被安全门拒绝。完成只读检查并切到仿真账户后，才改成 `true` 启动完整链路。

### 3. 启动服务

```powershell
python .\main.py --config config.yaml --workers 1
```

`--workers` 当前只允许 `1`，这是全账户 FIFO 的安全约束，不是可调并发参数。完整启动至少应看到：

```text
【系统】🚀 QMT跟单助手启动中 | 交易线程 1 | FIFO
【QMT】🔌 交易端已连接
【系统】🟢 Redis监听已启动
```

更完整的 Windows 环境核对、订单状态映射和模拟盘验收步骤见 [Windows miniQMT 部署验证清单](docs/windows-qmt-adapter-handoff.md)。

## 接入聚宽策略

将 `joinquant_signal_sender.py` 中以下函数复制进策略：

- `publish_trade_signal_to_redis(...)`
- `publish_watchlist_to_redis(...)`（如需盘前预订阅）
- `_cached_redis_client(...)`

把两个发送函数内部的 Redis 占位配置改成同一套真实配置，并保持 `stream` 与 Windows 端一致。不要把修改后的生产策略副本提交回仓库。

交易调用保持五参数：

```python
publish_trade_signal_to_redis(context, "buy", "510300.XSHG", 1000, 3.850)
publish_trade_signal_to_redis(context, "sell", "159915.XSHE", 500, 1.235)
```

选股完成后可以提前推送股票池，只订阅行情、不产生订单：

```python
publish_watchlist_to_redis(context, ["510300.XSHG", "159915.XSHE"])
```

发送函数会根据 `context.current_dt` 与当前时间判断运行模式。回测、研究和历史补跑不会写入 Redis；实时模式下 `XADD` 成功也只表示 Redis 已接收，**不表示 Windows 已下单或成交**。

## 手动发送与批量验证

两个脚本统一从被 Git 忽略的 `scripts/redis_targets.yaml` 读取连接信息。先在本机创建：

```yaml
targets:
  remote_prod:
    host: YOUR_REDIS_HOST
    port: 6379
    password: YOUR_REDIS_PASSWORD
    stream: tidal_quant_signals
```

单笔发送前，编辑 `scripts/send_manual_signal.py` 顶部的 `TARGET`、代码、方向、数量和参考价：

```powershell
python .\scripts\send_manual_signal.py
```

脚本只有在输入 `yes` 后才会写入 Redis。批量 FIFO 压力测试还会校验 100 股整手、卖出总量上限和动态确认口令：

```powershell
python .\scripts\send_batch_signals.py
```

这些脚本发送的是 `mode=live` 信号。只要同一 Stream 上存在已启用的真实执行端，就可能产生真实订单；运行前必须确认消费组、账号和参考价。

## 信号协议

Redis Stream 每条消息使用字段 `payload`，值为 JSON 字符串。交易信号示例：

```json
{
  "signal_id": "hunter-20260713093001-510300XSHG-buy-1000",
  "strategy_id": "hunter",
  "mode": "live",
  "action": "buy",
  "code": "510300.XSHG",
  "amount": 1000,
  "reference_price": 3.85,
  "created_at": "2026-07-13 09:30:01",
  "sent_at_ms": 1783906201000,
  "expire_at": "2026-07-13 09:30:21",
  "nonce": "a1b2c3d4"
}
```

| 字段 | 说明 |
| --- | --- |
| `signal_id` | 幂等键；默认由策略、时间、代码、方向和数量组成 |
| `strategy_id` | 信号来源标识 |
| `mode` | 只有 `live` 会进入真实发送/执行路径 |
| `action` | `buy` 或 `sell` |
| `code` | 聚宽代码，如 `510300.XSHG` |
| `amount` | 目标股数；执行端仍会按最新资金或持仓缩量 |
| `reference_price` | 策略意见价，仅用于偏离保护，不是最终委托价 |
| `created_at` | 策略侧时间；旧协议字段 `timestamp` 仍可解析 |
| `sent_at_ms` | 可选，发送时的毫秒时间戳，用于延迟日志 |
| `execute_at` | 可选，`YYYY-MM-DD HH:MM:SS`；仅独立 miniQMT 服务支持预约释放 |
| `expire_at` / `nonce` | 发送侧审计字段；当前独立执行端不据此过期或去重 |

默认 `signal_id` 在“同一秒、同一策略、同一代码、同一方向、同一数量”下会碰撞，这是有意的重复信号保护。确实需要在同一秒发送两笔独立订单时，发送侧必须生成不同 `signal_id`。

预订阅消息使用同一个 Stream：

```json
{
  "action": "subscribe",
  "codes": ["510300.XSHG", "159915.XSHE"],
  "strategy_id": "hunter",
  "mode": "live",
  "sent_at_ms": 1783905900000
}
```

## 定价与订单执行

`execution.pricing_mode` 支持：

- `slippage`：买入按最新成交价上浮 `buy_slippage_pct`，卖出按最新成交价下浮 `sell_slippage_pct`。
- `book`：买入按卖一价加 `book_tick_offset` 个 tick，卖出按买一价减相同 tick；对手盘缺失时回退到 `slippage`。

两种模式都会先比较定价基准与 `reference_price`。超过 `max_deviation_from_signal_price_pct` 时拒绝下单，而不是扩大滑点追价。

买单按当前委托价和可用资金计算最大数量，再向下取整到 100 股；卖单不超过实时可卖持仓。订单超时后不会直接视为完成，而是进入“申请撤单 → 等待终态 → 核对最终成交 → 只重挂剩余数量”的流程。

## 大 QMT 单文件备用执行器

如果券商大 QMT 只能导入一个 Python 文件，使用：

```text
bigqmt_follower/bigqmt_redis_follower.py
```

它保持上游信号和 Redis 契约不变，通过后台线程收消息，在大 QMT 调度线程中调用 `passorder`、查询订单和执行撤单。仓库版本的账号、Redis 地址和密码为空，`trading_enabled` 默认关闭。

它与独立服务的关键差异：

- 不使用 SQLite；重启后不恢复 `signal_id` 去重、FIFO 队列和未完成订单状态。
- 消费组首次从 `$` 创建，只接收创建后的新消息。
- 不自动认领旧 consumer 的 pending。
- 当前只支持普通股票账户，不支持信用账户。
- 必须在券商仿真资金账号的实时策略环境验证，平台“模拟运行模式”可能不会执行交易函数。

完整导入、配置和回调字段核对见 [大 QMT Redis 信号执行端说明](bigqmt_follower/README.md)。

## 本地验证

核心测试使用 `unittest` 和 fake Redis / broker / market data；不需要真实 Redis、QMT 或券商账号。

```bash
python -m unittest discover -v
python -m compileall qmt_follower scripts tests bigqmt_follower joinquant_signal_sender.py
```

只运行核心执行链路：

```bash
python -m unittest tests.test_executor tests.test_runtime tests.test_store -v
```

仓库忽略的本地 `strategies/` 部署副本可能有对应的专项测试；在没有这些本机文件的环境中，不应把该部分结果当作核心跟单服务的验证结论。

## 上线检查

- [ ] `config.yaml`、`scripts/redis_targets.yaml`、聚宽生产配置、日志和 SQLite 数据库未进入 Git。
- [ ] Redis 未直接暴露到公网，已使用内网、VPN、来源白名单或安全组。
- [ ] 聚宽发送函数与 Windows 执行端的 Stream 名称一致。
- [ ] 同一批信号只有一个执行端和一个目标账户。
- [ ] Windows、聚宽和 Redis 所在机器完成时间同步；否则延迟日志没有参考意义。
- [ ] 目标券商的行情字段、资金/持仓字段、订单状态与撤单返回值已核对。
- [ ] 仿真盘完成单笔买卖、拒单、部分成交、超时撤单和批量 FIFO 测试。
- [ ] 已确认 SQLite、QMT 委托、Redis pending/ACK 和日志时间线一致。
- [ ] 实盘第一次启用使用小额订单并由人工盯盘。

## 常见问题

### 启动时报 `trading.enabled is false`

这是安全门正常工作。只有在账号、`userdata_mini` 路径、QMT 登录状态和模拟盘验证都确认后，才把 `trading.enabled` 改为 `true`。

### 手动脚本找不到 Redis 配置

确认已创建 `scripts/redis_targets.yaml`，其中存在脚本顶部 `TARGET` 对应的名称。该文件故意被 Git 忽略。

### 出现价格偏离错误

先检查策略参考价是否过期、行情是否已订阅、Windows 与聚宽时间是否一致。不要仅为让订单通过而放大偏离阈值。

### 出现资金或持仓查询失败

执行端会拒绝提交订单。先恢复 QMT 连接并确认账号字段兼容，不要添加默认资金或默认持仓绕过检查。

### 出现“撤单终态未确认”

执行端会熔断后续交易。立即在 QMT 委托列表人工确认是否还有活动订单；在状态不明时不要直接重启并重发信号。

### Windows 服务重启后没有自动处理旧 pending

当前读取循环只消费尚未投递的新消息，没有实现跨 consumer 的自动 `XCLAIM`。先核对原委托是否可能成交，再根据 SQLite 和 Redis pending 做人工恢复。

## 相关文档

- [Windows miniQMT 部署验证清单](docs/windows-qmt-adapter-handoff.md)
- [大 QMT 单文件执行器说明](bigqmt_follower/README.md)
- [聚宽社区项目介绍](docs/joinquant-community-promo.md)

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

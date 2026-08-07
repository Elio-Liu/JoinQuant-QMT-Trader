# JoinQuant QMT 跟单助手

把聚宽云策略产生的交易信号，通过 Redis Stream 可靠地送到 Windows QMT 执行端。

聚宽侧只负责表达“买什么、卖什么、数量和参考价”；执行端负责读取实时行情、核对资金或持仓、计算委托价、下单、确认撤单与成交，并在订单进入明确终态后确认 Redis 消息。核心目标是：**尽量不改策略调用方式，同时让真实交易过程可追踪、可去重、可停止。**

> [!WARNING]
> 本项目包含真实交易链路。仓库默认关闭交易开关，macOS/Linux 单元测试也不能替代目标券商 Windows 客户端的仿真验证。首次启用必须使用仿真账户、小额订单，并由人工盯盘。

## 选择执行方式

仓库提供两条 Windows 执行路径，共用聚宽发送函数、Redis Stream 和交易信号格式，但运行状态与部署方式不同。

| 执行方式 | 适用场景 | 状态与可靠性 | 入口 |
| --- | --- | --- | --- |
| **独立 miniQMT 服务（推荐）** | 可以在 Windows 单独运行 Python 服务 | SQLite 持久账本、`signal_id` 幂等、买卖并发池与开盘卖单屏障、完整订单尝试记录 | `python main.py` |
| **大 QMT 单文件执行器（备用）** | 客户端要求策略只能放在一个 Python 文件中 | 买卖双通道 FIFO、去重和订单状态仅保存在本次运行的内存中，重启不恢复 | `bigqmt_follower/bigqmt_redis_follower.py` |

**同一资金账户严禁同时运行两个执行端**（不同消费组也会各自收到完整消息，造成重复下单）。
不同资金账户的多台交易机则相反——**每台必须使用不同的消费组**：Redis Stream 会把
同一条消息分别交给每个消费组，各机器拿到完整副本后对自己的账户执行（"一个策略 → 多台
交易机（多账户）"，见下文）。

### 一个策略 → 多台交易机（多账户）

聚宽模拟盘只发"意图型信号"（plan / sell_half / sell_all / 不带 amount 的 buy），
数量由每台交易机按**自己账户**的真实资金/持仓计算，因此账户资金不同也能正确跟单。

每台交易机一份 `config.yaml`，关键差异：

| 配置 | 机器 A | 机器 B |
| --- | --- | --- |
| `redis.group` | `qmt_executors_win_a` | `qmt_executors_win_b` |
| `redis.consumer` | `win-qmt-01` | `win-qmt-02` |
| `trading.account_id` | 账号 A | 账号 B |
| `redis.allowed_strategy_ids` | `["harvester"]` | `["harvester"]` |

注意：

- **新消费组从 Stream 最新位置开始消费**（`ensure_group` 使用 `id="$"`）：盘中新加入的
  机器不会重放历史消息（早上的 plan 没有过期时间，重放会导致误建仓）；错过 plan 的机器
  当日不买，符合"宁可少买不盲买"。已存在的组不受影响。
- 每台机器独立 SQLite 账本与日志，盘后按机器对账；单机熔断/宕机不影响其他机器。
- 多账户若要在展示端区分，web 端需按 account 分组（见 `tidalquant-web`），委托备注
  `strategy_name` 可带机名后缀。

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
买卖并发池 + 开盘屏障   买卖各一条内存 FIFO
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
  → 交易信号：按方向进入买入或卖出并发线程池
  → 09:25–09:30 盘前卖单先登记并预挂；买单等待开盘卖单屏障释放
  → SQLite INSERT OR IGNORE：signal_id 幂等门
  → 获取最新价、买一/卖一、可用资金或可卖持仓
  → 计算盘口价/滑点价
  → 提交委托并轮询订单状态
  → 超时：申请撤单，等待真实终态，核对撤单期间成交
  → 仍有剩余：刷新行情和账户资源后重挂
  → 写入最终执行状态
  → XACK
```

## 核心保障

- **低侵入接入**：保留 `publish_trade_signal_to_redis(context, action, code, amount, price)` 五参数签名，精确买卖信号与旧策略调用点无需改造；日计划、半仓/清仓意图由新增发送函数发布。
- **意图型信号按真实账户计算**：`plan` / `sell_half` / `sell_all` / 不带 `amount` 的 `buy` 由执行端按最新资金与持仓计算数量（清仓全卖、半仓取整手、自动买入等分资金并受单票上限约束），账户资金不同也能正确跟单。
- **可靠传输**：使用 Redis Stream 和消费组；Windows 暂时离线时，新消息仍保留在 Stream 中。
- **持久幂等**：独立 miniQMT 服务以 `signal_id` 为 SQLite 主键，重复投递不会再次触达券商。
- **买卖并发池与开盘卖出优先**：买卖各有独立并发池；09:25–09:30 只预挂卖单，09:30 的买单等待所有普通盘前卖单完成或明确终止后再进入执行引擎。
- **每次尝试都刷新状态**：重新读取行情、买入可用资金或卖出可用持仓，不使用旧资源快照继续下单。
- **资源查询失败即停止**：无法确认资金或持仓时不提交委托，不通过默认值继续交易。
- **撤单必须确认**：撤单请求成功不等于订单已撤；只有查询到已成、已撤或废单后，才处理剩余数量。
- **不确定状态熔断**：撤单终态长期无法确认时，当前进程锁停后续交易，避免新旧订单同时成交。
- **ETF tick 精度**：沪市 `5xxxxx`、深市 `1xxxxx` 基金按 `0.001` 报价，其余股票按 `0.01` 报价。
- **开盘卖单快速核对**：09:27 聚宽发布盘前卖单并由 MiniQMT 并行预挂，09:29 不发布买单；09:30 后首次卖单只给 0.5 秒回报宽限，普通未成交单确认撤单后立即刷新行情并重挂剩余数量。
- **跌停卖单不阻塞买入**：跌停卖单成功挂入跌停价队列后即释放开盘屏障，但原订单不撤不重挂，继续排队至成交或 14:56:30。
- **真实资源兜底**：买入的“查询可用资金 → 按 100 股整手缩量 → 提交订单”串行执行，委托受理后的状态轮询仍并行；实盘无可卖持仓时记录 `SKIPPED_NO_POSITION` 并跳过。
- **信号过期保护**：带 `expire_at` 的交易信号在真正开始执行时再次校验；过期或格式非法时不查询交易资源、不下单，并记录终态后 ACK。未携带该字段的旧信号保持兼容。
- **可观测性**：控制台保留简洁中文交易进度，文件日志记录 DEBUG 细节；`sent_at_ms` 可用于估算传输和端到端延迟。

> Redis pending 消息当前不会由新进程自动 `XCLAIM`。进程在执行中崩溃时，消息不会被错误 ACK，但恢复前需要人工核对 QMT 委托、SQLite 账本和 Redis pending，不能直接假设重启后会自动续跑。

## 目录结构

```text
.
├── main.py                              # 独立 miniQMT 服务入口
├── joinquant_signal_sender.py           # 聚宽侧发送函数（复制进策略使用，见「接入聚宽策略」）
├── config.example.yaml                  # 独立服务 YAML 配置模板
├── miniqmt_follower/
│   ├── app.py                           # 依赖组装、买卖并发池、日计划与开盘卖单屏障
│   ├── opening.py                       # 盘前卖单识别与开盘买入等待屏障
│   ├── plan_executor.py                 # 日计划展开为派生信号（清仓 + 待买）
│   ├── sizing.py                        # 意图型信号数量计算（清仓/半仓/自动买入）
│   ├── executor.py                      # 下单、查单、撤单确认、重试状态机
│   ├── redis_stream.py                  # Redis Stream 消费组与消息解析
│   ├── store.py                         # SQLite 信号、日计划和委托尝试账本
│   ├── pricing.py                       # 盘口/滑点/竞价排队定价、tick 和价格笼子
│   ├── models.py                        # 信号、日计划、行情、订单与执行状态模型
│   ├── config.py                        # UTF-8 YAML 配置加载
│   ├── logging_config.py                # 控制台与按天轮转文件日志
│   └── adapters/qmt.py                  # miniQMT 行情和交易适配器
├── bigqmt_follower/
│   ├── bigqmt_redis_follower.py         # 可直接导入大 QMT 的单文件执行器
│   └── README.md                        # 大 QMT 部署与仿真验收说明
```

`config.yaml`、`config_ali.yaml`、`config_tcent.yaml` 等本机配置文件、聚宽策略部署副本（可能含真实 Redis 地址）、本机测试套件（`tests/`）、日志和运行数据库都属于本机部署内容，默认不进入版本控制，不应强制提交。

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
| `execution.auction_aggressive_pct` | 9:15~9:30 排队报价的激进幅度（默认 2%，夹在涨跌停内；0 关闭） |
| `execution.cancel_confirm_timeout_sec` | 撤单后等待券商回报终态的时长，独立于总预算 |
| `execution.order_timeout_sec` / `max_attempts` | 单次等待和最多委托次数 |
| `execution.max_total_duration_sec` | 一条信号的总执行预算 |
| `execution.plan_enabled` | 日计划执行开关（默认 true） |
| `execution.plan_execute_at` | plan 执行时刻 HH:MM:SS（默认 09:30:00） |
| `execution.max_single_position_pct` | 单票买入上限=账户总资产×比例（默认 0.2） |
| `execution.sell_half_insufficient_lot_mode` | `sell_half` 半仓不足一手时的处理：`sell_all`=全卖（默认）/ `skip`=跳过不卖 |
| `execution.limit_down_sell_mode` | `queue`=确认跌停后挂跌停价排队，`skip`=跳过，`none`=普通定价 |
| `execution.queue_sell_deadline` / `max_concurrent_queue_sells` | 跌停卖单截止撤单时间和同时排队上限 |
| `execution.limit_up_buy_mode` | `queue`=确认涨停后挂涨停价排队，`skip`=跳过，`none`=普通定价 |
| `execution.queue_buy_deadline` / `max_concurrent_queue_buys` | 涨停买单截止撤单时间和同时排队上限 |
| `market_data.pre_subscribe_codes` | 启动时预订阅的聚宽格式代码 |
| `trading.account_id` / `miniqmt_path` | 资金账号和 `userdata_mini` 路径 |
| `trading.enabled` | 交易安全门，模板默认 `false` |
| `state_db` / `log_dir` | SQLite 账本和日志目录 |

首次运行先保持 `trading.enabled: false`，确认程序会被安全门拒绝。完成只读检查并切到仿真账户后，才改成 `true` 启动完整链路。

### 3. 启动服务

```powershell
python .\main.py --config config.yaml --workers 8
```

`--workers` 表示买入、卖出每个方向各自的工作线程数，默认 8。盘前多只卖单仍会并行预挂；买单线程可以并行等待开盘屏障。若启用涨跌停排队，应让 worker 数大于对应排队上限，给普通订单保留至少 2 个 worker。完整启动至少应看到：

```text
【系统】🚀 QMT跟单助手启动中 | 买卖各 8 线程并发
【QMT】🔌 交易端已连接
【系统】🟢 Redis监听已启动
```

更完整的参数说明和模拟盘验收步骤见下文「定价与订单执行」「上线检查」两节；大 QMT 备用路径的部署与验收见 [大 QMT Redis 信号执行端说明](bigqmt_follower/README.md)。

## 接入聚宽策略

发送函数集中在 `joinquant_signal_sender.py`，文件内已写明部署配置和典型用法。把
该文件完整内容（或只用到的函数）复制进聚宽策略即可，策略调用点无需改动。文件内包含：

- `publish_trade_signal_to_redis(context, action, code, amount, price)` —— 精确买卖信号，五参数签名保持不变
- `publish_watchlist_to_redis(context, codes)` —— 盘前预订阅股票池
- `publish_daily_plan_to_redis(context, codes_to_sell, codes_to_buy)` —— 日计划（清仓清单 + 待买清单）
- `publish_sell_half_to_redis(context, code, price)` / `publish_sell_all_to_redis(context, code, price)` —— 盘中半仓/清仓意图信号

把文件顶部 `SIGNAL_REDIS_CONFIG` 里的 `host` / `password` / `stream` 改成与
Windows 端 `config.yaml` 的 `redis` 段同一套真实配置，并保持 `stream` 同名；
`SIGNAL_STRATEGY_ID`（默认 `hunter`）要与 Windows 端 `redis.allowed_strategy_ids`
白名单对应。不要把带真实 Redis 配置的部署副本提交回仓库。

交易调用保持五参数：

```python
publish_trade_signal_to_redis(context, "buy", "510300.XSHG", 1000, 3.850)
publish_trade_signal_to_redis(context, "sell", "159915.XSHE", 500, 1.235)
```

选股完成后可以提前推送股票池，只订阅行情、不产生订单：

```python
publish_watchlist_to_redis(context, ["510300.XSHG", "159915.XSHE"])
```

龙头情绪收割机的开盘时序固定为：09:25:45 选股 → 09:26 预订阅 → 09:27 竞价止损
（收集清仓清单）→ 09:28 发送日计划（清仓清单+待买清单）→ 09:30 执行端先清仓、
再按真实可用资金等分买入。10:30 卖半仓 / 14:30 清仓 / 盘中硬止损均由聚宽发
`sell_half` / `sell_all` 意图信号，执行端按真实持仓计算数量。

部署这项时序变更需要同步两份独立部署物：

1. Windows 交易机更新 `miniqmt_follower/` 并重启 `qmt-trader`。
2. 聚宽实时模拟盘粘贴更新后的策略部署副本。

本机 `config.yaml` 不需要新增键；部署前仍需人工复核买卖两侧均为 `queue`、两个排队截止时间均为 `14:56:30`，以及买卖排队上限均为 5。

发送函数会根据 `context.current_dt` 与当前时间判断运行模式。回测、研究和历史补跑不会写入 Redis；实时模式下 `XADD` 成功也只表示 Redis 已接收，**不表示 Windows 已下单或成交**。

## 手动发送信号

仓库不再提供独立手动脚本；发送函数随聚宽策略部署副本维护。本地联调可以直接调用策略里的发布函数，或用 `RedisStreamClient.publish_signal(...)` 向 Stream 写入相同格式的 payload：

```python
from miniqmt_follower.config import load_config
from miniqmt_follower.redis_stream import RedisStreamClient

client = RedisStreamClient(load_config("config.yaml").redis)
client.publish_signal({...})  # 交易信号 / plan / subscribe 均可
```

写入的是 `mode=live` 消息。只要同一 Stream 上存在已启用的真实执行端，就可能产生真实订单；运行前必须确认消费组、账号和信号内容。

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
| `action` | `plan` / `sell_half` / `sell_all`（意图型）或 `buy` / `sell`（精确型） |
| `code` | 聚宽代码，如 `510300.XSHG` |
| `amount` | 目标股数；执行端仍会按最新资金或持仓缩量 |
| `reference_price` | 策略意见价，仅作审计/日志参考，不是最终委托价；执行端不做偏离拒单 |
| `created_at` | 策略侧时间；旧协议字段 `timestamp` 仍可解析 |
| `sent_at_ms` | 可选，发送时的毫秒时间戳，用于延迟日志 |
| `expire_at` | 可选，`YYYY-MM-DD HH:MM:SS`；两个执行端在真正下单前校验，过期后不执行并 ACK |
| `nonce` | 发送侧审计字段；当前执行端不据此去重 |

默认 `signal_id` 在“同一秒、同一策略、同一代码、同一方向、同一数量”下会碰撞，这是有意的重复信号保护。确实需要在同一秒发送两笔独立订单时，发送侧必须生成不同 `signal_id`。

历史消息即使仍带有 `execute_at` 也可正常解析，但两个执行端都会忽略该字段，不再等待预约时间。

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

意图型消息（聚宽只发意图，数量由执行端按真实账户计算）：

```json
{
  "signal_id": "harvester-20260806-plan",
  "strategy_id": "harvester",
  "mode": "live",
  "action": "plan",
  "codes_to_sell": ["000001.XSHE"],
  "codes_to_buy": ["600000.XSHG", "000002.XSHE"],
  "created_at": "2026-08-06 09:28:00",
  "sent_at_ms": 1786044480000
}
```

盘中指令 `action` 取值 `sell_half` / `sell_all`，均不带 `amount`：
执行端按真实可卖持仓计算数量；`buy` 不带 `amount` 时按
`min(可用资金÷待买只数, 总资产×execution.max_single_position_pct)` 计算整手。
旧 `buy`/`sell` + `amount` 协议保持兼容。

## 定价与订单执行

`execution.pricing_mode` 支持：

- `slippage`：买入按最新成交价上浮 `buy_slippage_pct`，卖出按最新成交价下浮 `sell_slippage_pct`。
- `book`：买入按卖一价加 `book_tick_offset` 个 tick，卖出按买一价减相同 tick；对手盘缺失时回退到 `slippage`。

9:15~9:30 另有排队报价：买入报最新价 ×(1+`auction_aggressive_pct`)、卖出报 ×(1−`auction_aggressive_pct`)，结果强制夹在当日涨跌停价之间；取不到涨跌停价则回退上面两种模式。这些委托错过了 9:25 的开盘集合竞价，排队等 9:30 连续竞价开撮，成交价由对手方挂单价逐档决定，所以激进报价买的是时间优先排位而非更差的成交价。

沪深 A 股候选委托价还会按实时对手盘夹进动态有效申报范围：买入基准依次取卖一、买一、最新价，上限取“基准价 102%”与“基准价加 10 tick”的较高者；卖出对称处理。ETF、基金不套用股票价格笼子。

执行端**不做**「行情价偏离 `reference_price` 就拒单」的风控：本端是跟单器，被拦下的委托会让实盘持仓与聚宽模拟盘永久分叉且没有补单机制。价格风险由实时盘口定价、竞价报价的涨跌停夹取和 ±10% 涨跌停带兜底，择时与选股风控属于策略端职责。

普通盘前卖单在 09:30 后先等待 0.5 秒回报；仍未完全成交时进入“申请撤单 → 等待终态 → 核对撤单期间成交 → 刷新行情 → 只重挂剩余数量”的流程。所有普通盘前卖单完成或明确终止后才放行买单。跌停卖单是例外：成功挂入跌停价队列即放行，但该订单继续保留至成交或 14:56:30。

买单按当前委托价和可用资金计算最大数量，再向下取整到 100 股。多个买单的资金查询与提交串行，避免重复使用同一笔现金；委托受理后的订单轮询保持并行。可用资金不足时能买多少整手就买多少，少于 100 股时不下单。卖单不超过实时可卖持仓；实盘可卖持仓为 0 时记录 `SKIPPED_NO_POSITION`，不提交也不重试。

已确认不会成交的废单默认刷新行情和账户资源后重试，包括未知拒单文案；停牌、账户异常、无交易权限、股东账户缺失、禁止买入等明确永久性原因立即终止。若下单是否受理或撤单终态无法确认，执行端停止后续交易，必须先在 MiniQMT 人工对账。

`limit_up_buy_mode: queue` 启用后，只有“涨停价可用、卖一为空、最新价或买一已触及涨停”才按涨停价挂单。该订单保留至完全成交或 `queue_buy_deadline`，期间不因普通超时撤单重挂；默认最多同时保留 `max_concurrent_queue_buys: 5` 笔。

## 大 QMT 单文件备用执行器

如果券商大 QMT 只能导入一个 Python 文件，使用：

```text
bigqmt_follower/bigqmt_redis_follower.py
```

它保持上游信号和 Redis 契约不变，通过后台线程收消息，在大 QMT 调度线程中调用 `passorder`、查询订单和执行撤单。仓库版本的账号、Redis 地址和密码为空，`trading_enabled` 默认关闭。

基础定价与风控规则与独立服务一致：同样的盘口/滑点模式、同样的 9:15~9:30 竞价排队报价
（`auction_aggressive_pct`，夹在涨跌停内，取不到涨跌停价则回退）、同样的 A 股动态价格笼子、
同样的涨跌停 `queue` / `skip` 模式（`limit_down_sell_mode` / `limit_up_buy_mode`）与拒单原因分类，
同样不做参考价偏离拒单；09:25~09:30 的盘前卖单同样会挡住买单，跌停排队单确认挂单后放行买入。
意图型信号（`plan` / `sell_half` / `sell_all` / `auto_buy`）与策略白名单已同步支持。尚未同步的
只有：SQLite 持久账本与重启恢复，以及并发买入资金锁/涨跌停排队并发上限（大 QMT 每方向单线程
FIFO，天然串行，不需要该锁）。

它与独立服务的关键差异：

- 不使用 SQLite；重启后不恢复 `signal_id` 去重、买卖方向 FIFO 队列和未完成订单状态。
- 事件驱动（`order_callback` / `deal_callback` 推送）而非轮询查单。
- 消费组首次从 `$` 创建，只接收创建后的新消息。
- 不自动认领旧 consumer 的 pending。
- 当前只支持普通股票账户，不支持信用账户。
- 必须在券商仿真资金账号的实时策略环境验证，平台“模拟运行模式”可能不会执行交易函数。

完整导入、配置和回调字段核对见 [大 QMT Redis 信号执行端说明](bigqmt_follower/README.md)。

## 本地验证

核心测试使用 `unittest` 和 fake Redis / broker / market data；不需要真实 Redis、QMT 或券商账号。
`tests/` 目录是本机测试套件，不随仓库分发（可能包含依赖私有策略部署副本的用例）；
以下命令只在配置了本地测试副本的机器上有效。

```bash
python -m unittest discover -v
python -m compileall miniqmt_follower bigqmt_follower tests
```

只运行核心执行链路：

```bash
python -m unittest tests.test_executor tests.test_runtime tests.test_store -v
```

策略契约测试（如 `tests/test_harvester_strategy_contract.py`）读取本机部署的策略副本，文件缺失时自动跳过；在没有这些本机文件的环境中，不应把该部分结果当作核心跟单服务的验证结论。

## 上线检查

- [ ] `config.yaml` 等本机配置文件、聚宽生产配置（`SIGNAL_REDIS_CONFIG`）、日志和 SQLite 数据库未进入 Git。
- [ ] Redis 未直接暴露到公网，已使用内网、VPN、来源白名单或安全组。
- [ ] 聚宽发送函数与 Windows 执行端的 Stream 名称一致。
- [ ] 同一批信号只有一个执行端和一个目标账户。
- [ ] Windows、聚宽和 Redis 所在机器完成时间同步；否则延迟日志没有参考意义。
- [ ] 目标券商的行情字段、资金/持仓字段、订单状态与撤单返回值已核对。
- [ ] 仿真盘完成单笔买卖、拒单、部分成交、超时撤单和批量信号测试。
- [ ] 已确认 SQLite、QMT 委托、Redis pending/ACK 和日志时间线一致。
- [ ] 实盘第一次启用使用小额订单并由人工盯盘。

## 常见问题

### 启动时报 `trading.enabled is false`

这是安全门正常工作。只有在账号、`userdata_mini` 路径、QMT 登录状态和模拟盘验证都确认后，才把 `trading.enabled` 改为 `true`。

### 怎么手动发一条信号

仓库不再提供独立脚本。本地联调可用 `RedisStreamClient.publish_signal(...)`（见「手动发送信号」），或直接调用聚宽策略部署副本里的发布函数。写入的是 `mode=live` 消息，同一 Stream 上有启用中的执行端就可能真实下单，发送前确认消费组、账号和信号内容。

### 出现价格偏离错误

先检查策略参考价是否过期、行情是否已订阅、Windows 与聚宽时间是否一致。不要仅为让订单通过而放大偏离阈值。

### 出现资金或持仓查询失败

执行端会拒绝提交订单。先恢复 QMT 连接并确认账号字段兼容，不要添加默认资金或默认持仓绕过检查。

### 出现“撤单终态未确认”

执行端会熔断后续交易。立即在 QMT 委托列表人工确认是否还有活动订单；在状态不明时不要直接重启并重发信号。

### Windows 服务重启后没有自动处理旧 pending

当前读取循环只消费尚未投递的新消息，没有实现跨 consumer 的自动 `XCLAIM`。先核对原委托是否可能成交，再根据 SQLite 和 Redis pending 做人工恢复。

## 相关文档

- [大 QMT Redis 信号执行端说明](bigqmt_follower/README.md)
- [AGENTS.md](AGENTS.md) —— 面向代码助手的架构、信号契约与测试说明

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

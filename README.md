# JoinQuant QMT 跟单助手

把聚宽策略里的买卖想法，自动变成 QMT 里的真实委托。你只管在聚宽写策略，下单、撤单、排队这些脏活交给这个程序。

> [!WARNING]
> 本项目连接真实交易链路。仓库默认关闭交易开关，macOS/Linux 上的测试也不能替代目标券商 Windows 客户端的仿真验证。第一次启用请用仿真账户、小额订单，并人工盯盘。

## 为什么要做这个项目

最早其实是件很实际的事。聚宽回测跑得挺好，但实盘不能靠人盯：开盘那几分钟最忙，选完股要买，手里套着的要卖，真人在 QMT 里手点，一慢就错过价格，一慌就下错单。白天还要上班，更不可能一直盯着屏幕。

后来想法又大了一点。一个人管多个账户的时候，同一个策略要重复下好几遍，而且每个账户资金不一样，手算数量很容易错。所以干脆做一个跟单器：聚宽策略只负责说买什么、卖什么，剩下的全交给执行端。

这个项目从头到尾就一个原则：**模拟盘怎么想的，实盘就怎么执行，中间不靠人。**

## 有什么用

对个人量化玩家来说，它主要解决三件事：

1. 自动执行。开盘抢单、盘中止损、半仓止盈、跌停排队逃生，程序按实时行情和账户情况算好数量直接下单，不用人守着。
2. 可靠落地。信号走 Redis Stream，Windows 关机也不丢；同一个信号不会重复下单；每笔委托、每次撤单都记在 SQLite 里，盘后能对账。
3. 多账户跟单。一份策略信号，多台交易机各跑各的账户，数量按各自真实资金和持仓计算，账户大小不一样也能跟。

它也有一些刻意保留的"笨"：拿不准的时候宁可不下单、宁可停下来，也不乱来。查不到资金就不下单，撤单状态不明就熔断，后面会细说。

## 两条执行路径

| 执行方式 | 适合谁 | 可靠性 | 入口 |
| --- | --- | --- | --- |
| 独立 miniQMT 服务（推荐） | Windows 上能单独跑 Python 服务 | SQLite 持久账本、signal_id 幂等、买卖并发池、开盘卖单屏障、完整委托记录 | `python main.py` |
| 大 QMT 单文件执行器（备用） | 券商大 QMT 只允许策略是单个 Python 文件 | 买卖双通道 FIFO，去重和订单状态只存在本次运行内存里，重启不恢复 | `bigqmt_follower/bigqmt_redis_follower.py` |

同一个资金账户严禁同时跑两个执行端。两边消费组不同，会各收到一份完整消息，等于把单子下两遍。反过来，多台机器管不同账户时，每台必须用不同的消费组，这样每台都能拿到完整消息、各自执行。

### 一份策略，多台机器

聚宽模拟盘现在发的是"意图型信号"（`plan` / `sell_half` / `sell_all` / 不带 `amount` 的 `buy`），信号里不含数量，每台交易机按自己账户的真实资金和持仓计算。账户资金不一样，也能正确跟单。

每台机器一份 `config.yaml`，差异就几个地方：

| 配置 | 机器 A | 机器 B |
| --- | --- | --- |
| `redis.group` | `qmt_executors_win_a` | `qmt_executors_win_b` |
| `redis.consumer` | `win-qmt-01` | `win-qmt-02` |
| `trading.account_id` | 账号 A | 账号 B |
| `redis.allowed_strategy_ids` | `["harvester"]` | `["harvester"]` |

两点要注意。新消费组从 Stream 最新位置开始消费，盘中新加的机器不会重放早上的消息（早上的 plan 没有过期时间，重放会误建仓）；错过 plan 的机器当天不买，宁可少买不盲买。每台机器有独立的 SQLite 账本和日志，盘后按机器对账，单台宕机不影响其他机器。

## 快速开始（部署）

### 1. 准备环境

需要三样东西：

- Windows 10/11，装好并登录了 miniQMT，账号能手工下单、撤单、查委托
- 一个 Windows 和聚宽都能访问的 Redis
- 一个能 `import xtquant` 的 Python 环境（xtquant 通常随 miniQMT 提供，别从别的环境乱拷）

装公共依赖：

```powershell
python -m pip install redis PyYAML
```

先确认当前解释器能用 xtquant：

```powershell
python -c "import xtquant; print('xtquant ok')"
python -c "from xtquant import xtdata; print('xtdata ok')"
python -c "from xtquant.xttrader import XtQuantTrader; print('xttrader ok')"
```

### 2. 建配置

```powershell
Copy-Item config.example.yaml config.yaml
$env:REDIS_PASSWORD="你的 Redis 密码"
```

打开 `config.yaml`，下面这几项是必改的：

| 配置 | 改什么 |
| --- | --- |
| `redis.host` / `port` / `password` | Redis 地址和密码，密码可写 `${REDIS_PASSWORD}` 从环境变量读 |
| `redis.stream` | Stream 名字，必须和聚宽发送函数里的一致 |
| `redis.group` / `consumer` | 消费组和执行器名字，多台机器各用各的 |
| `trading.account_id` / `miniqmt_path` | 资金账号和 miniQMT 的 `userdata_mini` 目录 |
| `trading.enabled` | 交易安全门，先保持 `false` |

先保持 `trading.enabled: false` 启动一次，看到安全门拒绝是正常的。只读检查都过了、仿真账户验证没问题，再改成 `true`。

### 3. 启动服务

```powershell
python .\main.py --config config.yaml --workers 8
```

`--workers` 是买卖方向各自的并发线程数，默认 8。启动正常会看到：

```text
【系统】🚀 QMT跟单助手启动中 | 买卖各 8 线程并发
【QMT】🔌 交易端已连接
【系统】🟢 Redis监听已启动
```

### 4. 接入聚宽策略

发送函数集中在 `joinquant_signal_sender.py`，文件里写明了用法。把文件内容（或只用到的函数）粘进聚宽策略，然后改文件顶部的 `SIGNAL_REDIS_CONFIG`：`host`、`password`、`stream` 要和 Windows 端 `config.yaml` 一致，`SIGNAL_STRATEGY_ID` 要和 `redis.allowed_strategy_ids` 白名单对应。填真实配置之前，先确认不会把带密码的副本提交到仓库。

策略里最常用的几个调用：

```python
# 精确买卖（数量由策略定）
publish_trade_signal_to_redis(context, "buy", "510300.XSHG", 1000, 3.850)
publish_trade_signal_to_redis(context, "sell", "159915.XSHE", 500, 1.235)

# 盘前预订阅股票池，只订阅行情、不下单
publish_watchlist_to_redis(context, ["510300.XSHG", "159915.XSHE"])

# 意图型信号：数量由执行端按真实持仓/资金算
publish_sell_half_to_redis(context, "000001.XSHE", 10.5)   # 卖半仓
publish_sell_all_to_redis(context, "000002.XSHE", 9.8)     # 清仓

# 日计划：清仓清单 + 待买清单
publish_daily_plan_to_redis(context, ["000001.XSHE"], ["600000.XSHG", "000002.XSHE"])
```

以龙头情绪收割机的开盘时序为例：09:25:45 选股，09:26 推送预订阅，09:27 竞价止损（收集清仓清单），09:28 发日计划，09:30 执行端先清仓、再按真实可用资金等分买入。10:30 卖半仓、14:30 清仓、盘中硬止损，都由聚宽发 `sell_half` / `sell_all` 意图信号，执行端按真实持仓算数量。

发送函数会按 `context.current_dt` 和系统时间判断回测还是实盘。回测、研究、历史补跑不会写 Redis；实盘 XADD 成功也只代表 Redis 收到了，不代表已经下单成交。

## 配置说明

配置在 `config.yaml` 里分四块：`redis`、`execution`、`market_data`、`trading`。除了上面必改项，下面这些按你的习惯调：

| 配置 | 说明 |
| --- | --- |
| `execution.pricing_mode` | `slippage`（最新价±滑点）或 `book`（吃对手盘），默认 `book` |
| `execution.auction_aggressive_pct` | 9:15~9:30 排队报价的激进幅度，默认 2%，0 关闭 |
| `execution.order_timeout_sec` / `max_attempts` / `max_total_duration_sec` | 单次等待、最多委托次数、总执行预算 |
| `execution.cancel_confirm_timeout_sec` | 撤单后等真实终态的时长，独立于总预算，默认 30，别调太小 |
| `execution.signal_expire_seconds` | 信号过期秒数，默认 600（10 分钟），0=不过期 |
| `execution.plan_enabled` / `plan_execute_at` | 日计划开关和执行时刻，默认开、09:30:00 |
| `execution.max_single_position_pct` | 自动买入单票上限=总资产×比例，默认 0.2 |
| `execution.sell_half_insufficient_lot_mode` | 半仓不足一手时 `sell_all`=全卖（默认）/ `skip`=不卖 |
| `execution.limit_down_sell_mode` / `limit_up_buy_mode` | 跌停卖/涨停买的处理：`queue`=挂涨跌停价排队、`skip`=跳过、`none`=普通定价 |
| `execution.queue_sell_deadline` / `queue_buy_deadline` | 排队截止时间，默认 14:56:30 |
| `execution.max_concurrent_queue_sells` / `max_concurrent_queue_buys` | 涨跌停排队并发上限 |
| `market_data.pre_subscribe_codes` | 启动时预订阅的代码，一般留空（策略会推送股票池） |
| `state_db` / `log_dir` | SQLite 账本路径和日志目录 |

两个经常需要解释的点：

信号过期。发送后 10 分钟内开始执行的信号都会执行，超过 10 分钟还没轮到就丢弃（记 `EXPIRED` 终态并 ACK）。这个秒数按机器单独配，网络慢的机器可以放宽。旧协议里自带 `expire_at` 的消息仍然优先按那个绝对时间判断。

半仓不足一手。`sell_half` 在半仓取整后不足 100 股时，默认把整个持仓卖掉（持仓 100 股就是全卖）；如果改成 `skip`，这种情况就不卖，留到下次 `sell_all` 再处理。

## 系统架构

```text
聚宽云策略
  ├─ publish_watchlist_to_redis(...)       盘前推送股票池，仅预订阅行情
  └─ publish_trade_signal_to_redis(...)    发送买卖/意图信号
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

独立服务的主链路：

```text
RedisStreamClient.read_forever()
  → 预订阅指令：立即订阅行情并 ACK
  → 交易信号：按方向进买入/卖出并发线程池
  → 09:25–09:30 盘前卖单先登记预挂，买单等开盘屏障
  → SQLite 幂等门（signal_id 主键）
  → 拉最新行情、可用资金/可卖持仓
  → 计算委托价（竞价排队 / 盘口 / 滑点）
  → 下单并轮询状态
  → 超时撤单，等真实终态，核对撤单期间成交
  → 有剩余就刷新行情和资源重挂
  → 写终态，XACK
```

执行端有几点设计是故意的，值得知道：

- 不做"价格偏离参考价就拒单"。本端是跟单器，拦下一笔单，实盘持仓就和聚宽模拟盘永远对不上，而且没人会补发。价格风险靠实时盘口定价、涨跌停夹取和 ±10% 涨跌停带兜底，择时选股是策略的事。
- 撤单必须确认。撤单请求发出去不等于撤了，只有查到已成、已撤或废单才算数，然后按撤单期间的成交核对剩余数量。
- 状态不明就熔断。下单是否受理、撤单是否成功都确认不了的时候，停止后续所有交易，等人去 QMT 里对账。
- 跌停卖单走排队。确认跌停锁盘后挂跌停价排队等开板，期间不撤不重挂（重挂丢队列位置），到 14:56:30 才收尾。涨停买单同理。

## 信号协议

Redis Stream 每条消息用字段 `payload` 装 JSON。精确买卖信号：

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
  "nonce": "a1b2c3d4"
}
```

| 字段 | 说明 |
| --- | --- |
| `signal_id` | 幂等键，默认由策略、时间、代码、方向、数量组成 |
| `strategy_id` | 信号来源 |
| `mode` | 只有 `live` 会进入真实执行路径 |
| `action` | `buy` / `sell`（精确）或 `plan` / `sell_half` / `sell_all`（意图） |
| `code` | 聚宽代码，如 `510300.XSHG` |
| `amount` | 精确信号的目标股数；执行端仍会按最新资金/持仓缩量 |
| `reference_price` | 策略参考价，只作审计，执行端按实时行情重新定价 |
| `created_at` | 策略侧时间，旧字段 `timestamp` 仍可解析 |
| `sent_at_ms` | 发送时刻毫秒时间戳，延迟日志和过期判断都用它 |
| `nonce` | 发送侧审计字段，执行端不据此去重 |

过期不再由发送端写死：发送端只带 `sent_at_ms`，过期秒数在交易端配置（见上）。旧协议消息里如果还带 `expire_at`，执行端仍会优先按它判断；`execute_at` 已废弃，解析但忽略。

默认 `signal_id` 在同一秒、同一策略、同一代码、同一方向、同一数量下会碰撞，这是故意的重复保护。确实要同秒发两笔独立订单，发送侧得自己改 `signal_id`。

意图型消息示例（日计划）：

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

盘中 `sell_half` / `sell_all` 不带 `amount`，数量按真实可卖持仓算；`buy` 不带 `amount` 时按 `min(可用资金÷待买只数, 总资产×max_single_position_pct)` 计算整手。旧的 `buy`/`sell` + `amount` 协议完全兼容。

## 大 QMT 单文件备用执行器

如果券商大 QMT 只允许策略是单个 Python 文件，就用 `bigqmt_follower/bigqmt_redis_follower.py`。它和独立服务共用同一套 Redis 契约，后台线程收消息，主线程里调 `passorder` 下单。仓库版本账号、Redis 地址、密码都是空的，`trading_enabled` 默认关。

定价、风控、涨跌停排队、意图信号和日计划都已和独立服务对齐，差别只剩：

- 不用 SQLite，重启后去重、FIFO 队列、未完成订单全部不恢复
- 事件驱动（order_callback / deal_callback）而不是轮询查单
- 每方向单线程 FIFO，天然串行，所以没有并发资金锁和排队并发上限
- 只支持普通股票账户，不支持信用账户

部署和验收步骤见 [大 QMT Redis 信号执行端说明](bigqmt_follower/README.md)。

## 上线检查

上线前过一遍这个清单：

- [ ] `config.yaml` 等本机配置、聚宽生产配置、日志、SQLite 数据库没进 Git
- [ ] Redis 没暴露公网，走内网、VPN、白名单或安全组
- [ ] 聚宽发送函数的 Stream 和 Windows 端 `config.yaml` 一致
- [ ] 同一批信号只有一个执行端、一个目标账户
- [ ] Windows、聚宽、Redis 时间同步（延迟日志和过期判断都依赖它）
- [ ] 券商行情字段、资金/持仓字段、订单状态和撤单返回值核对过
- [ ] 仿真盘跑过单笔买卖、拒单、部分成交、超时撤单、批量信号
- [ ] SQLite、QMT 委托、Redis pending/ACK、日志四条线能对得上
- [ ] 实盘第一次启用用小额订单，人工盯盘

## 本地验证

`tests/` 是本地测试套件，不随仓库分发（里面可能有依赖私有策略副本的用例）。在配好本地副本的机器上：

```bash
python -m unittest discover -v
python -m compileall miniqmt_follower bigqmt_follower tests
```

只跑核心执行链路：

```bash
python -m unittest tests.test_executor tests.test_runtime tests.test_store -v
```

测试只用 unittest 和 fake Redis / broker / 行情，不需要真实 Redis、QMT 或券商账号。

## 手动发一条信号（联调用）

仓库不提供独立脚本。本地联调可以直接用 `RedisStreamClient.publish_signal(...)` 往 Stream 写一条相同格式的消息：

```python
from miniqmt_follower.config import load_config
from miniqmt_follower.redis_stream import RedisStreamClient

client = RedisStreamClient(load_config("config.yaml").redis)
client.publish_signal({...})  # 交易信号 / plan / subscribe 均可
```

写入的是 `mode=live` 消息。只要同一 Stream 上有启用中的执行端，就可能真实下单，发送前确认消费组、账号和内容。

## 常见问题

### 启动时报 `trading.enabled is false`

安全门正常。账号、路径、QMT 登录、仿真盘都确认过，再改成 `true`。

### 信号显示 EXPIRED，没下单

信号发出 10 分钟（默认）内没开始执行就会被丢弃。先看执行端是不是一直没轮到它（队列积压、worker 不够），再看机器时间是否同步，最后确认 `signal_expire_seconds` 没被改小。

### 资金或持仓查询失败

执行端会拒单。恢复 QMT 连接、核对账号字段，不要用默认资金或默认持仓绕过。

### 撤单终态未确认

执行端会熔断。去 QMT 委托列表人工确认还有没有活动订单，状态不明时不要直接重启重发。

### 重启后没有自动处理旧 pending

程序只消费新消息，不会自动认领别的 consumer 的 pending。先核对原委托是否可能成交，再按 SQLite 和 Redis pending 人工恢复。

## 相关文档

- [大 QMT Redis 信号执行端说明](bigqmt_follower/README.md)
- [AGENTS.md](AGENTS.md)：面向代码助手的架构和信号契约说明

## 免责声明

本项目只用于技术研究和个人自动化实验，不构成投资建议。实盘交易有风险，使用前请确认符合券商、交易所和相关法律法规的要求，任何交易损失由使用者自行承担。

## 打赏赞助

如果这个项目帮你省了盯盘和手工操作的时间，欢迎请作者喝杯咖啡。

<div align="center">

<table>
<tr>
  <td align="center"><img src="images/微信.png" alt="微信打赏" width="220"/><br/>微信</td>
  <td align="center"><img src="images/支付宝.png" alt="支付宝打赏" width="220"/><br/>支付宝</td>
</tr>
</table>

</div>

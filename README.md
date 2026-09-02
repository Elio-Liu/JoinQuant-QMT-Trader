# JoinQuant QMT 跟单助手

把聚宽策略里的买卖想法，自动变成 QMT 里的真实委托。你只管在聚宽写策略，下单、撤单、排队这些脏活交给这个程序。开箱即用的**纯跟单模式**适合绝大多数策略；想把风控也搬到交易端的，还有**本地策略引擎**这个进阶玩法。

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

> [!IMPORTANT]
> 独立 miniQMT 服务有两种运行模式，由 `main.py` 同级有没有 `config.strategy.yaml` 决定：**纯跟单模式**（没有该文件，默认）就是泛用跟单程序，白名单策略发什么信号就执行什么；**本地策略引擎模式**（有该文件，进阶）把选股之外的风控全部搬进交易端。大多数用户只需要纯跟单模式，见下文「两种运行模式」。

| 执行方式 | 适合谁 | 可靠性 | 入口 |
| --- | --- | --- | --- |
| 独立 miniQMT 服务（推荐） | Windows 上能单独跑 Python 服务 | 纯跟单/本地策略引擎双模式、SQLite 持久账本、signal_id 幂等、买卖并发池、开盘卖单屏障、涨跌停排队专用池、完整委托记录 | `python main.py` |
| 大 QMT 单文件执行器（备用） | 券商大 QMT 只允许策略是单个 Python 文件 | 买卖双通道 FIFO，去重和订单状态只存在本次运行内存里，重启不恢复 | `bigqmt_follower/bigqmt_redis_follower.py` |

同一个资金账户严禁同时跑两个执行端。两边消费组不同，会各收到一份完整消息，等于把单子下两遍。反过来，多台机器管不同账户时，每台必须用不同的消费组，这样每台都能拿到完整消息、各自执行。

## 两种运行模式

独立 miniQMT 服务可以按你的需要切换成两种角色，开关很简单：**`main.py` 同级有没有 `config.strategy.yaml`**。

**先看结论：绝大多数人用纯跟单模式就够了。** 每个人的策略千差万别，`config.strategy.yaml` 那套规则配置只适合一种特定的玩法（把风控搬进交易端）；如果你的策略自己在聚宽里决定买卖时机，交易端只负责把信号变成真实委托，那就是纯跟单模式——一份 `config.yaml` 搞定，没有额外的策略文件要学。

| | 纯跟单模式（推荐，默认） | 本地策略引擎模式（进阶） |
| --- | --- | --- |
| 触发条件 | 不存在 `config.strategy.yaml` | 存在该文件 |
| 谁做买卖决策 | 聚宽策略（发什么跟什么） | 交易端本地引擎（候选计划驱动） |
| 接受的信号 | `plan` / `buy` / `sell` / `sell_half` / `sell_all` / `watchlist`，全部照常执行 | `candidate_plan` + 白名单内的 `sell_all` 卖出信号 |
| 策略白名单 | 可以列多个策略 id（空=不过滤，遗留行为） | 严格单策略（`allowed_strategy_ids` 只能有 `strategy_engine.strategy_id` 一项） |
| 策略层执行开关 | 可放在主配置 `execution`（机器默认值兜底） | 必须放在 `config.strategy.yaml` 的 `execution` 节点 |
| 适合谁 | 已有聚宽策略、只想自动跟单的通用场景 | 想把风控/资金分配下放到交易端的自营策略 |

- **纯跟单模式**：不建本地策略引擎、不拒绝任何普通信号，回到"策略说什么、交易端做什么"的经典跟单形态。`candidate_plan` 没有消费方，会被记日志后直接 ACK。部署时只需 `config.yaml` 一份配置，快速开始一节默认按这个模式走。
- **本地策略引擎模式**：聚宽只发选股结果和止损等退出信号，交易端按 `config.strategy.yaml` 的时间表自己决策。详细机制见「进阶：本地策略引擎」一章——这是本仓库的实现重点，但不是大多数人的刚需。

**为什么把风控放在交易端更快**：聚宽模拟盘有约 10 秒的行情与执行延迟，止损/退出信号从触发到送达交易端还要再过一个网络往返；而交易端本地引擎直接读 QMT 实时行情、按亚秒级节奏逐 tick 判断，触发即下单。开盘竞价定盘、盘中急跌这些场景下，本地风控通常能比"等聚宽喊话"早一个数量级反应。代价是策略逻辑要写成 `config.strategy.yaml` 的规则配置——如果你不需要这个收益，留在纯跟单模式就好。

### 一份策略，多台机器

纯跟单模式下发的是"意图型信号"（`plan` / `sell_half` / `sell_all` / 不带 `amount` 的 `buy`）；本地策略引擎模式下聚宽只发候选计划与带 `purpose` 标签的卖出信号。这些消息都不含数量，每台交易机按自己账户的真实资金和持仓计算。账户资金不一样，也能正确跟单。

每台机器一份 `config.yaml`，差异就几个地方：

| 配置 | 机器 A | 机器 B |
| --- | --- | --- |
| `redis.group` | `qmt_executors_win_a` | `qmt_executors_win_b` |
| `redis.consumer` | `win-qmt-01` | `win-qmt-02` |
| `trading.account_id` | 账号 A | 账号 B |
| `redis.allowed_strategy_ids` | `["YOUR_STRATEGY_ID"]` | `["YOUR_STRATEGY_ID"]` |

两点要注意。新消费组从 Stream 最新位置开始消费，盘中新加的机器不会重放早上的消息（早上的 plan 没有过期时间，重放会误建仓）；错过 plan 的机器当天不买，宁可少买不盲买。已经存在的组在交易机重启后会找回本组未确认消息，先按 `signal_id` 去 QMT 订单备注核对，再决定继续、确认或停机待人工处理。每台机器有独立的 SQLite 账本和日志，盘后按机器对账，单台宕机不影响其他机器。

## 快速开始（部署）

### 1. 准备环境

需要三样东西：

- Windows 10/11，装好并登录了 miniQMT，账号能手工下单、撤单、查委托
- 一个 Windows 和聚宽都能访问的 Redis
- 一个能 `import xtquant` 的 **Python 3.11 或更高版本**环境（xtquant 通常随 miniQMT 提供，别从别的环境乱拷）

3.11 是硬下限：`miniqmt_follower/models.py` 用了 `enum.StrEnum`，3.10 上整个包都 import 不进去。`python main.py` 启动时会先校验这一条，不达标会直接给一句人话而不是从包深处冒出来的 ImportError。

装公共依赖：

```powershell
python -m pip install -r requirements.txt
```

只装两个包：`redis` 和 `PyYAML`。**xtquant 不在里面**，它必须用交易机上 miniQMT 自带的那一份——PyPI 上那个同名包是第三方上传的，真正下单要的 `xtquant.xttrader` 依赖随终端分发的本地二进制，pip 装它只会掩盖问题。理由和其他运行环境（大 QMT、测试脚本、聚宽云端）各自的依赖，都写在 `requirements.txt` 的注释里。

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

**到这里，纯跟单模式（大多数人）就配完了**——不需要任何额外文件。白名单里的策略发什么信号，交易端就执行什么（`plan`/`buy`/`sell`/`sell_half`/`sell_all`/`watchlist`）。

<details>
<summary>进阶：切换到本地策略引擎模式（把风控搬进交易端）</summary>

在 `config.yaml` 同目录创建固定文件 `config.strategy.yaml`（从公开模板 `config.strategy.example.yaml` 复制改名，模板全是虚构示例值）。该文件没有命令行替代参数，并由 `.gitignore` 排除；真实策略开关、阈值和部署参数只保留在交易机本地，不得提交。缺失、关闭、字段拼错或与主配置冲突都会直接阻止启动。本机 `deploy/<机器名>/` 目录（不入库）还留有各交易机的部署配置对，复制到交易机即可。

本地策略引擎模式的联动条件（纯跟单模式不需要满足）：

- `redis.allowed_strategy_ids` 只能包含 `strategy_engine.strategy_id` 这一项；这是专用账户，不允许第二个策略进入同一真实账户。
- `config.strategy.yaml` 的 `execution.limit_down_sell_mode` 必须为 `queue`，因为 `strategy_engine.schedule.opening_exit.trigger_at`（示例 `09:27:00`）开始的跌停退出依赖跌停排队。
- 涨跌停排队单（买卖两侧）在 `qmt-queue` 专用线程池慢轮询，不再占用买卖 worker；`--workers`（默认 8）只需覆盖策略单日目标股数。优雅退出时排队单会主动撤单收口（上界 ≈ `cancel_confirm_timeout_sec` + 轮询间隔），硬杀语义不变：重启后按 QMT 订单备注恢复跟踪。
- `config.strategy.yaml` 内每条规则、总仓位上限和现金预留都有独立 `enabled` 开关；单票上限由 `execution.max_single_position_pct` 单一键约束（策略层开盘买入/回款补仓按持仓上限使用，执行层 auto_buy 按单笔买入上限使用）；比例与时间严格校验。

</details>

### 3. 启动服务

把 `config.yaml` 放在 `main.py` 同级目录（默认配置路径已锚定在同级，任意工作目录启动都能找到；本地策略引擎模式还需要同级的 `config.strategy.yaml`）：

```powershell
python .\main.py
```

需要覆盖默认配置路径或调整并发数时：

```powershell
python .\main.py --config path\to\config.yaml --workers 8
```

`--workers` 是买卖方向各自的并发线程数，默认 8。启动正常会看到：

```text
【系统】🚀 QMT跟单助手启动中 | 买卖各 8 线程并发
【系统】🧭 运行模式: 纯跟单 | 无本地策略引擎, 只执行白名单策略下发的普通信号
【QMT】🔌 交易端已连接
【系统】🟢 Redis监听已启动
```

本地策略引擎模式下第二行会显示 `运行模式: 本地策略引擎 | strategy_id=...`，其余相同。

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

上面这些函数就是**纯跟单模式**要用的全部：策略照常买卖，执行端负责落地。**本地策略引擎模式**则反过来——实际只调用下面这个函数，上面的交易函数在引擎模式下发出来也不会执行。止损等退出信号由策略部署副本内带 `purpose` 标签的 `sell_all` 发送函数发出（purpose 必须出现在 `config.strategy.yaml` 的 `external_signals.allowed_sell_purposes` 白名单里，发送函数带每分钟补发队列直到 XADD 成功），本仓库的 `joinquant_signal_sender.py` 是这些函数的基础版本，`strategies/` 下的部署副本保留完整逻辑。

```python
# 选股完成后发送有序候选列表；空列表也要发送，明确表示“今天不买”。
publish_candidate_plan_to_redis(
    context,
    selected_codes,
    strategy_version="local-engine-v1",
)
```

候选计划必须在 `strategy_engine.schedule.candidate_plan.accept_until`（示例值 `09:35:00`）之前收到并通过日期、超龄、行情、账户和开盘卖单检查。服务在 `strategy_engine.schedule.opening_exit.trigger_at`（示例值 `09:27:00`）后重启时，会先补做开盘退出；只要整批买入前置条件在 `strategy_engine.schedule.opening_buy.admit_until`（示例值 `09:35:00`）之前全部通过，兄弟买单即使随后在线程队列里跨过截止边界也继续执行；截止时刻所在整秒起不再接纳新批次，回款补仓窗口同步收口。

对于开盘调仓策略，可以先推送预订阅和待卖清单，再发日计划。执行端会先处理盘前卖单，买单等到开盘屏障放行后，按各账户真实可用资金计算数量。具体选股、发送和卖出时间由使用者自己的策略决定，不在公开仓库中记录。

发送函数会按 `context.current_dt` 和系统时间判断回测还是实盘。回测、研究、历史补跑不会写 Redis；实盘 XADD 成功也只代表 Redis 收到了，不代表已经下单成交。

## 进阶：本地策略引擎（交易端自己做风控）

> [!NOTE]
> 本章是进阶内容：**大多数跟单场景用不到它**。如果你的策略自己决定买卖时机（绝大多数情况），直接用上面的纯跟单模式即可，跳过本章。只有想把风控和资金分配从聚宽搬到交易端、规避约 10 秒模拟盘延迟时，才需要本章。

专用账户模式下，交易机上跑着一个完整的本地策略引擎，不靠聚宽逐笔指挥，而是每天按一张时间表自己决策。聚宽只做两件事：**盘前把选股结果发过来**（`candidate_plan`），**盘中发现止损等退出信号就喊一声**（`sell_all`，purpose 白名单可配置）。买多少、什么时候卖、卖多少，全部由交易机按自己的真实账户决定。这份"时间表"就是 `config.strategy.yaml`（公开模板见 `config.strategy.example.yaml`），改任何一个时间都会改变当天行为。

### 一天的完整时间线

下表全部时间都是**虚构示例值**（与 `config.strategy.example.yaml` 一致，不代表任何真实部署口径）；机器侧时间边界来自主配置 `machine_schedule`（示例值与 `config.example.yaml` 一致）：

| 时刻 | 发生了什么 | 配置来源 |
| --- | --- | --- |
| `09:15:00` | 集合竞价开始，执行层进入竞价保护与激进定价 | `machine_schedule.market_session` |
| `09:27:00` | 竞价定盘后首次检查持仓：跌停的挂跌停价排队清仓，低开的全清；盘前窗口内还同步评估本地硬止损，卖款将进入买入预算 | `opening_exit.trigger_at` |
| `09:25:30` | 第一波买入窗口：候选计划送达即用**盘前可用资金**整批接纳、盘前挂单排队，9:30 开盘价撮合（不等卖单屏障） | `opening_buy.preopen_start_at` |
| `09:30:00` | 开盘。第一波没发生（重启等）时，退回"等卖单屏障清空后整批买入"的路径 | `opening_buy.start_at` |
| `09:30:00` 起 | **回款补仓窗口**：每 tick 检查"可用资金 − 第一波基线"的回款池，卖款到账够一手就补一波（多波次）；买入提交即冻结资金、池子归零等下一笔回款 | `opening_buy.admit_until` 收口（示例 `09:35:00`） |
| `09:31:00` | 本地硬止损开始，按 `interval_sec` 间隔检查持仓（本地兜底止损；主止损由聚宽按纸面成本决策，两套并存先到先卖） | `hard_stop.start_at` / `interval_sec` |
| `10:00:00` | 上午退出：未涨停的票，亏损的全清、盈利的卖一半锁盈 | `morning_exit.trigger_at` |
| `14:00:00` | 下午退出：未涨停的全清；还涨停的持有过夜 | `afternoon_exit.trigger_at` |
| `14:56:00` | 本地策略最后一笔新卖单；硬止损最后一次检查 | `order_guard` / `hard_stop.last_check_at` |
| `14:56:30` | 涨跌停排队单统一撤单收尾（排队到这时还没成交就撤单走人） | `order_guard.limit_down/up_queue_cancel_at` |
| `15:02:00` | 按 QMT 真实账户输出日结，关闭策略日 | `lifecycle.daily_summary_at` |

中途重启不会乱套：每个时点都有"补执行窗口"（`retry_until`），已卖出的票被事件去重跳过，已接纳的批次从 SQLite 账本重建，详见下文"账本与重启续跑"。

### 买入：开盘两波 + 回款补仓

1. **第一波（盘前挂单）**：候选计划送达后，把当时账户的**可用资金**在候选股之间等分（扣除现金预留、单票上限，已持仓的跳过），盘前就挂单排队，开盘价撮合。这笔钱是昨天留的现金，不等任何卖单。
2. **回款补仓（第二波起）**：开盘后卖掉的持仓会回笼现金。引擎每 tick 计算 回款池 = 当前可用资金 − 第一波基线。池子够一手（含少量报价余量，防顶格一手被报价上浮顶成废单）就按等分再买一波；买单提交即冻结资金、池子自然归零，下一笔卖款到账再起下一波，直到 `admit_until`（示例 `09:35:00`）收口。这是**多波次**：卖一笔、补一笔，而不是"等所有卖单完成后一次性补"——失败的卖单终态也会清空屏障，旧的一次性方案会永久漏掉它迟到的回款。
3. **波次编号与重启**：每波信号 id 带波次序号（`...-topup01-...`、`topup02`…），从当日已落库的补仓信号数重建。重启后不会重发旧波，接着从下一波继续。
4. **不足一手就留池**：单波内某只候选分到的预算买不起一手（含报价余量），这笔钱留在池子里，和下一 tick 新增回款合并再分配，不产生零股废单。

### 卖出：五条规则，一个原则

| 规则 | 触发条件 | 动作 | 备注 |
| --- | --- | --- | --- |
| 开盘退出 | 跌停 / 低开低于昨收（阈值可配，`0`=低于昨收即触发） | 全清 | 跌停的挂跌停价排队，不贱卖 |
| 硬止损（本地兜底） | 现价跌破持仓成本×阈值（比例由 `config.strategy.yaml` 配置，示例 5%），按 `interval_sec` 检查 | 全清 | 主止损由聚宽按纸面成本下发信号；本地这条只在信号链丢失时兜底，两套并存、先到先卖 |
| 上午退出 | 10:00 未涨停：亏损→全清；盈利→卖一半 | 全清 / 卖半 | 半仓不足一手按配置 `sell_all` 或 `skip` |
| 下午退出 | 14:00 未涨停→全清；涨停→持有过夜 | 全清 / 不动 | 涨停票不追卖 |
| 回落止盈 | 现价自买入以来最高价回落 ≥ 阈值 | 全清 | 默认关闭，是否启用建议按回测结论；最高价=当日 QMT tick high + 跨日 SQLite 记录 |

所有卖出都以"事件"为单位落库：每条规则 × 每只股票每天只触发一次，触发过就再也不会重复卖。行情太旧、缺少涨跌停价、快照日期不对，规则会被**阻塞**（BLOCK）而不是带病决策——数据不合法就等下一 tick 再判断，绝不降级乱卖。

### 账本与重启续跑

策略引擎的状态全部落在同一个 SQLite 文件里（`state_db`），加上执行层的三张表，共八张：

| 表 | 存什么 |
| --- | --- |
| `signals` / `order_attempts` | 每条信号与每次下单尝试（执行层账本，幂等/撤单/重挂都在这里） |
| `plans` | 旧协议日计划的接收审计 |
| `candidate_plans` | 今天收到的候选计划（含 LATE/REJECTED 审计记录） |
| `strategy_days` | 策略日状态：ACTIVE / NO_PLAN / PLAN_CONFLICT / HALTED / CLOSED |
| `strategy_events` | 每条规则每只股票的事件：是否已触发/已提交/已终态 |
| `strategy_state` | 键值对（如第一波资金基线 `wave1_cash_baseline`，重启后靠它识别回款） |
| `position_highs` | 跨日持仓最高价记录（回落止盈用） |

还有一个总原则：**拿不准就不买**。候选计划迟到、内容冲突（同一天两个不同内容的计划）、账户数据读不出来，当天直接禁止新买入（熔断策略日）——但已经持有的票的止损和定时退出照常管理。卖出永远比买入优先。

### 策略配置模板：`config.strategy.example.yaml`

这份模板是本项目最有特色的部分：**它把"策略"变成了一份可以校验、可以版本管理、可以审计的配置**，而不是散落在代码里的 if-else。公开仓库随附的 `config.strategy.example.yaml` 只含虚构示例值，复制改名为 `config.strategy.yaml` 后逐项填写即可。全部字段严格必填——缺一个、写错一个、或者和主配置冲突，启动时直接给出人话报错，不会带着错配置悄悄跑。

模板分四大块，对应上文的四件事：

1. **`external_signals` + `schedule`**——接什么、几点干活。聚宽卖出信号的 purpose 白名单（防止旧消息双重决策），以及候选计划/开盘退出/开盘买入/硬止损/上午退出/下午退出各自的时间窗口与补执行截止。
2. **各规则开关与阈值**——怎么干。`candidate_plan`（候选数量与消息超龄上限）、`opening_buy`（是否等卖单屏障、回款补仓、资金等分/总仓位上限/现金预留/单票上限）、`opening_exit`（低开阈值、跌停排队要求）、`intraday_hard_stop`（兜底止损阈值、涨停豁免）、`morning_exit`（亏损清仓/盈利减半）、`afternoon_exit`（未涨停清仓/涨停持有）、`trailing_take_profit`（回落止盈，默认关）、`limit_detection` 与 `data_safety`（涨跌停判定容差、行情时效门控）。
3. **`execution` 四键**——策略层的执行语义：跌停卖/涨停买的处理模式（`queue`=挂涨跌停价排队）、半仓不足一手怎么处理、单票集中度上限。
4. **时间表的跨层校验**——策略时间表与主配置 `machine_schedule`（市场时段、最后报卖、排队撤单截止）自动核对时序，比如"下午退出的补执行截止不能晚于最后报卖时刻"，冲突直接拒绝启动。

由于交易端直接读 QMT 实时行情，这份配置里的每一条风控规则都以亚秒级节奏在真实账户上运行——比"聚宽模拟盘判断、再发信号过来"（约 10 秒延迟）快一个数量级。这是把风控从云端搬到交易端的核心收益，也是这份模板存在的意义。

## 配置说明

主配置 `config.yaml` 分五个主要区域：`redis`、`machine_schedule`、`execution`、`market_data`、`trading`。除了上面必改项，下面这些按你的习惯调：

| 配置 | 说明 |
| --- | --- |
| `execution.quote_band_pct` | 统一挂单包络：所有时段买卖单以 最新价×(1±该值) 挂出（默认 1.5%），成交价仍按对手盘逐档确定；0=原价挂单（不推荐） |
| `execution.pricing_mode` | `slippage`（按上面包络挂单，默认）或 `book`（吃对手盘：买挂卖一、卖挂买一，加减 `book_tick_offset` 个 tick；盘口缺失自动回退 slippage） |
| `execution.book_tick_offset` | `book` 模式下加减的 tick 数（股票 tick=0.01），默认 2 |
| `execution.order_timeout_sec` / `max_attempts` / `max_total_duration_sec` | 单次委托等待、最多委托次数、单条信号总执行预算（示例 2s / 3 次 / 90s） |
| `execution.cancel_confirm_timeout_sec` | 撤单后等真实终态的时长，独立于总预算（示例 60s），别调太小：调小会把普通未成交撤单误判成"状态不明"而熔断 |
| `execution.cash_fee_buffer_pct` | 买入委托金额按 可用资金×(1−该值) 封顶，防"用满资金被柜台以资金不足废单"（默认 0.3%） |
| `execution.signal_expire_seconds` | 信号过期秒数，默认 600（10 分钟），0=不过期 |
| `execution.quote_max_age_sec` | 行情快照时效门控：连续竞价时段快照超过该秒数重取一次，仍超龄带告警提交；0=关闭（建议 3） |
| `execution.opening_aggressive_window_sec` / `opening_order_timeout_sec` | 开盘首挂窗口（默认 60s）与窗口内首笔委托的耐心时长（0=沿用 `order_timeout_sec`；撤单慢的券商建议 10、快的 2） |
| `execution.opening_price_gap_wait_pct` / `opening_price_gap_wait_max_sec` / `opening_price_gap_wait_poll_sec` | 开盘首挂超时后的"价格感知等待"：偏离挂单价 ≤ 阈值就继续等（最多 6s，每 0.2s 重判一次），价格真甩开才撤单追价；任一为 0=关闭 |
| `execution.ghost_order_detect_grace_sec` / `ghost_order_auto_resubmit` | 幽灵单检测：报单受理后超过 N 秒仍不出现在 QMT 委托清单 → 三重校验确认后自动重挂剩余数量；0=关闭（默认），建议 3 |
| `execution.queue_sell_poll_interval_sec` | 跌停排队单的慢轮询间隔（默认 3s；排队单不撤不重挂，无需快轮询） |
| `execution.max_concurrent_queue_sells` / `max_concurrent_queue_buys` | 涨跌停排队并发上限（`qmt-queue` 专用线程池容量=两侧之和；示例各 5） |
| `execution.plan_enabled` / `plan_execute_at` | 日计划开关（默认开）；后者仅兼容旧配置，当前不控制延时 |
| `redis.pending_claim_idle_ms` / `pending_scan_interval_sec` | 其他旧进程的遗留消息超过多久才接管、多久检查一次，默认60秒/5秒；本机同 consumer 重启时立即核对 |
| `redis.block_ms` / `socket_connect_timeout_sec` / `socket_timeout_margin_sec` / `health_check_interval_sec` | 阻塞读与跨网络连接健壮性：BLOCK 是服务端推送，调大不增加收信延迟，只减少空轮询 |
| `config.strategy.yaml` 的 `strategy_engine.external_signals.allowed_sell_purposes` | 聚宽卖出信号的放行白名单：只有 `sell_all` 且 purpose 在名单内的消息被执行，其余同策略旧消息 ACK 拒绝；必填，空列表=拒绝一切外部卖出信号 |
| `config.strategy.yaml` 的 `execution.limit_down_sell_mode` / `limit_up_buy_mode` | 跌停卖/涨停买的处理：`queue`=挂涨跌停价排队、`skip`=跳过、`none`=普通定价（已从主配置迁移；纯跟单模式可写回主配置，引擎模式下主配置携带会被拒绝启动） |
| `config.strategy.yaml` 的 `execution.sell_half_insufficient_lot_mode` | 半仓不足一手时 `sell_all`=全卖 / `skip`=不卖（同上迁移规则） |
| `config.strategy.yaml` 的 `execution.max_single_position_pct` | 单票集中度上限=总资产×比例（同上迁移规则；策略层当持仓上限、执行层当单笔上限，两层共用） |
| `market_data.pre_subscribe_codes` | 启动时预订阅的代码，一般留空（策略会推送股票池） |
| `state_db` / `log_dir` / `log_level` / `log_file_level` | SQLite 账本路径、日志目录、控制台级别、文件级别（热路径的 ⏱️ 耗时日志另有 20ms 阈值门控，与级别无关） |

### 交易机日程：`machine_schedule`

下表的 9 个字段是交易机共享的完整日程契约，在 `config.yaml` 中全部必填。“包含/不包含”指程序在边界时刻是否仍执行对应动作；示例值与 `config.example.yaml` 一致。

| 完整路径 | 中文含义 | 边界 | 默认示例值 |
| --- | --- | --- | --- |
| `machine_schedule.market_session.call_auction_start_at` | A 股集合竞价保护和激进定价开始 | 包含；到点进入竞价时段 | `09:15:00` |
| `machine_schedule.market_session.preopen_sell_start_at` | 盘前普通卖单开始纳入开盘卖出屏障 | 包含；到点即识别 | `09:25:00` |
| `machine_schedule.market_session.continuous_trading_start_at` | 连续竞价开始，买单屏障放行且竞价保护结束 | 开始时刻属于连续竞价，不再属于盘前区间 | `09:30:00` |
| `machine_schedule.market_session.closing_call_auction_start_at` | 尾盘集合竞价开始 | 不包含；涨跌停排队撤单必须在此前完成 | `14:57:00` |
| `machine_schedule.market_session.market_close_at` | 正常收盘边界，用于校验和生命周期判断 | 包含；到点视为已收盘 | `15:00:00` |
| `machine_schedule.order_guard.strategy_sell_last_submit_at` | 本地策略新卖单的最后提交时刻 | 包含；该秒仍可最后报单 | `14:56:00` |
| `machine_schedule.order_guard.limit_down_queue_cancel_at` | 跌停排队卖单撤单收尾时刻 | 到点触发撤单 | `14:56:30` |
| `machine_schedule.order_guard.limit_up_queue_cancel_at` | 涨停排队买单撤单收尾时刻 | 到点触发撤单 | `14:56:30` |
| `machine_schedule.lifecycle.daily_summary_at` | 基于 QMT 真实账户输出日结并关闭策略日 | 到点触发；必须晚于收盘和全部排队撤单 | `15:02:00` |

### 策略日程：`strategy_engine.schedule`（仅本地策略引擎模式）

下表 13 个字段位于本地私有 `config.strategy.yaml`，全部必填且没有代码默认值。表中示例值为**虚构示例值**（与公开模板 `config.strategy.example.yaml` 一致，不代表任何真实部署口径）；改动后必须同时满足策略内部关系和与 `machine_schedule` 的跨层关系。纯跟单模式没有这份配置。

| 完整路径 | 中文含义 | 边界 | 示例值 |
| --- | --- | --- | --- |
| `strategy_engine.schedule.candidate_plan.accept_until` | 候选计划最晚接纳时刻 | 不包含；该秒起不再接纳 | `09:35:00` |
| `strategy_engine.schedule.opening_exit.trigger_at` | 开盘退出首次触发时刻（竞价定盘后立即检查） | 包含；到点可执行 | `09:27:00` |
| `strategy_engine.schedule.opening_exit.retry_until` | 开盘退出重启补执行截止 | 不包含；该秒起不再补执行 | `09:35:00` |
| `strategy_engine.schedule.opening_buy.preopen_start_at` | 第一波盘前挂单窗口起点 | 候选计划送达即接纳，不等此点；此点只约束重启/补挂路径 | `09:25:30` |
| `strategy_engine.schedule.opening_buy.start_at` | 开盘买入开始接纳时刻；第一波未发生时退回整批接纳的起点 | 包含；到点可接纳批次 | `09:30:00` |
| `strategy_engine.schedule.opening_buy.admit_until` | 开盘买入接纳与回款补仓窗口截止 | 不包含；截止前已原子接纳的整批可继续执行 | `09:35:00` |
| `strategy_engine.schedule.hard_stop.start_at` | 盘中硬止损开始检查 | 包含；到点做首次检查 | `09:31:00` |
| `strategy_engine.schedule.hard_stop.last_check_at` | 盘中硬止损最后检查时刻 | 包含；该秒仍检查一次 | `14:56:00` |
| `strategy_engine.schedule.hard_stop.interval_sec` | 盘中硬止损检查间隔，单位秒 | 正数；按上次检查后经过的秒数判断 | `1.0` |
| `strategy_engine.schedule.morning_exit.trigger_at` | 上午退出首次触发时刻 | 包含；到点可执行 | `10:00:00` |
| `strategy_engine.schedule.morning_exit.retry_until` | 上午退出重启补执行截止 | 不包含；该秒起不再补执行 | `10:05:00` |
| `strategy_engine.schedule.afternoon_exit.trigger_at` | 下午退出首次触发时刻 | 包含；到点可执行 | `14:00:00` |
| `strategy_engine.schedule.afternoon_exit.retry_until` | 下午退出重启补执行截止 | 不包含；该秒起不再补执行 | `14:56:00` |

除此之外 `strategy_engine.external_signals.allowed_sell_purposes`（聚宽卖出信号 purpose 白名单）同样必填，代码不内置任何默认值；空列表=拒绝一切外部卖出信号。

两份 YAML 采用一次性严格迁移：新日程字段全部必填，缺字段、多字段、时间格式错误、新旧字段并存或跨层时序冲突都直接拒绝启动；不会自动补默认值、复制旧值或记录警告后继续运行。`config.example.yaml` 是公开模板；`config.strategy.yaml` 及本地 `tests/` 由 `.gitignore` 排除，只用于本机部署和验收，不进入公开提交。

两个经常需要解释的点：

信号过期。发送后 10 分钟内开始执行的信号都会执行，超过 10 分钟还没轮到就丢弃（记 `EXPIRED` 终态并 ACK）。这个秒数按机器单独配，网络慢的机器可以放宽。旧协议里自带 `expire_at` 的消息仍然优先按那个绝对时间判断。

半仓不足一手。`sell_half` 在半仓取整后不足 100 股时，默认把整个持仓卖掉（持仓 100 股就是全卖）；如果改成 `skip`，这种情况就不卖，留到下次 `sell_all` 再处理。

## 系统架构

```text
聚宽云策略
  ├─ publish_candidate_plan_to_redis(...)   盘前推送今日有序候选
  └─ publish_sell_all_to_redis(...)         退出信号触发时下发 sell_all(purpose 白名单可配) 信号
                  │
                  ▼
          Redis Stream（XADD）
                  │
        ┌─────────┴─────────┐
        ▼                   ▼
独立 miniQMT 服务         大 QMT 单文件执行器
  ├─ 本地策略引擎          Redis 消费组
  │  (两波买入/回款补仓/   买卖各一条内存 FIFO
  │   硬止损/定时退出)     运行期 signal_id 去重
  ├─ 买卖并发池+开盘屏障   passorder 下单
  ├─ qmt-queue 排队专用池
  ├─ SQLite 幂等账本
  └─ xtquant 下单
        │                   │
        └─────────┬─────────┘
                  ▼
          Windows QMT / 券商柜台
```

独立服务的主链路：

```text
RedisStreamClient.read_forever()
  → 定期检查本账号消费组的未确认消息，排除当前正在执行与后台展开中的消息
  → 遗留消息先按 signal_id 查询 QMT 订单备注，确认无旧单才允许继续
  → 预订阅指令：立即订阅行情并 ACK
  → 消息日期早于本地今天的旧消息：记日志直接 ACK，不落库不执行
  → [仅本地策略引擎模式] 候选计划：写入 candidate_plans 审计后 ACK，交给本地策略引擎
  → [仅本地策略引擎模式] 引擎按日程 tick：从 QMT 批量读取真实资产/持仓/行情，
    纯规则生成内部 fixed_budget / sell_all / sell_half 信号
  → 交易信号：按方向进买入/卖出并发线程池
  → `machine_schedule.market_session.preopen_sell_start_at`（示例 `09:25:00`）至 `machine_schedule.market_session.continuous_trading_start_at`（示例 `09:30:00`）的盘前卖单先登记预挂，买单等开盘屏障
  → SQLite 幂等门（signal_id 主键）
  → 拉最新行情、可用资金/可卖持仓
  → 计算委托价（竞价排队 / 盘口 / 滑点）
  → 下单并轮询状态
  → 超时撤单，等真实终态，核对撤单期间成交
  → 有剩余就刷新行情和资源重挂
  → 涨跌停排队单落库后移交 qmt-queue 专用线程池慢轮询
  → 写终态，XACK
```

纯跟单模式就是上面这条链路去掉标注的两行"本地策略引擎"——聚宽信号直接进线程池执行，没有任何本地决策层。

本地策略协调层（引擎模式）位于 Redis 与既有订单执行状态机之间：候选计划先写入 `candidate_plans` 后 ACK；调度器从 QMT 批量读取真实资产、持仓与行情，纯规则生成确定性的内部 `fixed_budget/sell_all/sell_half` 信号；`strategy_days` 记录当日是否可买和熔断原因，`strategy_events` 保证每条规则每只股票只触发一次。内部订单仍进入原有 SQLite `signals/order_attempts`、撤单确认、重挂与 QMT 备注恢复链路。决策细节见「进阶：本地策略引擎」一章。

执行端有几点设计是故意的，值得知道：

- 不做"价格偏离参考价就拒单"。本端是跟单器，拦下一笔单，实盘持仓就和聚宽模拟盘永远对不上，而且没人会补发。价格风险靠实时盘口定价、涨跌停夹取和 ±10% 涨跌停带兜底，择时选股是策略的事。
- 撤单必须确认。撤单请求发出去不等于撤了，只有查到已成、已撤或废单才算数，然后按撤单期间的成交核对剩余数量。
- 状态不明就熔断。下单是否受理、撤单是否成功都确认不了的时候，停止后续所有交易，等人去 QMT 里对账。
- 跌停卖单走排队。确认跌停锁盘后挂跌停价排队等开板，期间不撤不重挂（重挂丢队列位置），到 `machine_schedule.order_guard.limit_down_queue_cancel_at` 才收尾。涨停买单使用对应的 `limit_up_queue_cancel_at`。

### 自保机制：为什么它敢全自动下单

上面四条是"拿不准就停下"的底线。下面这些是 2026-08 连续复盘后加的"少犯傻"机制，全部有开关、默认安全：

- **幽灵单检测**：`order_stock` 秒回单号、但订单从未出现在 QMT 委托清单（通道坏死、柜台没收到）时，旧代码会把它当成排队单傻等。现在超过 `ghost_order_detect_grace_sec`（建议 3，默认 0=关闭）仍不可见就做三重校验——直查委托、查冻结资金、查持仓成交痕迹——三查全空确认幽灵单后自动重挂剩余数量；连续重挂超 2 次熔断防刷单。任一有疑则保持熔断等人工对账。
- **价格拒单不重蹈覆辙**：被柜台以"委托价不正"拒掉后，重挂前强制刷新行情（上限 2s）并改按盘口锚定报价（买挂卖一、卖挂买一），不再用同一份旧行情反复撞墙。
- **开盘首挂有耐心**：开盘窗口内首笔买单超时未成交先看价——偏离挂单价 ≤ 阈值（默认 1%）就每 0.2s 重判继续等（最多 6s），价格真甩开才撤单追价，避免"回报迟到几秒"被误撤。
- **旧消息日期门控**：隔天开机时，消费组里昨天没读完的消息会被当新消息重放。`candidate_plan` 落 LATE 审计不执行；日计划和普通信号在派发前检查消息日期，早于本地今天就直接 ACK，不进引擎不下单。
- **排队单不占坑**：跌停排队卖、涨停排队买在挂单落库后移交 `qmt-queue` 专用线程池慢轮询（默认 3s 一次），买卖 worker 不会被全天占用；优雅退出时排队单主动撤单收口，重启按 QMT 订单备注恢复跟踪。
- **收信主循环减负**：订阅行情、查中文名、日计划展开都移到后台线程，收信处理 P95 目标 <50ms；聚宽发送端 XADD 带 2s 读写超时 + 3 次重试，Redis 抖动不丢信号（signal_id 不变，重复消息被幂等去重）。
- **行情与快照防呆**：行情取数失败退避重试 3 次；连续竞价时段快照超过 `quote_max_age_sec` 强制重取，防止开盘第一秒用 9:25 的旧盘口报价。

## 信号协议

Redis Stream 每条消息用字段 `payload` 装 JSON。精确买卖信号：

```json
{
  "signal_id": "strategy-a-20260713093001-510300XSHG-buy-1000",
  "strategy_id": "strategy_a",
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

本地策略引擎模式（进阶）使用的候选计划不携带任何交易参数：

```json
{
  "action": "candidate_plan",
  "schema_version": 1,
  "plan_id": "strategy-a-20260812-candidates",
  "strategy_id": "strategy-a",
  "mode": "live",
  "trading_date": "2026-08-12",
  "candidates": ["600000.XSHG", "000001.XSHE"],
  "strategy_version": "local-engine-v1",
  "created_at": "2026-08-12 09:25:45",
  "sent_at_ms": 1786497945000
}
```

候选代码保持有序且不能重复，空数组合法。相同 `plan_id` 重投必须内容完全一致；同 ID 改内容或同一交易日出现第二个 ID 会把策略日置为 `PLAN_CONFLICT`，当天禁止新买入，但已有持仓的止损和定时退出仍由 QMT 端继续管理。

| 字段 | 说明 |
| --- | --- |
| `signal_id` | 幂等键，默认由策略、时间、代码、方向、数量组成 |
| `strategy_id` | 信号来源 |
| `mode` | 只有 `live` 会进入真实执行路径 |
| `action` | `candidate_plan`（本地引擎选股输入）/ `buy` / `sell`（精确）或 `plan` / `sell_half` / `sell_all`（意图） |
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
  "signal_id": "strategy-a-20260806-plan",
  "strategy_id": "strategy_a",
  "mode": "live",
  "action": "plan",
  "codes_to_sell": ["000001.XSHE"],
  "codes_to_buy": ["600000.XSHG", "000002.XSHE"],
  "created_at": "2026-08-06 09:30:01",
  "sent_at_ms": 1786044480000
}
```

盘中 `sell_half` / `sell_all` 不带 `amount`，数量按真实可卖持仓算；`buy` 不带 `amount` 时按 `min(可用资金÷待买只数, 总资产×max_single_position_pct)` 计算整手。旧的 `buy`/`sell` + `amount` 协议完全兼容。

### 本地策略引擎的内部信号（仅引擎模式）

这些信号不经过 Redis，由交易机自己生成、自己执行，走同一条执行链路（幂等、撤单确认、重挂）。`signal_id` 稳定可预测，重启恢复靠它和 QMT 订单备注对上账：

| 用途 | signal_id 格式 | 数量语义 |
| --- | --- | --- |
| 开盘买入第一波 | `{策略id}-{YYYYMMDD}-opening-buy-{code}` | `fixed_budget`：按预算金额在报价包络内尽量成交 |
| 回款补仓第 N 波 | `{策略id}-{YYYYMMDD}-topup{NN}-{code}` | 同上；波次编号从当日已落库信号数重建 |
| 规则卖出 | `{策略id}-{YYYYMMDD}-{规则名}-{code}` | `sell_all` / `sell_half`，按真实可卖持仓计算 |

内部信号还携带 `purpose`（建仓 / 补仓 / 开盘止损 / 止损 / 卖半锁盈 / 清仓 / 回落止盈）用于日志与对账，第一波买入带 `preopen_submit` 盘前挂单标记。`purpose` 同时是专用账户模式下放行聚宽卖出信号的白名单依据：只有 `sell_all` 且 purpose 出现在 `config.strategy.yaml` 的 `external_signals.allowed_sell_purposes` 里的消息会执行，同策略的其他旧消息一律 ACK 拒绝，防止双重决策。

## 项目结构

```
.
├── main.py                     # 交易机启动入口（python main.py）
├── config.example.yaml         # 公开配置模板
├── config.strategy.example.yaml # 公开策略配置模板（虚构示例值）
├── config.yaml                 # 本机配置（不入库）
├── config.strategy.yaml        # 本机私有策略配置（不入库，专用账户模式必填）
├── joinquant_signal_sender.py  # 聚宽侧发送函数（粘进策略用，占位配置）
├── miniqmt_follower/           # 独立服务运行时
│   ├── app.py                  # 装配：收发信、线程池、白名单、日期门控
│   ├── executor.py             # 订单执行状态机（下单/撤单/重挂核心）
│   ├── strategy_engine.py      # 本地策略引擎（按日程决策）
│   ├── strategy_rules.py       # 纯决策规则（无副作用，可单测）
│   ├── strategy_config.py      # config.strategy.yaml 的严格解析与校验
│   ├── pricing.py / sizing.py  # 定价 / 数量计算
│   ├── store.py                # SQLite 八表账本
│   ├── redis_stream.py         # Redis Stream 消费组封装
│   ├── opening.py              # 开盘卖单屏障
│   ├── adapters/qmt.py         # QMT 行情/交易适配器
│   ├── recovery_cli.py         # RECOVERY_REQUIRED 人工对账工具
│   └── config.py / models.py / logging_config.py / process_lock.py ...
├── bigqmt_follower/            # 大 QMT 单文件备用执行器
├── deploy/                     # 各交易机的部署配置对（config.yaml + config.strategy.yaml）
├── tools/                      # 复盘脚本（委托重挂分布、撤单耗时、行情新鲜度）
├── docs/                       # 链路图（Excalidraw）、注释规范
└── tests/                      # 本地测试套件（不入库）
```

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
- [ ] 交易机 A 和 B 使用同一 Stream、不同 group、不同 consumer、不同 account_id 和 SQLite
- [ ] 运行模式与本机预期一致：本地策略引擎模式要有 `config.strategy.yaml`，纯跟单模式不要有该文件（启动 banner 会打印当前模式）
- [ ] 同一资金账户只有一个执行进程（重复启动会被账本锁拒绝）
- [ ] Windows、聚宽、Redis 时间同步（延迟日志和过期判断都依赖它）
- [ ] 券商行情字段、资金/持仓字段、订单状态和撤单返回值核对过
- [ ] `config.strategy.yaml` 关键值确认：跌停/涨停模式为 `queue`、排队撤单截止 `14:56:30`、排队容量两侧各 5、`trading.enabled` 已人工确认
- [ ] 新机制开关按本机券商行为确认：幽灵单检测宽限、开盘首挂耐心与价格等待
- [ ] 仿真盘跑过单笔买卖、拒单、部分成交、超时撤单、批量信号
- [ ] SQLite、QMT 委托、Redis pending/ACK、日志四条线能对得上
- [ ] 实盘第一次启用用小额订单，人工盯盘

## 本地验证

`tests/` 是本地测试套件，不随仓库分发（里面可能有依赖私有策略副本的用例）。在配好本地副本的机器上：

```bash
python -m pip install -r requirements-dev.txt
python -m unittest discover -v
python -m compileall miniqmt_follower bigqmt_follower tests
```

测试全部用 stdlib unittest 注入假对象，不需要真实 Redis、QMT 或网络，macOS/Linux 上照样跑。非 Windows 机器会有一批 skip——`xtquant.xttrader` 的本地二进制只随 Windows 版 miniQMT 分发，那是环境限制不是缺陷。

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

## 术语速查

| 术语 | 一句话解释 |
| --- | --- |
| 信号（signal） | Redis Stream 里的一条 JSON 消息，`signal_id` 是它的身份证 |
| 幂等 | 同一个 `signal_id` 只会真正下一笔单，重复消息被直接忽略 |
| 终态 | 信号执行完成后的最终状态（成交/部分成交/跳过/失败/过期），写入 SQLite 后才 ACK |
| 开盘屏障 | 9:25–9:30 之间普通卖单先挂、买单全部等待；卖单完成或终止后才放行买单 |
| 排队单 | 挂在涨跌停价的委托，只排队不重挂，交给 `qmt-queue` 线程池慢轮询 |
| 候选计划（candidate_plan） | 聚宽盘前发来的今日选股清单，只有代码、没有交易参数；仅本地策略引擎模式消费 |
| 纯跟单模式 | 没有 `config.strategy.yaml` 的运行模式：不本地决策，白名单策略发什么就跟什么 |
| 回款补仓 | 开盘后每笔卖款到账够一手，就对候选股补买一波，直到窗口收口 |
| 第一波基线 | 第一波买入时的可用资金快照，补仓池 = 当前可用资金 − 基线 |
| 熔断 | 拿不准就停：确认不了订单状态时停止后续所有交易，等人工对账 |
| 幽灵单 | 本地报单成功、柜台却从未收到的订单；三重校验后自动重挂或熔断 |
| 消费组（group） | Redis Stream 的进度账本；一台机器一个组，同组会瓜分消息 |
| ACK | 确认消息已处理完成；只有写到终态才 ACK，崩溃会重投 |

## 常见问题

### 启动时报 `trading.enabled is false`

安全门正常。账号、路径、QMT 登录、仿真盘都确认过，再改成 `true`。

### 信号显示 EXPIRED，没下单

信号发出 10 分钟（默认）内没开始执行就会被丢弃。先看执行端是不是一直没轮到它（队列积压、worker 不够），再看机器时间是否同步，最后确认 `signal_expire_seconds` 没被改小。

### 资金或持仓查询失败

执行端会拒单。恢复 QMT 连接、核对账号字段，不要用默认资金或默认持仓绕过。

### 撤单终态未确认

执行端会熔断。去 QMT 委托列表人工确认还有没有活动订单，状态不明时不要直接重启重发。

### 重启后如何处理旧 pending

程序重启时会立即拿回同 consumer 上次留下的未确认消息；其他旧 consumer 的消息超过 `pending_claim_idle_ms` 后才接管。恢复时先用由 `signal_id` 算出的 24 位稳定备注查 QMT 旧单（避免 miniQMT 截断长备注）：找到旧单就继续核对，不会直接重下；信号已开始处理但 QMT 查不到对应单、QMT 查询失败、或同一编号出现多笔活动订单时，会标记 `RECOVERY_REQUIRED`、停止该账号继续下单并保留 Redis pending。

出现 `RECOVERY_REQUIRED` 后：先停止该账号的跟单程序，在 QMT 委托/成交列表按 `signal_id` 备注核对，确定实际成交数量和最终状态。确认完成后才运行：

```powershell
python -m miniqmt_follower.recovery_cli `
  --state-db <该账号的db路径> `
  --signal-id <日志里的signal_id> `
  --status filled `
  --filled-qty <实际成交股数> `
  --confirm-qmt-reconciled
```

`--status` 可用 `filled`、`partially_filled_timeout`、`failed_timeout`、`failed_broker`。工具会检查跟单程序已停止，且只允许处理 `RECOVERY_REQUIRED`；然后重启跟单程序，它会确认这条 Redis 消息。不能手工重发交易信号。

### 报单返回了单号，但订单一直不出现（幽灵单）

通道坏死或柜台没收到时会出现：本地有单号、QMT 委托清单里始终查不到。超过 `ghost_order_detect_grace_sec` 后引擎会三重校验（直查委托 / 冻结资金 / 持仓成交痕迹），三查全空就确认幽灵单并自动重挂剩余数量；连续重挂超 2 次熔断防刷单。日志里出现"三查有疑"就先停机，按 `RECOVERY_REQUIRED` 流程人工对账。

### 隔天开机后，昨天的候选计划/信号被重放

正常。消费组会把昨天没读完的消息当新消息重放，程序有日期门控：`candidate_plan` 只落审计（状态 LATE，不执行），昨天的日计划和普通信号直接 ACK。看到 "⏳ 候选计划未采纳｜仅审计落库" 或 "昨日日计划已忽略" 说明门控生效，无需处理。

### 同一笔委托被柜台反复拒（如 "20009:委托价不正"）

价格类拒单重挂前会强制刷新行情（上限 2s）并按盘口锚定报价（买挂卖一 / 卖挂买一）。如果还在连拒，检查行情源是否冻结、涨跌停价是否缺失，以及 `quote_band_pct` 是否配得过大。

### 排队单为什么一直不撤也不动

跌停卖出 / 涨停买入挂的是涨跌停价排队，重挂会丢队列位置，所以排队期间不撤不重挂，到 `14:56:30` 统一收口。期间它占用的是 `qmt-queue` 专用线程池（慢轮询，默认 3s 一次），不挡其他买卖。

### 日志里出现 BLOCK / BLOCKED_DATA（仅本地策略引擎模式）

策略引擎发现行情太旧、缺涨跌停价、快照日期不对等数据问题时，会阻塞对应规则而不是带病决策。等下一 tick 数据恢复会自动继续；若持续 BLOCK，检查行情源连接和 `data_safety` 配置。

### 怎么切换成纯跟单模式

删除 `main.py` 同级的 `config.strategy.yaml` 后重启即可，不用改任何代码。启动 banner 的"运行模式"会变成"纯跟单"；此时白名单策略的 `plan`/`buy`/`sell`/`sell_half`/`sell_all`/`watchlist` 消息全部照常执行，`candidate_plan` 会被忽略。想回到本地策略引擎模式，把文件放回去再重启。

## 相关文档

- [大 QMT Redis 信号执行端说明](bigqmt_follower/README.md)
- [AGENTS.md](AGENTS.md)：面向代码助手的架构和信号契约说明
- [docs/注释规范.md](docs/注释规范.md)：全仓库 Python 代码的注释与 docstring 标准
- [docs/diagrams/](docs/diagrams/)：完整执行链路与策略流水线的 Excalidraw 图（可编辑，本机维护不入库）
- [tools/](tools/)：复盘脚本——`analyze_attempts.py` 委托终态/重挂分布、`analyze_cancel_latency.py` 撤单确认耗时、`quote_freshness_probe.py` 行情新鲜度

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

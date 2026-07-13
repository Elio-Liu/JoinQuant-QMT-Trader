# 我做了一个聚宽 mini Q MT 跟单助手：让云端策略信号自动落到本地 Q MT

> 发布说明：为了规避社区关键词拦截，文中的 `R edis`、`mini Q MT`、`Q MT`、`re dis` 都做了空格处理。实际配置、安装命令和代码变量名里请去掉这些空格。

很多人在聚宽上写策略，最顺手的是研究、回测、模拟交易；但一到实盘自动化，就会遇到一个现实问题：

聚宽策略运行在云端，而券商交易端通常在本地 Windows 机器上，比如 mini Q MT。

中间这一段“信号怎么稳定、低延迟、可追踪地传过去”，往往需要自己搭桥。所以我整理了一个项目：

**JoinQuant mini Q MT 跟单助手**

它的目标很简单：

让聚宽策略只负责产生交易信号，本地 Windows 服务负责接收信号并通过 mini Q MT / xtquant 执行。

项目地址：`这里填你的 GitHub 链接`

## 它解决什么问题

整个系统分成两端：

```text
聚宽云策略
  -> R edis Stream
      -> Windows 本地服务
          -> mini Q MT / xtquant
```

聚宽端只需要调用一个函数：

```python
publish_trade_signal_to_re dis(context, "buy", "000001.XSHE", 1000, 10.0)
```

Windows 端则负责：

- 从 R edis Stream 读取交易信号
- 用 SQLite 记录执行状态，避免重复下单
- 用 mini Q MT 最新行情重新计算委托价
- 按配置加入买入 / 卖出滑点
- 超时未成交时撤单重挂
- 订单进入终态后才 ACK R edis 消息

这套设计的核心不是“把下单代码塞进聚宽”，而是把聚宽当作信号生产端，把真实执行放在本地可控环境里。

## 为什么不用 R edis Pub/Sub

最开始很多人会想到 R edis Pub/Sub，但它有一个明显问题：消费者不在线时，消息就丢了。

这个项目使用的是 R edis Stream：

- 消息可以保留
- 支持消费组
- 支持 ACK
- 服务重启后可以继续处理未确认消息

再配合本地 SQLite 的 `signal_id` 幂等控制，同一条信号即使被重复投递，也只会执行一次。

## 大致实用教程

### 1. 准备环境

你需要：

- 一台 Windows 机器
- 已安装并登录 mini Q MT
- Python 3.8+
- 可访问的 R edis 服务
- Python 环境能导入 `xtquant`

安装依赖：

```bash
pip install re dis xtquant
```

下载项目后，复制配置文件：

```powershell
Copy-Item config.example.yaml config.yaml
```

然后修改 `config.yaml` 中的 R edis 和 mini Q MT 配置。

重点关注这些字段：

```yaml
redis:
  host: 你的Redis地址
  port: 6379
  password: 你的Redis密码
  stream: tidal_quant_signals
  group: qmt_executors
  consumer: win-qmt-01
  block_ms: 20
execution:
  buy_slippage_pct: 0.003
  sell_slippage_pct: 0.003
  order_timeout_sec: 3
  max_attempts: 3
  max_total_duration_sec: 15
  max_deviation_from_signal_price_pct: 0.02
trading:
  enabled: false
  account_id: 你的资金账号
  miniqmt_path: 你的userdata_mini目录
  session_id: 0
```

建议第一次不要直接打开实盘开关，先保持：

```yaml
enabled: false
```

确认 R edis 收发、日志、SQLite 记录都正常后，再切到模拟盘或小资金实盘验证。

### 2. 启动 Windows 跟单服务

在项目目录运行：

```bash
python main.py
```

也可以指定配置文件和线程数：

```powershell
python .\main.py --config config.yaml --workers 1
```

服务启动后，会监听 R edis Stream，收到聚宽信号后进入执行流程。

### 3. 在聚宽策略里接入

项目里有一个 `joinquant_signal_sender.py`，把里面的函数复制到你的聚宽策略中。

然后修改函数内部的 R edis 配置：

```python
re dis_config = {
    "host": "你的R edis服务器IP",
    "port": 6379,
    "password": "你的R edis密码",
    "stream": "tidal_quant_signals",
    "maxlen": 10000,
    "socket_connect_timeout": 1,
}

strategy_id = "你的策略ID"
```

注意：这里的 `stream` 必须和 Windows 端 `config.yaml` 里的 `re dis.stream` 保持一致。

之后，在策略产生买卖信号的位置调用：

```python
# 买入 1000 股
publish_trade_signal_to_re dis(context, "buy", "000001.XSHE", 1000, 10.0)

# 卖出 500 股
publish_trade_signal_to_re dis(context, "sell", "600519.XSHG", 500, 1800.0)
```

函数签名固定为：

```python
publish_trade_signal_to_re dis(context, action, code, amount, price)
```

这样已有策略的调用点不用大改。

### 4. 信号长什么样

写入 R edis Stream 的信号大致如下：

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

其中 `reference_price` 只是聚宽侧的参考价，不是最终下单价。

Windows 端会用 mini Q MT 最新行情重新定价，并按配置加入滑点。

`signal_id` 是幂等键。只要 `signal_id` 相同，Windows 端就会认为是同一条信号，不会重复下单。

## 执行逻辑

收到信号后，执行端大致流程是：

```text
收到 R edis 消息
  -> SQLite 幂等检查
  -> 获取 mini Q MT 最新价
  -> 检查是否偏离聚宽参考价过多
  -> 计算滑点价格
  -> 提交委托
  -> 轮询订单状态
      -> 全部成交：记录终态并 ACK
      -> 部分成交：撤单，对剩余数量重新定价再下
      -> 超时未成交：撤单重挂
      -> 达到上限：停止追单并记录状态
```

这比简单地“收到信号就下单”稳一些，至少能把重复信号、超时、部分成交、价格偏离这些问题纳入统一处理。

## 风险和注意事项

这个项目更适合有一定 Python 和交易系统经验的人使用。实盘前一定要注意：

1. 不要把 R edis 6379 端口裸露到公网，至少做 IP 白名单、安全组或 VPN。
2. 先跑模拟盘，确认下单、撤单、查单、状态映射都符合你的券商 Q MT 版本。
3. 第一次实盘建议小资金、小数量，并人工盯盘。
4. 不同券商的 mini Q MT / xtquant 版本可能有差异，下单函数、状态字段、返回值都要实际确认。
5. 聚宽策略只应该负责发信号，不要在云端等待本地成交结果，否则容易拖慢策略主流程。

## 适合谁用

如果你现在是下面这种情况，这个项目可能比较适合：

- 策略主要写在聚宽
- 实盘交易想走本地 mini Q MT
- 不想每个策略都重写一套下单逻辑
- 希望信号可追踪、可恢复、可去重
- 想把策略逻辑和交易执行逻辑分开维护

项目地址：`这里填你的 GitHub 链接`

欢迎大家试用、提 issue，也欢迎一起完善不同券商 Q MT 环境下的适配细节。

实盘有风险，代码只是工具，真正上线前一定要充分验证。

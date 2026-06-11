# Windows miniQMT Adapter Handoff Plan

本文档给 Windows 环境中的后续 agent 使用。当前 Mac 环境无法安装和运行 miniQMT/xtquant，因此本仓库已经完成可在 Mac 上验证的部分，真实交易适配需要在 Windows + miniQMT 环境继续。

## 当前已完成

- 聚宽策略已集成 Redis Stream 信号发送:
  - `strategies/etf_discount_live_signal_strategy.py`
  - 函数签名保持 `publish_trade_signal_to_redis(context, action, code, amount, price)`
  - Redis client 已缓存，避免每次信号新建连接
- 后端启动入口:
  - `python main.py`
- Redis Stream 消费:
  - `qmt_follower/redis_stream.py`
  - `redis.block_ms` 默认 20ms，用于开盘快速响应
- SQLite 本地账本:
  - `qmt_follower/store.py`
  - 已启用 WAL + `synchronous=NORMAL`
- 执行状态机:
  - `qmt_follower/executor.py`
  - 支持严格幂等、固定滑点、超时撤单、剩余数量重挂
- QMT 适配边界:
  - `qmt_follower/adapters/qmt.py`
  - 当前 `QmtBrokerAdapter` 故意抛错，防止误以为已能实盘下单

## Windows 环境准备

1. 安装并登录 miniQMT。
2. 确认 miniQMT 客户端能正常手工下单、撤单、查委托。
3. 确认 Python 环境能导入:

```bash
python -c "import xtquant; print('xtquant ok')"
python -c "from xtquant import xtdata; print('xtdata ok')"
python -c "from xtquant.xttrader import XtQuantTrader; print('xttrader ok')"
```

4. 安装项目依赖:

```bash
pip install redis
```

5. 复制配置:

```bash
copy config.example.json config.json
```

6. 设置 Redis 密码环境变量:

```bat
set REDIS_PASSWORD=你的Redis密码
```

## miniQMT API 待确认

后续 agent 在写真实适配器前，先在 Windows 上确认以下问题，不要凭空假设。

### 账号和连接

- 账号类型: 普通股票账户还是信用账户。
- 资金账号字符串格式。
- miniQMT 安装路径或 session 路径。
- `XtQuantTrader` 初始化参数。
- `trader.start()`、`trader.connect()`、`trader.subscribe(account)` 的返回值和异常行为。

### 代码格式

聚宽信号代码通常是:

```text
510300.XSHG
159915.XSHE
```

miniQMT 常见格式可能是:

```text
510300.SH
159915.SZ
```

必须实现并测试转换函数:

```python
def jq_code_to_qmt_code(code: str) -> str:
    if code.endswith(".XSHG"):
        return code.replace(".XSHG", ".SH")
    if code.endswith(".XSHE"):
        return code.replace(".XSHE", ".SZ")
    return code
```

### 下单 API

确认你的 xtquant 版本中下单函数的真实签名，例如:

```python
trader.order_stock(account, stock_code, order_type, order_volume, price_type, price, strategy_name, order_remark)
```

必须确认:

- 买入常量名称。
- 卖出常量名称。
- 限价委托常量名称。
- 返回值是订单 ID、错误码，还是需要从回调中拿订单编号。
- 下单失败时是返回负数、None，还是抛异常。

### 查单 API

必须确认:

- 查询全部委托的函数名。
- 返回对象字段名:
  - 订单编号
  - 证券代码
  - 委托数量
  - 已成交数量
  - 委托状态
  - 价格
- 部分成交、完全成交、已撤、废单分别对应哪些状态值。

### 撤单 API

必须确认:

- 撤单函数名。
- 撤单需要传入订单编号、系统委托号，还是证券代码 + 委托编号。
- 撤单成功和失败的返回值。

## 需要实现的文件

主要修改:

```text
qmt_follower/adapters/qmt.py
```

建议新增测试:

```text
tests/test_qmt_adapter_mapping.py
```

可选新增本地探针脚本:

```text
scripts/probe_xtquant_api.py
scripts/probe_qmt_account.py
```

## QmtBrokerAdapter 实现目标

`QmtBrokerAdapter` 必须实现 `executor.py` 中的 `BrokerAdapter` 协议:

```python
def submit_order(self, signal: TradeSignal, quantity: int, price: float) -> str:
    ...

def get_order_snapshot(self, order_id: str) -> OrderSnapshot:
    ...

def cancel_order(self, order_id: str) -> None:
    ...
```

### 建议结构

```python
class QmtBrokerAdapter:
    def __init__(self, account_id: str, miniqmt_path: str, session_id: int, strategy_name: str = "tidal_quant"):
        self.account_id = account_id
        self.strategy_name = strategy_name
        self.trader = XtQuantTrader(miniqmt_path, session_id)
        self.account = StockAccount(account_id)
        self.trader.start()
        connect_result = self.trader.connect()
        if connect_result != 0:
            raise QmtAdapterNotConfigured(...)
        self.trader.subscribe(self.account)

    def submit_order(self, signal, quantity, price):
        qmt_code = jq_code_to_qmt_code(signal.code)
        order_type = STOCK_BUY if signal.action == Action.BUY else STOCK_SELL
        order_id = self.trader.order_stock(
            self.account,
            qmt_code,
            order_type,
            int(quantity),
            FIX_PRICE,
            float(price),
            self.strategy_name,
            signal.signal_id,
        )
        if not order_id or int(order_id) < 0:
            raise RuntimeError(f"QMT下单失败: {order_id}")
        return str(order_id)

    def get_order_snapshot(self, order_id):
        orders = self.trader.query_stock_orders(self.account)
        target = find_order_by_id(orders, order_id)
        return qmt_order_to_snapshot(target)

    def cancel_order(self, order_id):
        result = self.trader.cancel_order_stock(self.account, int(order_id))
        if result not in SUCCESS_VALUES:
            raise RuntimeError(f"QMT撤单失败: {result}")
```

以上是结构示例，不是可直接复制的最终代码。后续 agent 必须按 Windows 实际 xtquant API 调整。

## 状态映射要求

将 QMT 委托状态映射为内部状态:

```python
BrokerOrderStatus.OPEN
BrokerOrderStatus.PARTIALLY_FILLED
BrokerOrderStatus.FILLED
BrokerOrderStatus.CANCELED
BrokerOrderStatus.REJECTED
```

最低要求:

- 未成交、已报、待报 -> `OPEN`
- 部成 -> `PARTIALLY_FILLED`
- 已成 -> `FILLED`
- 已撤 -> `CANCELED`
- 废单、拒单、失败 -> `REJECTED`

`filled_qty` 必须是该订单自身的累计成交数量，不是整个信号累计成交数量。

## 低延迟验收标准

先不要承诺“0.5s 内成交”。验收目标应是:

```text
Redis 收到信号 -> QMT submit_order 返回订单号 <= 500ms
```

建议打点:

```text
t0: Redis message read
t1: signal parsed
t2: idempotency accepted
t3: latest price read
t4: submit_order called
t5: order_id returned
```

日志示例:

```text
latency signal_id=... redis_to_order_ms=138 price_ms=3 submit_ms=91
```

## Windows 端测试顺序

1. 只测试代码转换:

```bash
python -m unittest tests.test_qmt_adapter_mapping -v
```

2. 只连接 miniQMT，不下单:

```bash
python scripts/probe_qmt_account.py
```

3. 查询行情:

```bash
python -c "from qmt_follower.adapters.qmt import QmtMarketDataAdapter; print(QmtMarketDataAdapter().latest_price('510300.SH'))"
```

4. 使用极小数量或测试账户做一笔手动 dry-run/模拟交易。

5. 启动后端:

```bash
python main.py
```

6. 从 Redis 手工写入一条测试信号，确认:

- SQLite 出现 `signals` 记录。
- `order_attempts` 出现订单记录。
- miniQMT 委托列表出现对应委托。
- 成交/撤单/重挂状态更新正确。

## 风控和安全开关

真实接入前建议新增:

- `trading.enabled`: 默认 `false`，必须手动开启。
- `trading.account_id`: 资金账号。
- `trading.min_qmt_path`: miniQMT 路径。
- `risk.max_single_order_value`: 单笔最大金额。
- `risk.allowed_symbols`: 白名单。
- `risk.open_time_only`: 是否只允许交易时间下单。

未完成这些开关前，不建议直接接大资金账户。

## 当前不可在 Mac 完成的事项

- 导入并验证 `xtquant.xttrader`。
- miniQMT 登录、连接、订阅账号。
- 真实下单、撤单、查委托。
- 委托状态字段映射。
- 券商返回码和异常行为确认。
- 真实延迟测量。

## 给后续 Agent 的第一句话

请在 Windows + miniQMT 环境中继续实现 `qmt_follower/adapters/qmt.py`。先运行 `docs/windows-qmt-adapter-handoff.md` 中的探针步骤确认 xtquant API，再实现 `QmtBrokerAdapter.submit_order/get_order_snapshot/cancel_order`，不要改变 `OrderExecutionEngine` 的外部行为。完成后必须用小资金或模拟账户验证 Redis 信号到 miniQMT 委托的端到端链路。

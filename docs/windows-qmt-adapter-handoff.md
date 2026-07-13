# Windows miniQMT 部署验证清单

`QmtBrokerAdapter` 已实现连接、订阅、行情、资金/持仓查询、下单、查单和撤单，并已由用户在一个 miniQMT 模拟账户完成批量交易验证。不同券商版本的 `xtquant` 字段、状态和返回码仍可能不同，每个目标环境上线前都要执行本清单。

## 安全前提

1. miniQMT 已登录目标模拟账户，且能手工下单、撤单、查委托。
2. 同一 Redis 消费组只运行一个交易执行端。
3. `trading.enabled=false`，完成只读检查后再临时开启模拟盘。
4. 本地 `config.yaml`、策略部署文件、Redis 密码不进入 Git。

## 环境检查

```powershell
python -c "import xtquant; print('xtquant ok')"
python -c "from xtquant import xtdata; print('xtdata ok')"
python -c "from xtquant.xttrader import XtQuantTrader; print('xttrader ok')"
python -m compileall qmt_follower scripts tests
python -m unittest discover -v
```

复制并编辑配置：

```powershell
Copy-Item config.example.yaml config.yaml
python .\main.py --config config.yaml --workers 1
```

启动日志至少应出现：

```text
【系统】🚀 QMT跟单助手启动中 | 交易线程 1 | FIFO
【QMT】🔌 交易端已连接
【系统】🟢 Redis监听已启动
```

## 当前执行约束

- 所有交易信号共用一个单工作线程，严格 FIFO。
- 每次委托前重新获取行情以及可用资金/可卖持仓。
- 买入数量按当前委托价计算，并向下取整到 100 股。
- 卖出数量不超过 miniQMT 返回的可卖持仓。
- 资源查询失败时不向券商提交订单。
- 撤单返回 0 仅表示请求受理；必须继续查询到真实终态。
- 撤单状态长时间无法确认时，当前进程锁停后续交易。

## 订单状态核对

目标 miniQMT 构建必须与以下语义一致：

| QMT 状态 | 内部状态 | 是否终态 |
| --- | --- | --- |
| 未报、待报、已报、已报待撤 | `OPEN` | 否 |
| 部成、部成待撤 | `PARTIALLY_FILLED` | 否 |
| 已成 | `FILLED` | 是 |
| 部撤、已撤 | `CANCELED` | 是 |
| 废单 | `REJECTED` | 是 |

重点确认 `ORDER_REPORTED_CANCEL`（已报待撤）和 `ORDER_PARTSUCC_CANCEL`（部成待撤）不能被当成已撤，否则会在旧订单仍可能成交时重复挂单。

## 模拟盘验证顺序

### 只读阶段

1. 启动服务但保持 `trading.enabled=false`，确认配置安全门生效。
2. 使用目标 Python 环境检查行情字段 `lastPrice`、`askPrice`、`bidPrice`。
3. 确认持仓对象提供 `can_use_volume` 或 `m_nCanUseVolume`。
4. 确认资产对象提供可用资金字段。

### 小额单笔阶段

设置脚本所需环境变量：

```powershell
$env:QMT_REDIS_HOST="Redis地址"
$env:QMT_REDIS_PORT="6379"
$env:QMT_REDIS_PASSWORD="Redis密码"
$env:QMT_REDIS_STREAM="tidal_quant_signals"
```

核对 `scripts/send_manual_signal.py` 的代码、方向、数量和当前参考价，然后运行：

```powershell
python .\scripts\send_manual_signal.py
```

检查：

- `signals` 账本立即出现信号记录。
- `order_attempts` 记录 QMT 委托号、价格、数量和最终成交量。
- 终端交易行使用 `【买单】`/`【卖单】` 单标签。
- 委托成交后 Redis 消息才 ACK。

### 批量 FIFO 阶段

先核对压力脚本中的参考价格、卖出上限和发送间隔：

```powershell
python .\scripts\send_batch_signals.py
```

验收：

- 信号按发送顺序触达 miniQMT，不出现同时活动的本系统订单。
- 后一条信号在前一条达到终态后才提交。
- 每次重试前都能看到新的行情、资金或持仓查询结果。
- 每次失败/部分成交尝试只输出一条 `🔁` 明细。
- 撤单期间新增成交被计入最终成交量，下一单只提交真实剩余量。

## 异常处置

- 出现 `🛑 撤单终态未确认`：立即在 miniQMT 委托列表人工核对，确认没有活动订单后再重启服务。
- 出现持仓/资金查询失败：不要绕过风控；先恢复 miniQMT 连接。
- 出现价格偏离：更新发送脚本参考价或检查行情订阅，不要扩大偏离阈值掩盖问题。
- 出现重复信号：核对 `signal_id`，不要删除 SQLite 幂等记录后直接重放。

## 上线前最终检查

- [ ] 目标券商状态映射已核对。
- [ ] 单笔买卖、部分成交、撤单和拒单均在模拟盘验证。
- [ ] 批量 FIFO 压力测试通过。
- [ ] SQLite、Redis ACK 和终端日志时间线一致。
- [ ] `config.yaml`、策略凭据、运行数据库和日志没有进入 Git。
- [ ] 实盘首次启用使用小额并由人工盯盘。

# Redis 信号总线 —— Windows 服务器部署教程

适用于本项目的三服务器架构：**2 台 Windows 交易机 + 1 台 Windows Redis 服务器**。

```
   聚宽模拟盘 (harvester)
        │  XADD
        ▼
   ┌──────────────────────────────┐
   │  Redis 5.0.14.1 (Windows)    │   ← 本教程
   │  AOF 持久化 / noeviction      │
   │  非默认端口 / 强随机口令       │
   └───┬──────────────────────┬───┘
       │ XREADGROUP           │ XREADGROUP
 group=qmt_executors_gj  group=qmt_executors_hx
       ▼                      ▼
  Win 机 A (国金 QMT)     Win 机 B (华鑫 QMT)
```

---

## 零、版本说明

本教程针对 **Redis 5.0.14.1 for Windows**（tporadowski 分支，原生 Windows 服务）。

```
Redis server v=5.0.14.1 sha=ec77f72d:0 malloc=jemalloc-5.2.1-redis bits=64
```

这个版本对本项目来说是够用的：

| 能力 | 5.0.14.1 | 说明 |
|---|---|---|
| Stream（`XADD` / `XREADGROUP` / 消费组） | ✅ | Stream 是 5.0 引入的，整套跟单的基础 |
| `XINFO GROUPS` 的 `pending` / `last-delivered-id` | ✅ | 排查"消息卡住"够用 |
| `XINFO GROUPS` 的 `lag` 字段 | ❌ | 7.0 才有，不影响使用 |
| `XAUTOCLAIM` | ❌ | 7.0 才有，本项目没用 |
| ACL（多账号分权） | ❌ | 6.0 才有，本教程统一用 `requirepass` |

> ⚠️ 别装成微软那个 `microsoftarchive/redis`（3.0.504 / 3.2.100）。它是搜索
> "redis windows" 的第一个结果，但 **3.x 完全没有 Stream** —— `XADD` 会直接
> 报 unknown command，而聚宽端只会在日志里记一行发送失败，排查非常费时间。
> 如果不确定，跑一下：
>
> ```powershell
> redis-cli -p 6380 -a <口令> XADD probe '*' k v
> ```

---

## 一、安装

### 1.1 下载与解压

1. 从 `https://github.com/tporadowski/redis/releases` 下载 **Redis-x64-5.0.14.1.zip**
   （用 zip 不用 msi，目录结构更可控）。
2. 解压到 `C:\Redis`。
3. 建数据和日志目录：

```powershell
New-Item -ItemType Directory -Force -Path C:\Redis\data, C:\Redis\logs
```

### 1.2 配置文件与口令

把本仓库的 `deploy/redis/redis.windows.conf` 复制到 `C:\Redis\redis.windows.conf`，改两处：

```conf
port 6380                                    # 与两台交易机、聚宽三方保持一致
requirepass <这里换成下面生成的强口令>
```

生成强口令（32 字节 base64）：

```powershell
[Convert]::ToBase64String((1..32 | ForEach-Object { Get-Random -Max 256 }))
```

> 聚宽出口 IP 不固定、没法做安全组白名单，所以**这个口令 + 非默认端口 + 危险
> 命令禁用**就是这台服务器的全部防线。务必用随机生成的长口令，别用生日或项目名。
> 口令会随策略代码存在聚宽平台上，所以它也不该复用到任何别的系统。

### 1.3 注册成 Windows 服务

```powershell
cd C:\Redis
.\redis-server.exe --service-install .\redis.windows.conf --service-name Redis
.\redis-server.exe --service-start --service-name Redis
```

配置开机自启 + 崩溃自动重启（交易系统必备）：

```powershell
sc.exe config Redis start= auto
sc.exe failure Redis reset= 86400 actions= restart/5000/restart/10000/restart/30000
```

验证：

```powershell
C:\Redis\redis-cli.exe -p 6380 -a "<你的口令>" --no-auth-warning PING
# 期望: PONG
```

### 1.4 防火墙与安全组

只放行两台交易机的 IP：

```powershell
New-NetFirewallRule -DisplayName "Redis-QMT" -Direction Inbound -Protocol TCP `
  -LocalPort 6380 -Action Allow `
  -RemoteAddress 10.0.0.11,10.0.0.12   # ← 换成两台交易机的 IP
```

云厂商的**安全组**也要做同样的限制（它在系统防火墙之前生效，是更硬的一道）。

聚宽那一侧只能放开 —— 出口 IP 不固定，拿不到网段。这就是为什么口令强度是关键。

---

## 二、初始化 Stream 与消费组

Redis 起来之后先手动建一次，避免第一条消息到达时的边界情况：

```powershell
$cli = "C:\Redis\redis-cli.exe"
$args = @("-p","6380","-a","<口令>","--no-auth-warning")

# 建 stream（Stream 不能凭空存在，塞一条占位消息）
& $cli @args XADD tidal_quant_signals '*' payload '{"action":"noop"}'

# 两台交易机各建各的消费组 —— 名字必须不同！
& $cli @args XGROUP CREATE tidal_quant_signals qmt_executors_gj '$'
& $cli @args XGROUP CREATE tidal_quant_signals qmt_executors_hx '$'

& $cli @args XINFO GROUPS tidal_quant_signals
```

> **为什么 group 必须不同**：Redis 消费组是"组内分发"不是"广播"。两台机器用
> 同一个 group，每条信号只会投递给其中一台 —— 开盘 5 只票会被随机劈成"国金买
> 3 只、华鑫买 2 只"，而且日志里没有任何异常。
>
> 这一点和机器是否物理独立**无关**：消费组状态存在服务端，不在客户端。
> `state_db` 换机器就隔离了，`group` 不会。

---

## 三、验收

### 3.1 持久化验收 —— 最重要的一条

```powershell
$cli = "C:\Redis\redis-cli.exe"
$args = @("-p","6380","-a","<口令>","--no-auth-warning")

& $cli @args XADD tidal_quant_signals '*' payload '{"test":"persist"}'
& $cli @args XREADGROUP GROUP qmt_executors_gj win-gj-01 COUNT 10 STREAMS tidal_quant_signals '>'
# 故意不 ACK，制造一条 pending

net stop Redis
net start Redis

& $cli @args XLEN tidal_quant_signals          # 期望: > 0
& $cli @args XINFO GROUPS tidal_quant_signals  # 期望: 两个组都还在，pending 也还在
```

**如果重启后 `XLEN` 是 0 或提示 `no such key`，说明 AOF 没生效，绝对不能接实盘。**
回去检查 `appendonly yes` 和 `C:\Redis\data` 的写权限。

为什么关键：09:28 的 plan 发出后、交易机还没消费完时 Redis 重启一次，不开 AOF
的话当天的清仓单和买入单会**永久消失**，且没有任何报错 —— 执行端只会安静地
什么都不做。

### 3.2 不驱逐验收

配置里禁用了 `CONFIG` 命令，所以从 `INFO` 看：

```powershell
& $cli @args INFO memory | Select-String "maxmemory_policy"
# 期望: maxmemory_policy:noeviction
```

### 3.3 连通性验收

在**两台交易机**上分别执行：

```powershell
redis-cli -h <Redis服务器IP> -p 6380 -a <口令> --no-auth-warning PING
```

连不上时按顺序查：端口、口令、Windows 防火墙规则、云安全组。

### 3.4 收发链路验收（用 `tools/` 里的两个脚本）

这一步是接实盘前的核心验证，**必须做**。两个脚本零依赖（只要 `pip install redis`），
可以直接拷到任何一台机器上跑，不需要装项目。

**第一步**，在两台交易机上各开一个窗口，分别启动接收端（注意 group 不同）：

```powershell
# 国金机
python recv_test_signal.py --host <RedisIP> --port 6380 --password <口令> `
    --group qmt_executors_gj --consumer win-gj-01

# 华鑫机
python recv_test_signal.py --host <RedisIP> --port 6380 --password <口令> `
    --group qmt_executors_hx --consumer win-hx-01
```

**第二步**，从任意一台机器发一整轮测试信号：

```powershell
python send_test_signal.py --host <RedisIP> --port 6380 --password <口令>
```

**第三步**，确认**两台各自都收到了全部 4 条**（预订阅 / 日计划 / 卖半仓 / 清仓），
批次号一致。正确的样子：

```
[08:10:37.814] 📡 watchlist | 预订阅 3 只
[08:10:37.915] 📋 plan      | 日计划 清仓1只 待买2只
[08:10:38.017] 📈 signal    | sell 000001.XSHE sell_half (数量由执行端按账户计算)
[08:10:38.118] 📈 signal    | sell 000001.XSHE sell_all (数量由执行端按账户计算)

本次统计 | 共收到 4 条
  交易信号 2 | 日计划 1 | 预订阅 1 | 无法解析 0
  group=qmt_executors_gj consumer=win-gj-01
```

**如果两台加起来才 4 条**（比如一台 1 条、另一台 3 条），就是 group 配重了。
用这条确认：

```powershell
python send_test_signal.py --host <RedisIP> --port 6380 --password <口令> --kind inspect
```

看到只有一个消费组，就去改两份 config 的 `redis.group`。

**第四步**，再发一条畸形消息，确认交易机不会被打崩：

```powershell
python send_test_signal.py --host <RedisIP> --port 6380 --password <口令> --kind malformed
```

接收端应该打出 `⛔ rejected | KeyError: 'code'` 然后**继续正常运行**，
后续信号照收。这条验证的是：Stream 是多策略共享的，别的策略换个 schema
就可能产生本执行端不认识的消息，那种消息绝不能让当天的跟单收工。

> 默认 `strategy_id` 是 `harvester_test`，会被交易机的 `allowed_strategy_ids`
> 白名单挡掉，所以**不会真的下单**，只验证收信链路。想让信号真正进执行引擎，
> 加 `--strategy-id harvester`，但**务必先把 `trading.enabled` 设成 false**。

---

## 四、两台交易机的配置

从 `config.example.yaml` 复制一份到各自机器上改名（比如 `config.gj.yaml`）。
`.gitignore` 已经把 `config*.yaml` 排除在版本库外，只留模板 —— 真实的 host、
账号、QMT 路径、凭据都不会进 git。

两台之间必须不同的项：

| 配置项 | 国金机 | 华鑫机 |
|---|---|---|
| **`redis.group`** | `qmt_executors_gj` | `qmt_executors_hx` |
| **`redis.consumer`** | `win-gj-01` | `win-hx-01` |
| `trading.account_id` | 国金账号 | 华鑫账号 |
| `trading.miniqmt_path` | 国金 userdata_mini | 华鑫 userdata_mini |
| `state_db` | `data/gj.db` | `data/hx.db` |
| `log_dir` | `logs/gj` | `logs/hx` |

口令走环境变量，不写进文件（管理员 PowerShell，`setx /M` 写系统级）：

```powershell
setx /M REDIS_PASSWORD "<Redis 口令>"
```

设完要**重启终端或服务**才生效。配置文件里保持 `${REDIS_PASSWORD}` 占位。

聚宽策略侧改文件顶部的 `SIGNAL_REDIS_CONFIG`：

```python
SIGNAL_REDIS_CONFIG = {
    "host": "<Redis 服务器 IP>",
    "port": 6380,
    "password": "<同一个口令>",
    ...
}
```

启动后确认 banner 里这几项两台各不相同：

```
【系统】🟢 Redis监听已启动 | stream=tidal_quant_signals | group=qmt_executors_gj |
        consumer=win-gj-01 | 账户=8885417080 | 账本=data\gj.db
```

---

## 五、日常运维

### 时钟同步

三台机器都要，且**指向同一个 NTP 源**。执行端判断"什么时候可以下买单"用的是
本机时钟：快 5 秒会在 09:29:55 就把买单送到柜台（非交易时段废单），慢 5 秒则
09:30:05 才动手，09:28 预发抢的那点时间白抢了。

```powershell
w32tm /config /manualpeerlist:"ntp.aliyun.com,ntp1.aliyun.com" /syncfromflags:manual /update
net stop w32time; net start w32time
w32tm /resync
w32tm /query /status
```

### 备份

AOF 文件很小，收盘后拷一份就够：

```powershell
$d = Get-Date -Format "yyyyMMdd"
New-Item -ItemType Directory -Force -Path C:\Redis\backup | Out-Null
Copy-Item C:\Redis\data\appendonly.aof "C:\Redis\backup\appendonly-$d.aof"
```

### 盘中想看一眼状态

```powershell
# 消费组进度与积压
python send_test_signal.py --host <RedisIP> --port 6380 --password <口令> --kind inspect
```

两个组的 `pending` 在开盘后应该很快回到 0。长期不为 0 说明有信号执行不下去，
去对应那台机器的日志里找。

### 快速重建预案

Redis 是这套架构里唯一的单点。主从 + Sentinel 对两个账户是过度设计，但要能在
5 分钟内拉起替代实例 —— 把 `redis.windows.conf` 和这份教程放进版本库、加一份
最近的 AOF 备份，就够了。

按性价比，单点缓解的优先级：AOF 持久化（免费，已做）→ 策略端发布重试（已做，
3 次）→ 执行端断线重连（已做，指数退避）→ 快速重建预案 → 最后才是 Sentinel。

---

## 六、常见问题

**`XADD` 报 `unknown command`** —— 装的是 Redis 3.x（微软那个老版本）。
Stream 需要 5.0+，换成 tporadowski 的 5.0.14.1。

**交易机连不上，服务器本地 `redis-cli` 正常** —— 依次查：`bind` 是不是只绑了
127.0.0.1、Windows 防火墙规则、云安全组、端口是否三方一致。

**两台机器加起来才收到一份消息** —— `redis.group` 配重了。`--kind inspect`
确认是不是只有一个消费组。

**重启后消费组没了** —— AOF 没生效。查 `appendonly yes`、`C:\Redis\data` 目录
权限、以及那个目录下是否真的有 `appendonly.aof` 在长大。

**接收端一条都收不到** —— 新建的消费组从 `$`（最新）开始，收不到建组之前的
消息。先起接收端、再发信号；或者给 `recv_test_signal.py` 加 `--from-start`
补看历史。

**想临时用 `CONFIG` / `FLUSHALL`** —— 配置里禁用了。注释掉对应的
`rename-command` 行并重启服务，用完改回来。

# 下单提速与可靠性优化 实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 在不改变核心交易语义（先卖后买、ACK 后置、fail-closed、幂等）的前提下，修掉信号链路上的确定性瓶颈，并为"开盘首挂即成交"专门强化报价与首挂等待，每项都有可量化的验收目标。

**Architecture:** 局部加固 + 开盘专项：聚宽发送端加超时与重试；执行引擎行情取数加退避重试与**快照时效门控**；收信主循环重活移入后台线程；消费端 Redis 走内网（纯部署）；本地策略引擎账户快照加 1 秒短缓存；跌停排队卖出移入专用线程池；**开盘窗口内买卖报价沿用激进竞价逻辑、首挂放宽等待**（按券商A/券商B差异分机调参）；新增只读复盘脚本（终态/重挂分布 + **撤单确认耗时**）支撑调参；热路径日志阈值门控。

**Tech Stack:** Python ≥3.11、stdlib unittest、SQLite（只读复盘）、redis-py、xtquant（仅 Windows 实盘）。测试全部用 fakes，不触真实 Redis/QMT。

## Global Constraints

- 交易语义冻结：不改 SELL-before-BUY 开盘屏障、不改 ACK 后置到终态、不改资源 fail-closed、不改 signal_id 幂等。
- 新增配置项一律**默认关闭**（pct=0 / 秒数=0），老配置零行为变化；只有显式开启的机器才启用开盘强化。
- 券商A/券商B存在环境差异（券商A撤单确认 ~16s 常态，券商B快），一切开盘相关参数按机器配置，代码里不做券商特判。
- `joinquant_signal_sender.py` 必须保持单文件、可直接粘贴进聚宽策略，不 import 本仓库任何包。
- 所有新增执行路径在未配置/未启用时必须与现状完全一致（例如 `queue_executor=None` 走原同步路径）。
- tests/ 是本地套件不进版本库；每次提交只 add 源码、tools/、docs/。
- 日志沿用中文 + emoji 约定；新增打点一律 DEBUG 级（复盘用）。
- 每完成一个任务跑：`python -m compileall miniqmt_follower bigqmt_follower tests` 与 `python -m unittest discover -v`，全绿才提交。

---

### Task 1: 聚宽发送端加超时与重试

**Files:**
- Modify: `joinquant_signal_sender.py:71-134`
- Test: `tests/test_joinquant_sender.py`

**改动说明（白话）：** 现在发信号是"发一次，发不出去就拉倒"，而且网络假死时可能把整个策略卡住。给 Redis 连接加读写超时（2 秒），发送失败自动重试 3 次（间隔 0.5 秒）。因为 signal_id 不变，万一"第一次其实已到达只是回包丢了"，重发产生的重复消息会被 Windows 端幂等去重，不会重复下单。

- [ ] **Step 1: 写失败测试（沿用该文件现有 FakeRedis 手法）**

```python
class _FlakyXaddClient:
    """前 N 次 xadd 抛错, 之后返回成功 id; 记录调用次数。"""

    def __init__(self, fail_times):
        self.fail_times = fail_times
        self.calls = 0

    def xadd(self, *args, **kwargs):
        self.calls += 1
        if self.calls <= self.fail_times:
            raise ConnectionError("simulated redis outage")
        return "1699999999999-0"


class _FakeContext:
    current_dt = dt.datetime.now()


class XaddRetryTest(unittest.TestCase):
    def setUp(self):
        import joinquant_signal_sender as sender

        self.sender = sender
        self.client = _FlakyXaddClient(fail_times=2)
        sender.publish_trade_signal_to_redis._redis_client = self.client
        sender.publish_trade_signal_to_redis._redis_config_key = object()
        self._real_sleep = sender.time.sleep
        sender.time.sleep = lambda _sec: None  # 测试不真睡
        self.addCleanup(lambda: setattr(sender.time, "sleep", self._real_sleep))

    def test_retries_then_succeeds(self):
        result = self.sender.publish_trade_signal_to_redis(
            _FakeContext(), "buy", "000001.XSHE", 100, 10.0
        )
        self.assertTrue(result["sent"])
        self.assertEqual(self.client.calls, 3)  # 1 次失败×2 + 1 次成功

    def test_gives_up_after_max_retries(self):
        self.client.fail_times = 10**9
        result = self.sender.publish_trade_signal_to_redis(
            _FakeContext(), "sell", "000001.XSHE", 100, 10.0
        )
        self.assertFalse(result["sent"])
        self.assertEqual(self.client.calls, 3)  # 重试到上限即止, 不无限重试
```

- [ ] **Step 2: 跑测试确认失败**

Run: `python -m unittest tests.test_joinquant_sender.XaddRetryTest -v`
Expected: FAIL（`_xadd` 尚无重试逻辑，`client.calls == 1`）

- [ ] **Step 3: 实现**

`joinquant_signal_sender.py` 顶部 import 区加 `import time`，配置块与 `_xadd` 改为：

```python
SIGNAL_REDIS_CONFIG = {
    "host": "YOUR_REDIS_HOST",   # 部署前替换；生产地址不要提交到仓库
    "port": 6379,
    "password": None,
    "stream": "jq_qmt_signals",
    "maxlen": 10000,
    "socket_connect_timeout": 1,
    "socket_timeout": 2,         # 读写超时: 半死连接最多挂 2 秒, 不会卡死策略
}
SIGNAL_STRATEGY_ID = "YOUR_STRATEGY_ID"
SIGNAL_MAX_LIVE_LAG_SECONDS = 600
# 发送失败重试: signal_id 不变, 重复消息由执行端幂等去重, 重试不会重复下单。
SIGNAL_XADD_MAX_RETRIES = 3
SIGNAL_XADD_RETRY_DELAY_SEC = 0.5


def _xadd(payload):
    """写入 Stream; 失败自动重试, 全部失败才抛给上层记日志。"""
    client = _cached_redis_client()
    last_error = None
    for attempt in range(1, SIGNAL_XADD_MAX_RETRIES + 1):
        try:
            return client.xadd(
                SIGNAL_REDIS_CONFIG["stream"],
                {"payload": json.dumps(payload, ensure_ascii=False)},
                maxlen=SIGNAL_REDIS_CONFIG["maxlen"],
                approximate=True,
            )
        except Exception as exc:
            last_error = exc
            _log("[信号] Redis写入失败 第{}次重试: {}".format(attempt, exc))
            if attempt < SIGNAL_XADD_MAX_RETRIES:
                time.sleep(SIGNAL_XADD_RETRY_DELAY_SEC)
    raise last_error
```

同时 `_cached_redis_client` 里给 `redis.Redis(...)` 补 `socket_timeout=SIGNAL_REDIS_CONFIG["socket_timeout"]`。

- [ ] **Step 4: 跑测试确认通过 + 全量回归**

Run: `python -m unittest tests.test_joinquant_sender -v` → PASS
Run: `python -m unittest discover -v` → 全绿

- [ ] **Step 5: Commit**

```bash
git add joinquant_signal_sender.py
git commit -m "feat(sender): 聚宽发送端加读写超时与3次重试, 信号不再因Redis抖动丢失"
```

**验收目标（Task 1）：**
- 测试层面：模拟 Redis 连续失败 → 自动重试后成功（`sent=True`，调用 3 次）；永久失败 → 最多 3 次即返回 `sent=False`，不会无限重试、不会抛穿策略。
- 线上预期：Redis 抖动 1~2 秒内恢复时零丢单；半死连接最多阻塞 2 秒（原来是无限），策略不再整体卡死。
- 回滚方式：把 `SIGNAL_XADD_MAX_RETRIES` 改回 1、删掉 `socket_timeout` 即恢复原行为。

---

### Task 2: 执行端行情取数加退避重试

**Files:**
- Modify: `miniqmt_follower/executor.py:687-690`、`miniqmt_follower/executor.py:1051-1067`
- Test: `tests/test_executor.py`

**改动说明（白话）：** 现在取一下最新价，如果这一下因为行情抖动没取到，信号直接判"券商失败"当天再也不碰。券商拒单都还会重试，行情抖一下反而直接判死，不合理。给取价包一层"失败等 0.2 秒再试，最多 3 次"。注意：行情正常时零开销，只在失败时才多等。

- [ ] **Step 1: 写失败测试**

```python
class FlakyMarketData:
    """前 fail_times 次 latest_quote 抛错, 之后委托给真实 fake。"""

    def __init__(self, inner, fail_times):
        self.inner = inner
        self.fail_times = fail_times
        self.calls = 0

    def latest_quote(self, code):
        self.calls += 1
        if self.calls <= self.fail_times:
            raise RuntimeError("simulated quote hiccup")
        return self.inner.latest_quote(code)

    def instrument_name(self, code):
        return None


class QuoteRetryTest(unittest.TestCase):
    # 复用现有 test_executor 的 FakeBroker/engine 装配;
    # 关键: 把 engine.market_data 换成 FlakyMarketData(engine.market_data, 2)
    def test_quote_hiccup_is_retried_then_filled(self):
        engine = self._build_engine()
        engine.market_data = FlakyMarketData(engine.market_data, 2)
        result = engine.execute(self._buy_signal("000001.XSHE", 100))
        self.assertEqual(result.status, ExecutionStatus.FILLED)

    def test_quote_all_failures_still_fail_broker(self):
        engine = self._build_engine()
        engine.market_data = FlakyMarketData(engine.market_data, 99)
        result = engine.execute(self._buy_signal("000001.XSHE", 100))
        self.assertEqual(result.status, ExecutionStatus.FAILED_BROKER)
```

（`_build_engine`/`_buy_signal` 用该文件里现成的 helper；若没有，就地补两个 10 行以内的小 helper。）

- [ ] **Step 2: 跑测试确认失败**

Run: `python -m unittest tests.test_executor.QuoteRetryTest -v`
Expected: FAIL（第一次取价抛错即 FAILED_BROKER）

- [ ] **Step 3: 实现**

`executor.py` 常量区加：

```python
_QUOTE_RETRY_MAX = 3
_QUOTE_RETRY_DELAY_SEC = 0.2
```

类内新增方法（放在 `_resolve_intent_amount` 附近）：

```python
def _latest_quote_with_retry(self, signal: TradeSignal) -> Quote:
    """取行情快照, 失败短暂退避重试 —— 行情抖动不应直接杀死信号。"""
    last_error: Exception | None = None
    for attempt in range(1, _QUOTE_RETRY_MAX + 1):
        try:
            return self.market_data.latest_quote(signal.code)
        except Exception as exc:
            last_error = exc
            logger.warning(
                "%s | %s | 行情取数失败 第%d/%d次 | %s",
                signal.console_event("重试"), signal.display_code,
                attempt, _QUOTE_RETRY_MAX, exc,
            )
            if attempt < _QUOTE_RETRY_MAX:
                time.sleep(_QUOTE_RETRY_DELAY_SEC)
    raise last_error  # type: ignore[misc]
```

替换三处调用（行 688、1052、1061）：
`self.market_data.latest_quote(signal.code)` → `self._latest_quote_with_retry(signal)`

- [ ] **Step 4: 跑测试确认通过 + 全量回归**

Run: `python -m unittest tests.test_executor.QuoteRetryTest -v` → PASS
Run: `python -m unittest discover -v` → 全绿（行情正常路径零变化，旧用例应全部原样通过）

- [ ] **Step 5: Commit**

```bash
git add miniqmt_follower/executor.py
git commit -m "feat(executor): 行情取数失败退避重试3次, 不再让行情抖动直接判死信号"
```

**验收目标（Task 2）：**
- 测试层面：前 2 次取价失败第 3 次成功 → 最终 `FILLED`；全部失败 → `FAILED_BROKER`（与现行为一致）。
- 线上预期：行情源抖动的日子里，因"行情取数失败"产生的 `FAILED_BROKER` 数量趋零（用 Task 7 复盘脚本对比 signals 表）。
- 回滚方式：`_QUOTE_RETRY_MAX` 改回 1 即恢复原行为。

---

### Task 3: 收信主循环减负（订阅/取名/plan 展开移出主线程）

**Files:**
- Modify: `miniqmt_follower/adapters/qmt.py`（新增 `cached_instrument_name`）
- Modify: `miniqmt_follower/app.py`（watchlist 后台订阅、收单取名改缓存版、plan 展开后台化、收信处理耗时打点）
- Test: `tests/test_qmt_adapter_mapping.py`、`tests/test_runtime.py`

**改动说明（白话）：** 收信线程现在一边收信一边干重活：订阅行情（首次订阅可能下载数据，几百毫秒到秒级）、查股票中文名、展开日计划（逐票全量查持仓）。干重活期间，后面到的信号在 Redis 里没人接。把这三样全挪到后台线程，收信线程只收信、校验、派活。

- [ ] **Step 1: 写失败测试（三组）**

```python
# tests/test_qmt_adapter_mapping.py —— 用现有 fake xtdata 手法
class CachedInstrumentNameTest(unittest.TestCase):
    def test_miss_returns_none_without_query(self):
        adapter = self._build_adapter()  # fake xtdata 记录调用次数
        name = adapter.cached_instrument_name("000001.XSHE")
        self.assertIsNone(name)
        self.assertEqual(self.fake_xtdata.detail_calls, 0)

    def test_hit_reads_cache_without_query(self):
        adapter = self._build_adapter()
        adapter.instrument_name("000001.XSHE")   # 首次真查
        calls = self.fake_xtdata.detail_calls
        name = adapter.cached_instrument_name("000001.XSHE")
        self.assertEqual(name, "平安银行")
        self.assertEqual(self.fake_xtdata.detail_calls, calls)  # 未新增查询
```

```python
# tests/test_runtime.py —— 用现有 Fake 装配 main() 的调度函数
class DispatchOffloadTest(unittest.TestCase):
    def test_watchlist_dispatch_returns_before_subscribe_finishes(self):
        # FakeMarketData.subscribe 阻塞在一个 Event 上
        # 断言 _dispatch 在 subscribe 完成前就返回(即不堵收信)
        ...

    def test_signal_dispatch_never_calls_full_instrument_name(self):
        # 收单时 stock_name 为空 → 只走 cached_instrument_name, 不触发 QMT 查询
        ...

    def test_plan_dispatch_runs_expansion_in_background(self):
        # FakeBroker.query_position 阻塞在 Event 上
        # 断言 _dispatch 先返回, plan 展开在后台线程完成
        ...
```

（具体 Event 阻塞与 join 断言沿用 `tests/test_runtime.py` 现有线程测试风格；每个用例 30 行内。）

- [ ] **Step 2: 跑测试确认失败**

Run: `python -m unittest tests.test_qmt_adapter_mapping.CachedInstrumentNameTest tests.test_runtime.DispatchOffloadTest -v`
Expected: FAIL（方法不存在 / 主循环仍同步处理）

- [ ] **Step 3: 实现**

**3a. qmt.py 新增缓存版取名（只读缓存，未命中不查询）：**

```python
def cached_instrument_name(self, code: str) -> str | None:
    """只读缓存的证券中文名, 未命中返回 None 且不触发 QMT 查询 —— 收信线程专用。

    收信线程绝不能为了日志里的中文名去等一次 get_instrument_detail;
    名字没缓存就先用代码显示, 工作线程里的 instrument_name 兜底补名。
    """
    today = dt.date.today().isoformat()
    if today != self._instrument_detail_cache_day:
        return None
    qmt_code = jq_code_to_qmt_code(code)
    detail = self._instrument_detail_cache.get(qmt_code)
    if detail is None:
        return None
    name = detail.get("InstrumentName") or detail.get("instrument_name")
    return str(name).strip() if name else None
```

**3b. app.py：把 main() 循环里从 `if message.rejected is not None:` 到 `_submit_trade(...)` 的整段搬进 main() 内部的嵌套函数 `_dispatch(message)`（闭包引用外层变量，原 `continue` 改为 `return`），并在其中做三处替换：**

替换 1 —— watchlist 分支改为"先 ACK、后台订阅"：

```python
if message.watchlist is not None:
    if not _strategy_allowed(message.watchlist.strategy_id, allowed_strategies):
        logger.info(
            "【行情】🛂 预订阅忽略 | 策略 %s 不在白名单",
            message.watchlist.strategy_id or "<空>",
        )
        stream.ack(message.message_id)
        return
    # 订阅只是预热(失败有懒订阅兜底), 先 ACK 再丢后台, 绝不堵收信。
    stream.ack(message.message_id)
    _subscribe_watchlist_in_background(message.watchlist, market_data)
    return
```

替换 2 —— 收单取名改缓存版：

```python
sig = message.signal
if not sig.stock_name:
    sig = sig.with_stock_name(market_data.cached_instrument_name(sig.code))
    message = replace(message, signal=sig)
```

替换 3 —— plan 分支改后台线程：

```python
if message.plan is not None:
    _handle_plan_message_in_background(
        message=message, config=config, allowed_strategies=allowed_strategies,
        plan_executor=plan_executor, pools=pools, engine=engine,
        opening_barrier=opening_barrier, pending=pending, stream=stream,
        store=store,
    )
    return
```

新增两个模块级函数（放在 `_handle_plan_message` 附近）：

```python
def _subscribe_watchlist_in_background(watchlist, market_data) -> None:
    """盘前预订阅放后台线程: 首次订阅可能拉取 tick 历史, 不能堵收信循环。"""

    def _run() -> None:
        try:
            market_data.subscribe(watchlist.codes)
            code_labels = [
                format_stock_label(code, market_data.instrument_name(code))
                for code in watchlist.codes
            ]
            logger.info(
                "【行情】📡 预订阅 | 策略=%s | %s只 | %s",
                watchlist.strategy_id, len(watchlist.codes), ",".join(code_labels),
            )
        except Exception as exc:
            logger.exception(
                "【行情】❌ 预订阅失败 | 策略=%s | %s", watchlist.strategy_id, exc,
            )

    threading.Thread(
        target=_run, name="qmt-watchlist-subscribe", daemon=True,
    ).start()


def _handle_plan_message_in_background(**kwargs) -> None:
    """plan 展开要逐票全量查持仓(QMT同步调用), 放后台线程, 不堵收信循环。"""
    threading.Thread(
        target=_handle_plan_message, kwargs=kwargs,
        name="qmt-plan-expand", daemon=True,
    ).start()
```

循环处加打点（`_dispatch` 返回后）：

```python
t0 = time.monotonic()
_dispatch(message)
logger.debug(
    "⏱️ 收信处理耗时 | %.1fms | msg_id=%s",
    (time.monotonic() - t0) * 1000, message.message_id,
)
```

- [ ] **Step 4: 跑测试确认通过 + 全量回归**

Run: `python -m unittest tests.test_qmt_adapter_mapping tests.test_runtime -v` → PASS
Run: `python -m unittest discover -v` → 全绿
Run: `python -m compileall miniqmt_follower` → 无错

- [ ] **Step 5: Commit**

```bash
git add miniqmt_follower/adapters/qmt.py miniqmt_follower/app.py
git commit -m "feat(app): 收信主循环减负, 订阅/取名/plan展开移入后台线程"
```

**验收目标（Task 3）：**
- 测试层面：watchlist 订阅进行中收信函数已返回；收单不触发 `get_instrument_detail`；plan 展开在后台线程完成。
- 线上预期：日志新增「收信处理耗时」打点，watchlist 5~10 只 + plan 5 只的场景下 P95 < 50ms（改动前实测可达数百 ms 到秒级）。9:27 卖单批与 9:30 买单批到池时间整体提前、离散度缩小。
- 回滚方式：`_dispatch` 内部三个分支可各自改回同步调用（`git revert` 该 commit 即可整体回退）。

---

### Task 4: 消费端 Redis 走内网（纯部署，无代码）

**Files:** 无代码改动；涉及 `config_gj.yaml`、`config_hx.yaml` 的 `redis.host`（这两个文件不进版本库）。

- [ ] **Step 1: 量基线**

在交易机上（先于任何改动）记录公网基线：
`redis-cli -h <Redis公网IP> -p 6380 -a <password> --latency`（约 30 秒，记平均值）
同时从当日日志取「传输 ms」中位数。

- [ ] **Step 2: 开通内网通路**

确认阿里云 ECS（<Redis公网IP>）地域，开通该 ECS 与交易机所在网络的内网互通（同 VPC / 对等连接 / 专线，择一），安全组放行 6380 内网网段。

- [ ] **Step 3: 验证内网**

`redis-cli -h <内网IP> -p 6380 -a <password> --latency` → 与公网基线对比；`ping <内网IP>` 丢包为 0。

- [ ] **Step 4: 切换并验证**

两台交易机 `config_gj.yaml` / `config_hx.yaml` 的 `redis.host` 改为内网地址，重启 `python main.py`；确认日志 🟢 启动 banner、消费正常、XACK 正常。聚宽发送端保持公网地址不变（聚宽云进不了你的 VPC）。

**验收目标（Task 4）：**
- `redis-cli --latency` 平均值从公网基线（通常 20~80ms）降到 < 5ms。
- 日志「传输 ms」中位数下降幅度与 RTT 下降一致；运行一周内 Redis 断线重连次数明显下降。
- 如实说明边界：这只改善「交易机 ↔ Redis」一跳；「聚宽云 → Redis」仍是公网，端到端仍含一段公网 RTT。若聚宽侧延迟占比高，此任务收益会被压缩——先看日志「传输 ms」的构成再决定是否值得做。
- 回滚方式：host 改回公网 IP 重启即可。

---

### Task 5: 本地策略引擎账户快照加短缓存

**Files:**
- Modify: `miniqmt_follower/strategy_engine.py:252-308`（`tick()` 与 `__init__`）
- Test: `tests/test_strategy_engine.py`

**改动说明（白话）：** 策略引擎在开盘前后每 0.2 秒就全量查一次账户资产和持仓，每次都是两次同步 QMT 查询，还和下单线程抢同一把 QMT 锁。做决策不需要 0.2 秒一次的新鲜度——给账户快照加 1 秒缓存：1 秒内复用上次结果，下单路径不受影响（下单侧每次仍查最新值）。15:02 的日结照旧用全新查询。

- [ ] **Step 1: 写失败测试**

```python
class AccountSnapshotCacheTest(unittest.TestCase):
    # 复用现有 FakeBroker(带 query_account_snapshot 计数) 与引擎装配
    def test_second_tick_within_ttl_reuses_snapshot(self):
        engine = self._build_engine(account_snapshot_ttl_sec=60.0)
        engine.tick(_t(9, 30, 0))
        engine.tick(_t(9, 30, 0))          # 间隔 0 秒 < TTL
        self.assertEqual(self.fake_broker.account_query_calls, 1)

    def test_tick_after_ttl_refreshes_snapshot(self):
        engine = self._build_engine(account_snapshot_ttl_sec=60.0)
        engine.tick(_t(9, 30, 0))
        engine._account_cache = None       # 模拟 TTL 到期(测试不真睡)
        engine.tick(_t(9, 30, 1))
        self.assertEqual(self.fake_broker.account_query_calls, 2)
```

- [ ] **Step 2: 跑测试确认失败**

Run: `python -m unittest tests.test_strategy_engine.AccountSnapshotCacheTest -v`
Expected: FAIL（第二个 tick 又查了一次账户）

- [ ] **Step 3: 实现**

`strategy_engine.py` 顶部常量加：

```python
_ACCOUNT_SNAPSHOT_TTL_SEC = 1.0
```

`__init__` 签名与赋值加：

```python
def __init__(self, *, config, machine_schedule, store, market_data, broker,
             executor, pools, opening_barrier, stop_event=None, clock=dt.datetime.now,
             account_snapshot_ttl_sec: float = _ACCOUNT_SNAPSHOT_TTL_SEC):
    ...
    self._account_snapshot_ttl_sec = account_snapshot_ttl_sec
    self._account_cache: tuple[float, object] | None = None
```

新增方法：

```python
def _account_snapshot_cached(self) -> object:
    """带短TTL的账户快照: 策略决策容忍秒级新鲜度, 不必每个tick都全量查QMT。

    下单路径不经过这里(执行引擎每次仍实时查资金/持仓), 缓存只服务决策;
    日结(15:02)在 _close_day 里另行全新查询, 不共用本缓存。
    """
    now_mono = time.monotonic()
    if (
        self._account_cache is not None
        and now_mono - self._account_cache[0] < self._account_snapshot_ttl_sec
    ):
        return self._account_cache[1]
    snapshot = self.broker.query_account_snapshot()
    self._account_cache = (now_mono, snapshot)
    return snapshot
```

`tick()` 中 `account = self.broker.query_account_snapshot()` 改为 `account = self._account_snapshot_cached()`。`_close_day` 里的查询保持原样不动。

- [ ] **Step 4: 跑测试 + 全量回归（重点：老用例若依赖"每 tick 都查"需适配）**

Run: `python -m unittest tests.test_strategy_engine -v`
若个别旧用例假定"两次 tick 各查一次账户"而失败：给该用例构造引擎时传 `account_snapshot_ttl_sec=0.0`（TTL=0 时每次必查），不改业务断言。
Run: `python -m unittest discover -v` → 全绿

- [ ] **Step 5: Commit**

```bash
git add miniqmt_follower/strategy_engine.py
git commit -m "feat(strategy): 账户快照1秒短缓存, 开盘窗口少抢QMT锁"
```

**验收目标（Task 5）：**
- 测试层面：TTL 内两次 tick 只查 1 次账户；到期后刷新。
- 线上预期：开盘窗口（9:27~9:35）策略引擎 `query_stock_asset`/`query_stock_positions` 调用次数下降 ≥80%（约从每 2 秒 10 次降到 2 次），QMT 锁排队时间缩短，报单/查单更快。持仓变化最多延迟 1 秒反映到策略决策——可接受（策略 tick 本来就是离散的）。
- 回滚方式：`_ACCOUNT_SNAPSHOT_TTL_SEC` 改回 0 即恢复每次全查。

---

### Task 6: 跌停排队卖出移入专用线程池（卖出 worker 不再被排队单占用）

**Files:**
- Modify: `miniqmt_follower/executor.py`（`_queue_sell_at_limit_down` 拆分为"移交 + `_run_queued_sell`"、新增 `queue_executor`/`queue_future_for`/`_forget_queued_future`）
- Modify: `miniqmt_follower/app.py`（建 `qmt-queue` 线程池、`_execute_with_barrier` 条件释放屏障、`_reap_one` 挂接排队终态 ACK）
- Test: `tests/test_executor.py`、`tests/test_runtime.py`

**改动说明（白话）：** 一只跌停排队卖单从挂上到 14:56 才撤，期间只是每 3 秒看一次状态，却占着 1 个卖出 worker（最多 5 只占 5 个，只剩 3 个应付盘中所有正常卖出）。把"挂单之后的慢轮询"挪到一个专用的小线程池（容量 = 5+5）里，卖出 worker 挂完单立刻腾出来接下一个信号。排队单的成交机会完全不变（单子本来就挂在券商那里）。为控制风险，本轮只迁移**跌停排队卖出**；涨停排队买入路径里有"到期回主循环补剩余量"的分支，留待验证卖出侧效果后再做。

- [ ] **Step 1: 写失败测试**

```python
# tests/test_executor.py
class QueueSellOffloadTest(unittest.TestCase):
    def test_execute_returns_placeholder_while_queue_polls_in_dedicated_worker(self):
        # 装配: FakeBroker 挂单成功且状态一直 OPEN; 单边行情(bid1=None, 贴跌停价);
        # monkeypatch executor_module._seconds_until_queue_sell_cancel 返回 0.5
        engine = self._build_engine(queue_executor=ThreadPoolExecutor(max_workers=1))
        t0 = time.monotonic()
        result = engine.execute(self._queue_sell_signal())
        elapsed = time.monotonic() - t0
        self.assertEqual(result.status, ExecutionStatus.QUEUED_LIMIT_DOWN)
        self.assertLess(elapsed, 0.3)          # 挂单即返回, 不陪慢轮询
        queued = engine.queue_future_for(result.signal_id)
        self.assertIsNotNone(queued)
        final = queued.result(timeout=5)       # 专用线程里等到截止并收尾
        self.assertIn(final.status, {ExecutionStatus.LIMIT_DOWN_QUEUE_EXPIRED,
                                     ExecutionStatus.PARTIALLY_FILLED_TIMEOUT})

    def test_without_queue_executor_behavior_unchanged(self):
        # queue_executor=None → 同步跑完再返回(旧行为), 现有用例应原样全绿
        ...
```

```python
# tests/test_runtime.py
class QueuedTerminalAckTest(unittest.TestCase):
    def test_ack_fires_when_queue_future_reaches_durable_terminal(self):
        # 装配 fake store(排队中间态 → 终态)、fake stream
        # 断言 _reap_one 后注册回调; queue future 完成后 stream.ack 被调用
        ...
```

- [ ] **Step 2: 跑测试确认失败**

Run: `python -m unittest tests.test_executor.QueueSellOffloadTest tests.test_runtime.QueuedTerminalAckTest -v`
Expected: FAIL（无 `queue_executor` 参数 / 无 `queue_future_for`）

- [ ] **Step 3: 实现**

**3a. executor.py** —— `__init__` 签名与赋值：

```python
from concurrent.futures import Future, ThreadPoolExecutor

def __init__(self, store, market_data, broker, config, machine_schedule, *,
             on_limit_down_queued=None, queue_executor: ThreadPoolExecutor | None = None):
    ...
    self._queue_executor = queue_executor
    self._queued_futures: dict[str, Future] = {}
    self._queued_futures_lock = threading.Lock()
```

新增两个小方法：

```python
def queue_future_for(self, signal_id: str) -> Future | None:
    """排队单专用线程的 future, 供上层在终态时补发 ACK。"""
    with self._queued_futures_lock:
        return self._queued_futures.get(signal_id)

def _forget_queued_future(self, signal_id: str, future: Future) -> None:
    with self._queued_futures_lock:
        if self._queued_futures.get(signal_id) is future:
            del self._queued_futures[signal_id]
```

`_queue_sell_at_limit_down` 改为"校验 + 容量闸 + 移交"，原方法体后半（从 `attempt_qty = self._cap_attempt_to_available_resources(...)` 起）原样迁入新方法 `_run_queued_sell`，其中 `self._on_limit_down_queued(signal.signal_id)` 改为放 `_run_queued_sell` 的 `finally` 里（成功挂单与各种降级跳过路径都会走到，保证开盘屏障一定被释放）：

```python
def _queue_sell_at_limit_down(self, signal, quote, remaining_qty, total_filled, attempts):
    code_label = signal.display_code

    def _fallback_skip(message: str) -> ExecutionResult:
        status = (ExecutionStatus.PARTIALLY_FILLED_TIMEOUT if total_filled
                  else ExecutionStatus.SKIPPED_LIMIT_DOWN)
        return self._finish(signal, status, total_filled, attempts, message)

    low_limit = quote.low_limit
    if low_limit is None or low_limit <= 0:
        return _fallback_skip("limit-down queue fallback: low_limit unavailable")
    queue_cancel_at = self.machine_schedule.order_guard.limit_down_queue_cancel_at
    wait_sec = _seconds_until_queue_sell_cancel(queue_cancel_at)
    if wait_sec <= 0:
        return _fallback_skip("limit-down queue fallback: past queue deadline")
    with self._queue_sell_lock:
        if self._active_queue_sells >= self.config.max_concurrent_queue_sells:
            over_capacity = True
        else:
            self._active_queue_sells += 1
            over_capacity = False
    if over_capacity:
        return _fallback_skip("limit-down queue fallback: queue capacity reached")

    if self._queue_executor is None:
        # 未配置专用池: 完全保持旧行为(同步挂单+慢轮询)。
        return self._run_queued_sell(
            signal, remaining_qty, total_filled, attempts,
            low_limit, queue_cancel_at, wait_sec,
        )

    future = self._queue_executor.submit(
        self._run_queued_sell,
        signal, remaining_qty, total_filled, attempts,
        low_limit, queue_cancel_at, wait_sec,
    )
    with self._queued_futures_lock:
        self._queued_futures[signal.signal_id] = future
    future.add_done_callback(
        lambda f: self._forget_queued_future(signal.signal_id, f)
    )
    logger.info(
        "%s | %s | 跌停排队已转交专用线程 | 截止 %s",
        signal.console_event("竞价"), code_label, queue_cancel_at,
    )
    # 占位结果: 主 worker 立即返回, 队列慢轮询在专用线程继续。
    return ExecutionResult(
        signal_id=signal.signal_id,
        status=ExecutionStatus.QUEUED_LIMIT_DOWN,
        requested_qty=signal.amount,
        filled_qty=total_filled,
        attempts=attempts,
        message="limit-down queue handed to dedicated worker",
    )
```

`_run_queued_sell(self, signal, remaining_qty, total_filled, attempts, low_limit, queue_cancel_at, wait_sec)` 即旧方法体后半（含原 try/finally 减容量计数），`finally` 中追加 `self._on_limit_down_queued(signal.signal_id)`。

**3b. app.py** —— `_build_execution_components` 加参数并透传：

```python
def _build_execution_components(*, config, store, market_data, broker,
                                queue_executor=None):
    ...
    engine = OrderExecutionEngine(
        store=store, market_data=market_data, broker=broker,
        config=config.execution, machine_schedule=config.machine_schedule,
        on_limit_down_queued=opening_barrier.release_all,
        queue_executor=queue_executor,
    )
```

main() 内建专用池（与两个买卖池并列嵌套）：

```python
queue_capacity = (
    config.execution.max_concurrent_queue_sells
    + config.execution.max_concurrent_queue_buys
)
...
with (
    ThreadPoolExecutor(max_workers=args.workers, thread_name_prefix="qmt-sell") as sell_pool,
    ThreadPoolExecutor(max_workers=args.workers, thread_name_prefix="qmt-buy") as buy_pool,
    ThreadPoolExecutor(max_workers=max(1, queue_capacity),
                       thread_name_prefix="qmt-queue") as queue_pool,
):
    ...
    opening_barrier, engine = _build_execution_components(
        config=config, store=store, market_data=market_data, broker=broker,
        queue_executor=queue_pool,
    )
```

`_execute_with_barrier` 条件释放屏障（排队单转交后不等终态就释放会改变"卖单完成才放买"的语义，排队单的屏障释放仍由 `_run_queued_sell` 挂单落库后经 `release_all` 完成）：

```python
def _execute_with_barrier(engine, signal, opening_barrier, *, recover_existing):
    result: ExecutionResult | None = None
    try:
        ...
        result = engine.recover(signal) if recover_existing else engine.execute(signal)
        return result
    except Exception as exc:
        ...
        raise
    finally:
        if signal.action == Action.SELL and (
            result is None
            or result.status not in {
                ExecutionStatus.QUEUED_LIMIT_DOWN,
                ExecutionStatus.QUEUED_LIMIT_UP,
            }
        ):
            opening_barrier.release(signal.signal_id)
```

`_reap_one` / `_reap_one_safe` / `_reap_completed` 增加 `engine` 参数并在各调用点传入；`_reap_one` 信号分支在"非终态不 ACK"之前插入排队挂接：

```python
if result.status in {ExecutionStatus.QUEUED_LIMIT_DOWN, ExecutionStatus.QUEUED_LIMIT_UP} \
        and engine is not None:
    queued = engine.queue_future_for(result.signal_id)
    if queued is not None and not queued.done():
        queued.add_done_callback(
            lambda f, m=message: _reap_queued_terminal(f, m, stream, store)
        )
        return
```

新增两个模块级函数（回调内绝不抛，套 try）：

```python
def _reap_queued_terminal(future, message, stream, store) -> None:
    """排队单在专用线程到达终态后补发 ACK; 未落库终态则保留 Redis 待办。"""
    try:
        result = future.result()
        if not _result_is_durably_terminal(result, store):
            logger.error(
                "%s | 排队单未落库终态 | 保留Redis待办", result.signal_id,
            )
            return
        logger.debug("🏁 排队单执行完成 | %s 状态=%s", result.signal_id, result.status.value)
        stream.ack(message.message_id)
    except Exception:
        logger.exception("【系统】❌ 排队单终态回调异常 | %s", message.message_id)


def _reap_plan_queued(future, message, results, stream, store) -> None:
    """日计划里有排队派生信号时, 最后一个排队单终态后复查全部结果再 ACK。"""
    try:
        if not all(_result_is_durably_terminal(r, store) for r in results):
            return
        stream.ack(message.message_id)
    except Exception:
        logger.exception("【计划】❌ 排队派生信号终态回调异常 | %s", message.message_id)
```

plan 分支（`_reap_one` 里 `if message.plan is not None:`）在"存在未落库终态"时改为：给每个 QUEUED_* 结果对应的 queue future 注册 `_reap_plan_queued(f, message, results, stream, store)` 回调后再返回。

**崩溃恢复已覆盖**：排队期间进程死 → Redis 不 ACK → 重启重投 → `recover()` 对 `QUEUED_LIMIT_DOWN` 走 `_wait_recovered_queue_order`（现有逻辑），无需新代码，但要写一条注释说明。

- [ ] **Step 4: 跑测试 + 全量回归**

Run: `python -m unittest tests.test_executor tests.test_runtime -v` → PASS
Run: `python -m unittest discover -v` → 全绿（重点确认旧跌停排队用例在 `queue_executor=None` 下原样通过）
Run: `python -m compileall miniqmt_follower` → 无错

- [ ] **Step 5: Commit**

```bash
git add miniqmt_follower/executor.py miniqmt_follower/app.py
git commit -m "feat(executor): 跌停排队卖出移入专用线程池, 卖出worker不再被全天占用"
```

**验收目标（Task 6）：**
- 测试层面：有专用池时 `execute()` 挂单落库后立即返回（耗时 < 0.3s），排队慢轮询在 `qmt-queue` 线程完成并落终态；无专用池时行为与现状完全一致（旧测试全绿）。
- 线上预期：5 只跌停排队单活跃时，卖出池 8/8 空闲，盘中止损卖零等待进池提交；排队单成交机会不变（单子始终挂在券商）。线程名 `qmt-queue` 可从日志/线程转储观测。
- 回滚方式：`queue_executor=None`（或该 commit 整体 revert）即恢复旧行为。

---

### Task 7: 复盘脚本（终态/重挂/撤单确认耗时）+ 分机调参

**Files:**
- Create: `tools/analyze_attempts.py`、`tools/analyze_cancel_latency.py`
- Modify（视复盘结论）: `config_gj.yaml`、`config_hx.yaml`（不进版本库）

**改动说明（白话）：** 2026-08-14 实盘日志已证明两件事：① 券商A撤单确认 16~18 秒是常态（撤单请求 09:30:00.85 → 终态 09:30:17.2），叠加 15s 总预算后"撤单重挂"在券商A开盘基本不存在；② 0.5s 超时在开盘快市下首挂即被撤，纯靠撤单竞态捡回 200 股。所以复盘脚本做两件：账本统计（终态/重挂分布），日志统计（撤单确认耗时分布，直接量化券商A vs 券商B的环境差异）。调参按机器分开：券商A走"开盘首挂耐心 + 最多一次重挂"，券商B保留快节奏。

- [ ] **Step 1: 实现账本复盘脚本（纯 stdlib，只读打开）**

```python
#!/usr/bin/env python3
"""只读复盘: 统计 state_db 的信号终态与委托尝试分布, 支撑分机调参。

用法: python tools/analyze_attempts.py --db data/gj.db
"""
import argparse
import sqlite3


def _tables(db_path: str):
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    status = conn.execute(
        "SELECT action, status, COUNT(*) n FROM signals "
        "GROUP BY action, status ORDER BY action, n DESC"
    ).fetchall()
    attempts = conn.execute(
        "SELECT attempt_no, COUNT(*) n FROM order_attempts "
        "GROUP BY attempt_no ORDER BY attempt_no"
    ).fetchall()
    relist = conn.execute(
        "SELECT COUNT(*) n FROM (SELECT signal_id FROM order_attempts "
        "GROUP BY signal_id HAVING MAX(attempt_no) >= 2)"
    ).fetchone()["n"]
    hourly = conn.execute(
        "SELECT substr(created_at, 12, 2) hh, COUNT(*) n FROM order_attempts "
        "GROUP BY hh ORDER BY hh"
    ).fetchall()
    conn.close()
    return status, attempts, relist, hourly


def main() -> None:
    parser = argparse.ArgumentParser(description="复盘执行账本")
    parser.add_argument("--db", default="data/miniqmt_follower.db")
    args = parser.parse_args()
    status, attempts, relist, hourly = _tables(args.db)
    print("== 信号终态分布 (action × status) ==")
    for row in status:
        print(f"  {row['action']:<4} {row['status']:<28} {row['n']:>6}")
    total_attempts = sum(r["n"] for r in attempts)
    print("\n== 委托尝试分布 (attempt_no) ==")
    for row in attempts:
        print(f"  第{row['attempt_no']}次挂单: {row['n']:>6} "
              f"({row['n'] / total_attempts * 100:.1f}%)")
    print(f"\n== 重挂指标 ==")
    print(f"  挂过 ≥2 次单的信号数: {relist}")
    print(f"  重挂尝试占比(第2次及以后 / 全部): "
          f"{sum(r['n'] for r in attempts if r['attempt_no'] >= 2) / total_attempts * 100:.1f}%")
    print("\n== 报单时刻分布 (按小时) ==")
    for row in hourly:
        print(f"  {row['hh']}:00 档: {row['n']:>6}")


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: 实现撤单确认耗时脚本（解析文件日志的毫秒时间戳）**

```python
#!/usr/bin/env python3
"""统计撤单确认耗时分布: 券商A开盘实测 ~16-18s, 是"撤单重挂能否成立"的命门。

用法: python tools/analyze_cancel_latency.py logs/<机器名>/miniqmt_follower.log
"""
import argparse
import re
from datetime import datetime

_TS = r"\[(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3})\]"
_REQ = re.compile(_TS + r".*🔙 撤单请求已提交 \| QMT单号=(\d+)")
_CONF = re.compile(
    _TS + r".*🔙 撤单终态已确认 \| broker单号=(\d+) 状态=\S+ 成交=(\d+)"
)


def main() -> None:
    parser = argparse.ArgumentParser(description="统计撤单确认耗时分布")
    parser.add_argument("logfile")
    args = parser.parse_args()
    pending: dict[str, datetime] = {}
    latencies: list[float] = []
    fills: list[int] = []
    with open(args.logfile, encoding="utf-8") as fh:
        for line in fh:
            m = _REQ.search(line)
            if m:
                pending[m.group(2)] = datetime.strptime(
                    m.group(1), "%Y-%m-%d %H:%M:%S.%f"
                )
                continue
            m = _CONF.search(line)
            if m and m.group(2) in pending:
                start = pending.pop(m.group(2))
                end = datetime.strptime(m.group(1), "%Y-%m-%d %H:%M:%S.%f")
                latencies.append((end - start).total_seconds())
                fills.append(int(m.group(3)))
    latencies.sort()
    print(f"样本数: {len(latencies)}")
    if latencies:
        print(
            f"确认耗时: 最小 {latencies[0]:.1f}s / "
            f"中位 {latencies[len(latencies) // 2]:.1f}s / "
            f"P90 {latencies[int(len(latencies) * 0.9)]:.1f}s / "
            f"最大 {latencies[-1]:.1f}s"
        )
        print(f"撤单期间捡回成交的样本数: {sum(1 for q in fills if q > 0)} / "
              f"总成交量: {sum(fills)}股")
    if pending:
        print(f"仍未确认的撤单: {len(pending)} 笔")


if __name__ == "__main__":
    main()
```

- [ ] **Step 3: 对真实数据跑通出基线**

Run: `python tools/analyze_attempts.py --db data/gj.db`、`--db data/hx.db`
Run: `python tools/analyze_cancel_latency.py logs/<机器名>/miniqmt_follower.log`、券商B同路径
Expected: 四段统计 + 撤单耗时分布。用 2026-08-14 券商A日志应能看到中位 ~16-18s（验证脚本正确性），券商B应为亚秒级。若本机无实盘文件，先用手工造的小库/样例日志验证格式，实盘在交易机上跑。

- [ ] **Step 4: 分机调参（依赖 Task 10/11 的配置项已落地）**

| 机器 | `order_timeout_sec` | `opening_order_timeout_sec` | `opening_aggressive_pct` / `opening_aggressive_window_sec` | `quote_max_age_sec` | `max_total_duration_sec` | 逻辑 |
|---|---|---|---|---|---|---|
| 券商A | 0.5（盘中不变） | 10 | 0.02 / 60 | 2 | 45 | 撤单确认 ~16s：开盘首挂耐心 10s；总预算 45s 允许恰好一次重挂（t=0 挂→t=10 撤→t≈26 确认→t=26 重挂→t=36 撤→t≈52 确认超预算收尾） |
| 券商B | 0.5（盘中不变） | 2 | 0.02 / 60 | 2 | 15 | 撤单确认快：保留快节奏循环，首挂 2s 足够 |

`config_gj.yaml` / `config_hx.yaml` 按表改（配置不进库），重启服务。

- [ ] **Step 5: Commit（脚本部分；配置不进库）**

```bash
git add tools/analyze_attempts.py tools/analyze_cancel_latency.py
git commit -m "feat(tools): 复盘脚本增加撤单确认耗时统计, 支撑分机调参"
```

**验收目标（Task 7）：**
- 两个脚本在两台交易机的账本与日志上跑通；撤单耗时脚本对 2026-08-14 券商A日志输出中位 16~18s（与人工观察一致）。
- 调参后下一个交易日复跑对比：开盘买单 `FILLED` 占比上升；重挂尝试占比不升（券商A开盘首挂 10s 内成交优先，撤单次数下降）；`FAILED_TIMEOUT` 占比下降。
- 若对比无改善：配置逐项回退（每项一行），无代码牵连。

---

### Task 8: 文档同步

**Files:** Modify `AGENTS.md`、`CLAUDE.md`

- [ ] **Step 1:** 在「Execution pipeline」的 Key design decisions 段落补七条一句话说明：行情取数退避重试、执行侧行情快照时效门控（`quote_max_age_sec`，仅连续竞价时段生效）、开盘首挂强化（激进报价窗口 + 首挂耐心窗口，按券商A/券商B分机调参）、收信主循环重活后台化、策略账户快照 1 秒缓存、跌停排队卖出专用线程池（含 `queue_future_for` 的 ACK 挂接与崩溃恢复已覆盖）、热路径日志阈值门控与 `log_file_level` 配置。
- [ ] **Step 2:** Commit

```bash
git add AGENTS.md CLAUDE.md
git commit -m "docs: 同步下单提速与可靠性优化的架构说明"
```

**验收目标（Task 8）：** 两份文档与代码行为一致，新接手者能据此理解新路径而不必翻 diff。

---

### Task 9: 热路径日志瘦身（人手发现的第 8 项）

**Files:**
- Modify: `miniqmt_follower/adapters/qmt.py`（4 处 ⏱️ 耗时日志改阈值门控、删 3 处"忽略回调"纯噪音日志）
- Modify: `miniqmt_follower/config.py`（新增 `log_file_level`）
- Modify: `miniqmt_follower/logging_config.py`（文件 handler 支持可配置级别）
- Modify: `miniqmt_follower/app.py`（传入 `config.log_file_level`）
- Modify: `config.example.yaml`（新增键与注释）
- Test: `tests/test_qmt_adapter_mapping.py`、`tests/test_yaml_config.py`（如现有用例断言了被删/被门控的日志行，同步调整断言，不调业务行为）

**改动说明（白话）：** 开盘时"每次查单写一行 ⏱️ 日志"这类记录没有任何信息量（0.5ms 的快调用不需要记），还每次同步写盘占时间。改成两条腿：① QMT 各调用（资金/报单/全量查单/撤单）的耗时日志只在"慢得可疑"（≥20ms）时才落盘——快调用零输出，慢调用保留，正好给 Task 5 的锁竞争诊断留信号；② 删掉"忽略非本服务回调"这类纯噪音行。另外给文件日志加一个级别旋钮 `log_file_level`（默认 DEBUG 不改现状），生产环境可设 INFO 再砍一刀。

- [ ] **Step 1: 写失败测试**

```python
# tests/test_qmt_adapter_mapping.py
class SlowCallLogTest(unittest.TestCase):
    def test_fast_call_produces_no_timing_log(self):
        records = self._capture_records("miniqmt_follower.adapters.qmt")
        _log_slow_call("QMT全量查单耗时 1笔", time.monotonic())  # 刚发生 → 快
        self.assertEqual([r for r in records if "⏱️" in r.getMessage()], [])

    def test_slow_call_logs_timing(self):
        records = self._capture_records("miniqmt_follower.adapters.qmt")
        _log_slow_call("QMT全量查单耗时 1笔", time.monotonic() - 0.1)  # 100ms 前开始
        self.assertTrue(any("⏱️" in r.getMessage() and "100" in r.getMessage()
                            for r in records))

    def test_ignore_callback_logs_removed(self):
        # _cache_owned_order_update 对非本服务订单: 只更新状态, 不再落日志
        ...
```

```python
# tests/test_yaml_config.py
class LogFileLevelTest(unittest.TestCase):
    def test_default_is_debug(self):
        cfg = load_config(self._tmp_yaml(redis={...}, machine_schedule={...}))
        self.assertEqual(cfg.log_file_level, "DEBUG")

    def test_invalid_level_rejected(self):
        with self.assertRaises(ValueError):
            load_config(self._tmp_yaml(..., log_file_level="VERBOSE"))
```

（`_capture_records` 用 `assertLogs` 或挂一个 capturing handler，按该文件既有日志测试手法。）

- [ ] **Step 2: 跑测试确认失败**

Run: `python -m unittest tests.test_qmt_adapter_mapping.SlowCallLogTest tests.test_yaml_config.LogFileLevelTest -v`
Expected: FAIL（`_log_slow_call` 不存在 / 无 `log_file_level` 键）

- [ ] **Step 3: 实现**

**3a. qmt.py** —— 模块级加常量与门控函数：

```python
_SLOW_CALL_LOG_THRESHOLD_SEC = 0.02


def _log_slow_call(label: str, started: float) -> None:
    """QMT 调用耗时只在"慢得可疑"时落日志: 正常毫秒级调用不再刷屏刷盘。"""
    if not logger.isEnabledFor(logging.DEBUG):
        return
    elapsed = time.monotonic() - started
    if elapsed >= _SLOW_CALL_LOG_THRESHOLD_SEC:
        logger.debug("⏱️ %s | %.1fms", label, elapsed * 1000)
```

四处替换：

| 位置 | 原文 | 改为 |
|---|---|---|
| `query_available_cash` (约行 576-582) | `logger.debug("⏱️ QMT资金查询耗时 \| %.1fms", ...)` | `_log_slow_call("QMT资金查询耗时", started)` |
| `submit_order` 成功 (约行 752-755) | `logger.debug("⏱️ QMT报单调用耗时 \| %s \| %.1fms", ...)` | `_log_slow_call(f"QMT报单调用耗时 {signal.label}", started)` |
| `_refresh_orders_cache` (约行 965-971) | `logger.debug("⏱️ QMT全量查单耗时 \| %.1fms \| %s笔", ...)` | `_log_slow_call(f"QMT全量查单耗时 {len(orders)}笔", started)` |
| `cancel_order` (约行 1067-1070) | `logger.debug("⏱️ QMT撤单调用耗时 \| QMT单号=%s \| %.1fms \| 返回=%s", ...)` | `_log_slow_call(f"QMT撤单调用耗时 QMT单号={order_id} 返回={cancel_result}", started)` |

`submit_order` 的失败 debug 行保留（失败是稀有事件，且异常随后被上层记录）。删除三处纯噪音 debug 行：`_cache_owned_order_update` 的"忽略非本服务订单回调"、`_cache_owned_trade_update` 的"忽略非本服务成交回调"与"忽略重复成交回调"（保留两处"已采信"日志，它们只对本服务订单触发且有核对价值）。

**3b. config.py** —— `RuntimeConfig` 加字段 `log_file_level: str = "DEBUG"`；`load_config` 中：

```python
log_file_level=_validated_log_level(raw.get("log_file_level", "DEBUG")),
```

新增校验函数：

```python
def _validated_log_level(raw_value: object) -> str:
    """日志级别白名单校验, 写错直接启动失败而不是静默丢日志。"""
    level = str(raw_value or "DEBUG").strip().upper()
    if level not in {"DEBUG", "INFO", "WARNING", "ERROR"}:
        raise ValueError(
            f"log_file_level 取值非法: {raw_value!r} (允许: DEBUG/INFO/WARNING/ERROR)"
        )
    return level
```

**3c. logging_config.py** —— `setup_logging(level=logging.INFO, log_dir="logs", file_level=logging.DEBUG)`，`file_handler.setLevel(file_level)`（原来是写死 `DEBUG`）。

**3d. app.py** —— `setup_logging(level=config.log_level, log_dir=config.log_dir, file_level=config.log_file_level)`。

**3e. config.example.yaml** —— `log_level` 附近加：

```yaml
log_level: INFO # 控制台日志级别；文件日志级别见 log_file_level。
log_file_level: DEBUG # 文件日志级别。生产环境可设 INFO 进一步减少写盘;
                       # 排障时改回 DEBUG 拿到完整明细。热路径的 ⏱️ 耗时
                       # 日志另有 20ms 阈值门控, 与这里无关。
```

- [ ] **Step 4: 跑测试 + 全量回归（重点：清理旧用例对 ⏱️ 日志行的断言）**

Run: `python -m unittest tests.test_qmt_adapter_mapping tests.test_yaml_config -v`
若个别旧用例断言了"每次调用必打 ⏱️ 行"或"忽略回调必打日志"，把断言改成新行为（快调用无日志/慢调用有日志），业务断言一律不动。
Run: `python -m unittest discover -v` → 全绿
Run: `python -m compileall miniqmt_follower` → 无错

- [ ] **Step 5: Commit**

```bash
git add miniqmt_follower/adapters/qmt.py miniqmt_follower/config.py \
        miniqmt_follower/logging_config.py miniqmt_follower/app.py config.example.yaml
git commit -m "perf(logging): QMT耗时日志改20ms阈值门控并删纯噪音行, 文件日志级别可配"
```

**验收目标（Task 9）：**
- 测试层面：快调用（<20ms）零 ⏱️ 日志；慢调用（≥20ms）仍记录耗时；"忽略回调"日志不再出现；`log_file_level` 非法值启动即报错。
- 线上预期：开盘时段日志文件写入量大幅下降——改动前后同窗口对比，⏱️ QMT 耗时行从数千行/日降到接近 0（只保留真慢调用）；你举例的 `⏱️ QMT全量查单耗时 | 0.5ms | 1笔` 不再落盘。同步写盘对热路径的时间占用趋零。慢调用告警恰好为 Task 5 的锁竞争诊断保留信号（正常 <20ms、抢锁排队 >20ms 会显形）。
- 进一步可选：生产配置 `log_file_level: INFO` 再砍掉全部 DEBUG 明细，排障时改回。
- 回滚方式：`git revert` 该 commit；或 `_SLOW_CALL_LOG_THRESHOLD_SEC` 改 0 恢复全量记录。

---

### Task 10: 执行侧行情快照时效门控

**Files:**
- Modify: `miniqmt_follower/models.py`（`Quote` 加 `quote_time`、`ExecutionConfig` 加 `quote_max_age_sec`）
- Modify: `miniqmt_follower/adapters/qmt.py`（`latest_quote` 填充 `quote_time`）
- Modify: `miniqmt_follower/config.py`（加载与校验）
- Modify: `miniqmt_follower/executor.py`（`_latest_quote_with_retry` 拆出时效门控）
- Test: `tests/test_executor.py`、`tests/test_yaml_config.py`

**改动说明（白话）：** 2026-08-14 天洋新材的教训：09:30:00.016 取到"卖一 10.20"，我们 4ms 后挂 10.23（高出 3 个 tick）却 0.83 秒零成交——最可能的解释是这份快照来自 9:25 的旧盘口，开盘第一秒盘口已经跑了。策略引擎那边早就要求行情不能超 3 秒（`max_tick_age_sec`），下单路径反而没这个检查。给 `Quote` 带上行情时间戳，连续竞价时段取到"太旧"的快照就重取（有界 ~1 秒），还拿不到新鲜的就带告警提交（不判死信号——行情冻结不能变成整批废单）。盘前/集合竞价时段不检查（那个时段的快照本来就不动）。

- [ ] **Step 1: 写失败测试**

```python
# tests/test_executor.py —— 注意: 时效门控只在"连续竞价时段"生效,
# 测试里要 monkeypatch executor_module._in_continuous_session 恒 True。
def _quote(age_sec):
    qtime = None if age_sec is None else dt.datetime.now() - dt.timedelta(seconds=age_sec)
    return Quote(last_price=10.0, ask1=10.01, bid1=9.99, quote_time=qtime)


class _SequencedMarketData:
    """按序吐快照, 之后重复最后一张; 记录调用次数。"""

    def __init__(self, quotes):
        self._quotes = list(quotes)
        self.calls = 0

    def latest_quote(self, code):
        self.calls += 1
        return self._quotes[min(self.calls, len(self._quotes)) - 1]

    def instrument_name(self, code):
        return None


class QuoteFreshnessTest(unittest.TestCase):
    def test_stale_quote_is_refreshed_before_submit(self):
        engine = self._build_engine()
        engine.config = replace(engine.config, quote_max_age_sec=2.0)
        engine.market_data = _SequencedMarketData([_quote(5.0), _quote(5.0), _quote(0.0)])
        result = engine.execute(self._buy_signal("000001.XSHE", 100))
        self.assertEqual(result.status, ExecutionStatus.FILLED)
        self.assertEqual(engine.market_data.calls, 3)   # 超龄两次 → 重取到新鲜

    def test_never_fresh_submits_with_warning(self):
        engine = self._build_engine()
        engine.config = replace(engine.config, quote_max_age_sec=2.0)
        engine.market_data = _SequencedMarketData([_quote(99.0)] * 30)
        result = engine.execute(self._buy_signal("000001.XSHE", 100))
        self.assertEqual(result.status, ExecutionStatus.FILLED)  # 带告警提交, 不判死
        self.assertEqual(engine.market_data.calls, 11)           # 1 + 10 次有界重取

    def test_quote_time_none_skips_gate(self):
        engine = self._build_engine()
        engine.config = replace(engine.config, quote_max_age_sec=2.0)
        engine.market_data = _SequencedMarketData([_quote(None)])
        result = engine.execute(self._buy_signal("000001.XSHE", 100))
        self.assertEqual(result.status, ExecutionStatus.FILLED)
        self.assertEqual(engine.market_data.calls, 1)
```

- [ ] **Step 2: 跑测试确认失败**

Run: `python -m unittest tests.test_executor.QuoteFreshnessTest -v`
Expected: FAIL（`Quote` 无 `quote_time` / 无重取逻辑，calls==1）

- [ ] **Step 3: 实现**

**3a. models.py** —— 顶部加 `import datetime as dt`；`Quote` 加字段：

```python
    # 行情快照时间(本地时间); None 表示行情源未提供, 时效门控自动跳过。
    quote_time: dt.datetime | None = None
```

`ExecutionConfig` 加字段：

```python
    # 执行侧行情快照时效门控: 连续竞价时段取到的快照超过该秒数时重取(有界),
    # 仍超龄则带告警提交; 0 = 关闭。与策略侧 max_tick_age_sec 互补,
    # 这里是下单路径的守门人(教训: 开盘用 9:25 旧盘口报价, 挂单永远追不上)。
    quote_max_age_sec: float = 0.0
```

**3b. qmt.py** —— `latest_quote` 末尾填充时间戳（复用现有 `_quote_datetime`）：

```python
        quote_time = _quote_datetime(
            tick.get("time") or tick.get("timetag") or tick.get("stime")
        )
        return Quote(
            last_price=last_price,
            ask1=_first_book_level(tick.get("askPrice")),
            bid1=_first_book_level(tick.get("bidPrice")),
            high_limit=high_limit,
            low_limit=low_limit,
            quote_time=quote_time,
        )
```

**3c. config.py** —— 加通用校验并接线：

```python
def _validated_non_negative_float(raw_value: object, field: str) -> float:
    """非负浮点校验, 用于"0 表示关闭"的开关类参数。"""
    value = float(raw_value)
    if value < 0:
        raise ValueError(f"{field} 不能为负数: {raw_value!r}")
    return value
```

`load_config` 中 `ExecutionConfig(...)` 加：

```python
            quote_max_age_sec=_validated_non_negative_float(
                execution_raw.get("quote_max_age_sec", 0.0),
                "execution.quote_max_age_sec",
            ),
```

**3d. executor.py** —— 模块级常量与函数：

```python
_QUOTE_RETRY_MAX = 3
_QUOTE_RETRY_DELAY_SEC = 0.2
# 连续竞价时段行情快照超龄时的有界重取次数与间隔(合计约 1 秒)。
_QUOTE_STALE_RETRY_MAX = 10
_QUOTE_STALE_RETRY_DELAY_SEC = 0.1


def _in_continuous_session(machine_schedule: MachineScheduleConfig) -> bool:
    """时效门控只在连续竞价时段生效: 盘前/集合竞价快照本来就不更新。"""
    session = machine_schedule.market_session
    now = dt.datetime.now().time()
    return (
        session.continuous_trading_start_at
        <= now
        < session.closing_call_auction_start_at
    )
```

`_latest_quote_with_retry` 改为（原错误重试逻辑保留，成功路径交给时效门控）：

```python
    def _latest_quote_with_retry(self, signal: TradeSignal) -> Quote:
        """取行情快照: 失败退避重试; 连续竞价时段对超龄快照做有界重取。"""
        last_error: Exception | None = None
        for attempt in range(1, _QUOTE_RETRY_MAX + 1):
            try:
                quote = self.market_data.latest_quote(signal.code)
            except Exception as exc:
                last_error = exc
                logger.warning(
                    "%s | %s | 行情取数失败 第%d/%d次 | %s",
                    signal.console_event("重试"), signal.display_code,
                    attempt, _QUOTE_RETRY_MAX, exc,
                )
                if attempt < _QUOTE_RETRY_MAX:
                    time.sleep(_QUOTE_RETRY_DELAY_SEC)
                continue
            return self._refresh_stale_quote(signal, quote)
        raise last_error  # type: ignore[misc]

    def _refresh_stale_quote(self, signal: TradeSignal, quote: Quote) -> Quote:
        """快照超龄时有界重取; 仍不新鲜则带告警提交, 不让时效门控杀死信号。"""
        max_age = self.config.quote_max_age_sec
        if (
            max_age <= 0
            or quote.quote_time is None
            or not _in_continuous_session(self.machine_schedule)
        ):
            return quote
        for attempt in range(1, _QUOTE_STALE_RETRY_MAX + 1):
            age = (dt.datetime.now() - quote.quote_time).total_seconds()
            if age <= max_age:
                if attempt > 1:
                    logger.debug(
                        "%s | %s | 行情快照已更新 | 第%d次重取 | 时效 %.2fs",
                        signal.console_event("重试"), signal.display_code,
                        attempt, age,
                    )
                return quote
            logger.warning(
                "%s | %s | 行情快照超龄 %.1fs(>%ss) | 第%d/%d次重取",
                signal.console_event("重试"), signal.display_code,
                age, max_age, attempt, _QUOTE_STALE_RETRY_MAX,
            )
            time.sleep(_QUOTE_STALE_RETRY_DELAY_SEC)
            try:
                quote = self.market_data.latest_quote(signal.code)
            except Exception as exc:
                logger.warning(
                    "%s | %s | 快照重取失败, 保留现有快照 | %s",
                    signal.console_event("重试"), signal.display_code, exc,
                )
                return quote
        logger.warning(
            "%s | %s | 行情快照仍超龄 | 带告警提交",
            signal.console_event("重试"), signal.display_code,
        )
        return quote
```

- [ ] **Step 4: 跑测试 + 全量回归**

Run: `python -m unittest tests.test_executor.QuoteFreshnessTest -v` → PASS
Run: `python -m unittest discover -v` → 全绿（`quote_time=None` 的旧 fake 不受影响，默认 0 关闭）
Run: `python -m compileall miniqmt_follower` → 无错

- [ ] **Step 5: Commit**

```bash
git add miniqmt_follower/models.py miniqmt_follower/adapters/qmt.py \
        miniqmt_follower/config.py miniqmt_follower/executor.py
git commit -m "feat(executor): 执行侧行情快照时效门控, 开盘不再用旧盘口报价"
```

**验收目标（Task 10）：**
- 测试层面：超龄快照被重取到新鲜后才报价；始终超龄则带告警提交不判死；`quote_time=None` / 门控关闭（默认 0）行为与现状完全一致。
- 线上预期（配合 Task 11）：开盘首挂基于真实 09:30 盘口报价，不再出现"挂单高于快照卖一 3 tick 仍 0.83s 零成交"的旧盘口追价问题；日志出现「行情快照超龄…重取」时即可证明门控在工作。
- 回滚方式：`quote_max_age_sec` 配置改 0，或 revert 该 commit。

---

### Task 11: 开盘首挂强化（激进报价窗口 + 首挂耐心窗口）

**Files:**
- Modify: `miniqmt_follower/models.py`（`ExecutionConfig` 加 3 个字段）
- Modify: `miniqmt_follower/config.py`（加载与校验）
- Modify: `miniqmt_follower/pricing.py`（`_auction_queue_price` 增加开盘窗口分支）
- Modify: `miniqmt_follower/executor.py`（`_first_attempt_timeout` + `_wait_for_terminal_or_timeout` 支持显式超时）
- Test: `tests/test_pricing.py`、`tests/test_executor.py`

**改动说明（白话）：** 对"首挂即成交优先"，开盘有两件事要做：① 报价更激进——连续竞价开始后的前 `opening_aggressive_window_sec`（默认 60s）内，沿用竞价那套"最新价 ± opening_aggressive_pct"的排队报价（默认 2%，强制夹进涨跌停带），买时间优先；② 首挂更耐心——券商A撤单确认要 ~16 秒，0.5s 超时等于"首挂即撤"，所以开盘窗口内的买单首笔委托等 `opening_order_timeout_sec`（券商A建议 10s）再撤。两个开关默认全关，老配置零变化。盘前卖单的首挂语义保持不变（距开盘 + 0.5s 宽限）。

> **问题 4 已拍板（选 A）**：维持"买等全部盘前卖单终态"的原语义，本任务不改变。券商A上若某天盘前卖单开盘未成交，买单最坏晚 ~17s 进场，接受该代价；先用 Task 7 的撤单耗时脚本观察几轮，真有"卖单拖累买单"再另议。

- [ ] **Step 1: 写失败测试**

```python
# tests/test_pricing.py
class OpeningAggressiveTest(unittest.TestCase):
    def test_opening_window_uses_opening_pct(self):
        # setUpModule 已 monkeypatch pricing._in_call_auction=False;
        # 这里再 monkeypatch pricing._in_opening_window=True。
        config = replace(self.config, auction_aggressive_pct=0.0,
                         opening_aggressive_pct=0.02, opening_aggressive_window_sec=60.0)
        quote = Quote(last_price=10.0, ask1=10.01, bid1=9.99,
                      high_limit=11.0, low_limit=9.0)
        price = calculate_order_price(self.buy_signal, quote, config, self.schedule)
        self.assertAlmostEqual(price, 10.2, places=3)   # 10 × 1.02, 笼子内

    def test_opening_pct_zero_falls_back_to_normal(self):
        config = replace(self.config, opening_aggressive_pct=0.0)
        price = calculate_order_price(self.buy_signal, self.quote, config, self.schedule)
        self.assertAlmostEqual(price, self.book_price, places=3)  # 与现有 book 用例同值
```

```python
# tests/test_executor.py
class OpeningFirstShotTest(unittest.TestCase):
    def test_opening_buy_first_attempt_waits_longer(self):
        # monkeypatch executor_module._in_opening_timeout_window → True
        # config: order_timeout_sec=0.5, opening_order_timeout_sec=5,
        #         opening_aggressive_window_sec=60, max_total_duration_sec=15
        # FakeBroker: 挂单后在 t≈2s 变 FILLED(时间驱动 fake)
        result = engine.execute(buy_signal)
        self.assertEqual(result.status, ExecutionStatus.FILLED)
        self.assertEqual(self.fake_broker.cancel_calls, 0)   # 2s 成交, 5s 前绝不撤

    def test_unfilled_opening_buy_cancels_after_opening_timeout(self):
        # FakeBroker 一直 OPEN; max_total_duration_sec 放大到 20
        result = engine.execute(buy_signal)
        self.assertGreaterEqual(self.fake_broker.cancel_at - self.fake_broker.submit_at, 5.0)

    def test_opening_timeout_zero_keeps_legacy_behavior(self):
        # opening_order_timeout_sec=0 → 0.5s 即撤(与旧用例一致)
```

（时间驱动 FakeBroker 按该文件既有 fake 风格实现：`get_order_snapshot` 里按 `time.monotonic() - submit_at` 决定返回 OPEN/FILLED，并记录 `cancel_calls`/`cancel_at`。）

- [ ] **Step 2: 跑测试确认失败**

Run: `python -m unittest tests.test_pricing.OpeningAggressiveTest tests.test_executor.OpeningFirstShotTest -v`
Expected: FAIL（无 `opening_aggressive_pct` 配置 / 首挂仍按 0.5s 撤）

- [ ] **Step 3: 实现**

**3a. models.py** —— `ExecutionConfig` 加：

```python
    # 开盘首挂强化(首挂即成交优先): 连续竞价开始后 opening_aggressive_window_sec
    # 秒内, 报价沿用竞价激进逻辑(最新价 ± opening_aggressive_pct, 夹涨跌停带);
    # 0 关闭。窗口内 BUY 首笔委托等待 opening_order_timeout_sec 秒才撤,
    # 0 = 用 order_timeout_sec。券商A撤单确认 ~16s, 首挂必须耐心; 券商B撤单快可设小值。
    opening_aggressive_pct: float = 0.0
    opening_aggressive_window_sec: float = 60.0
    opening_order_timeout_sec: float = 0.0
```

**3b. config.py** —— `load_config` 中 `ExecutionConfig(...)` 加：

```python
            opening_aggressive_pct=_validated_non_negative_float(
                execution_raw.get("opening_aggressive_pct", 0.0),
                "execution.opening_aggressive_pct",
            ),
            opening_aggressive_window_sec=_validated_non_negative_float(
                execution_raw.get("opening_aggressive_window_sec", 60.0),
                "execution.opening_aggressive_window_sec",
            ),
            opening_order_timeout_sec=_validated_non_negative_float(
                execution_raw.get("opening_order_timeout_sec", 0.0),
                "execution.opening_order_timeout_sec",
            ),
```

**3c. pricing.py** —— 新增模块级函数（测试可 monkeypatch）：

```python
def _in_opening_window(
    machine_schedule: MachineScheduleConfig, window_sec: float
) -> bool:
    """连续竞价开始后的前 window_sec 秒内返回 True; window<=0 恒 False。"""
    if window_sec <= 0:
        return False
    session = machine_schedule.market_session
    now = dt.datetime.now()
    if now.time() < session.continuous_trading_start_at:
        return False
    open_dt = now.replace(
        hour=session.continuous_trading_start_at.hour,
        minute=session.continuous_trading_start_at.minute,
        second=session.continuous_trading_start_at.second,
        microsecond=0,
    )
    return 0 <= (now - open_dt).total_seconds() <= window_sec
```

`_auction_queue_price` 开头改为：

```python
    if config.auction_aggressive_pct > 0 and _in_call_auction(machine_schedule):
        aggressive_pct = config.auction_aggressive_pct
        mode_label = "auction"
    elif config.opening_aggressive_pct > 0 and _in_opening_window(
        machine_schedule, config.opening_aggressive_window_sec
    ):
        aggressive_pct = config.opening_aggressive_pct
        mode_label = "opening"
    else:
        return None
```

其后两处 `config.auction_aggressive_pct` 换成 `aggressive_pct`，debug 日志文案带 `mode_label`（"竞价/开盘"）。

**3d. executor.py** —— 模块级：

```python
def _in_opening_timeout_window(
    machine_schedule: MachineScheduleConfig, window_sec: float
) -> bool:
    """首挂耐心窗口: 连续竞价开始后的前 window_sec 秒。"""
    if window_sec <= 0:
        return False
    session = machine_schedule.market_session
    now = dt.datetime.now()
    if now.time() < session.continuous_trading_start_at:
        return False
    open_dt = now.replace(
        hour=session.continuous_trading_start_at.hour,
        minute=session.continuous_trading_start_at.minute,
        second=session.continuous_trading_start_at.second,
        microsecond=0,
    )
    return 0 <= (now - open_dt).total_seconds() <= window_sec
```

类内新增：

```python
    def _first_attempt_timeout(self, signal: TradeSignal) -> float | None:
        """首笔委托的轮询超时; None 表示走默认 order_timeout_sec(+竞价顺延)。

        - 盘前卖单保持原有语义: 距开盘秒数 + 0.5s 报告宽限;
        - 开盘窗口内的买单: opening_order_timeout_sec(>0 时) ——
          首挂即决战, 券商A 16s 撤单确认下 0.5s 就撤等于放弃。
        """
        if is_preopen_sell(signal, self.machine_schedule):
            return (
                _seconds_until_market_open(self.machine_schedule)
                + _OPENING_SELL_RECONCILE_GRACE_SEC
            )
        if (
            signal.action == Action.BUY
            and self.config.opening_order_timeout_sec > 0
            and _in_opening_timeout_window(
                self.machine_schedule, self.config.opening_aggressive_window_sec
            )
        ):
            return self.config.opening_order_timeout_sec
        return None
```

`_wait_for_terminal_or_timeout` 签名改为显式超时（去掉 `opening_sell_first_attempt`）：

```python
    def _wait_for_terminal_or_timeout(
        self, order_id: str, *, timeout: float | None = None,
    ) -> OrderSnapshot:
        """轮询订单直到终态或单次委托超时。

        timeout=None 时用 order_timeout_sec + 竞价顺延; 显式传入则覆盖
        (开盘首挂耐心窗口用)。前 1 秒快速轮询、之后降速的策略不变。
        """
        if timeout is None:
            timeout = self.config.order_timeout_sec + _seconds_until_market_open(
                self.machine_schedule
            )
        deadline = time.monotonic() + timeout
        ...
```

`execute()` 主循环里原调用（约行 873）改为：

```python
            first_timeout = self._first_attempt_timeout(signal) if attempts == 1 else None
            snapshot = self._wait_for_terminal_or_timeout(order_id, timeout=first_timeout)
```

- [ ] **Step 4: 跑测试 + 全量回归**

Run: `python -m unittest tests.test_pricing tests.test_executor -v` → PASS
旧用例中若有直接传 `opening_sell_first_attempt=True` 调用 `_wait_for_terminal_or_timeout` 的，改为 `timeout=<原期望秒数>`。
Run: `python -m unittest discover -v` → 全绿（三个新参数默认全关，老行为不变）
Run: `python -m compileall miniqmt_follower` → 无错

- [ ] **Step 5: Commit**

```bash
git add miniqmt_follower/models.py miniqmt_follower/config.py \
        miniqmt_follower/pricing.py miniqmt_follower/executor.py
git commit -m "feat(executor): 开盘首挂强化, 激进报价窗口与首挂耐心窗口可配"
```

**验收目标（Task 11）：**
- 测试层面：开盘窗口内买单首挂等待 `opening_order_timeout_sec` 才撤（期间绝不撤单）；报价 = 最新价×(1+opening_aggressive_pct) 且夹进涨跌停带；三个参数为 0 时与现状逐字节一致。
- 线上预期（配合 Task 10/7 调参）：开盘买单基于新鲜盘口 + 激进报价首挂，券商A首挂 10s 内成交占比显著上升；「首笔委托耗时」保持毫秒级；撤单次数下降（首挂即成交优先的必然结果）。
- 回滚方式：三个配置项全部设 0（或 revert 该 commit）。



**Spec coverage:** 前两档 7 项全部有对应任务（发送端兜底=T1、行情重试=T2、收信减负=T3、Redis 内网=T4、账户缓存=T5、排队单专用池=T6、复盘+调参=T7），人手补充的第 8 项日志噪音=T9，实盘日志分析后新增的行情时效门控=T10、开盘首挂强化=T11，另加文档同步 T8。第三档（熔断粘性、同秒去重）按约定未纳入；问题 4 已拍板选 A（维持先卖后买语义，无改动）。

**Placeholder scan:** 无 TBD/TODO；测试代码给出完整用例（T3 的 Event 阻塞用例留了装配要点说明，因依赖现有 tests/test_runtime.py 的 Fake 装配，实现时按该文件既有线程测试风格落地；T11 的时间驱动 FakeBroker 按 test_executor.py 既有 fake 风格落地）。

**Type consistency:** `_run_queued_sell` 参数序列在定义与提交处一致；`queue_future_for(signal_id) -> Future | None` 在 executor 与 app 两端签名一致；`_wait_for_terminal_or_timeout(order_id, *, timeout: float | None = None)` 的新签名在 execute/recover 两处调用点一致；`ExecutionStatus` 复用既有枚举无新类型；`Quote.quote_time: dt.datetime | None` 在 qmt.py 构造与测试 fake 两侧一致（None 时门控自动跳过）。

## Execution Handoff

计划已保存。执行顺序建议：T1→T2→T3→T5→T9 可连续做（互不依赖的快速加固），T10→T11 是开盘专项（先做 10 再做 11，11 依赖 10 的新鲜快照才完整生效），T6 改动最大放后面，T7（含撤单耗时复盘）依赖 T10/T11 的配置项落地后调参，T4 部署动作可并行安排，T8 收尾。每任务独立提交、独立可回滚。

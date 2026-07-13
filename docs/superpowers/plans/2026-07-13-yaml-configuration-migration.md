# YAML Configuration Migration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make YAML the only runtime configuration format, with native comments in the tracked template and unchanged execution semantics.

**Architecture:** `load_config()` accepts only `.yaml` and `.yml` paths, parses their root mapping with `yaml.safe_load`, then keeps the existing dataclass construction and `${ENV_VAR}` password resolution. The CLI and operator documents point to `config.yaml`; the JSON signal payload contract remains unchanged.

**Tech Stack:** Python 3, PyYAML, stdlib `unittest`, pip.

## Global Constraints

- Preserve every existing configuration key, default value, environment-variable expansion, and dataclass type conversion.
- Do not change Redis signal JSON, QMT execution, FIFO behavior, pricing, risk controls, SQLite state, or local ignored `config.json` files.
- Accept only `.yaml` and `.yml` configuration paths; reject `.json` with a clear `ValueError`.
- Add no parser fallback and no dependencies beyond `PyYAML`.

---

### Task 1: Specify YAML loader behavior with regression tests

**Files:**
- Create: `tests/test_yaml_config.py`

**Interfaces:**
- Consumes: `qmt_follower.config.load_config(path: str | Path) -> RuntimeConfig`
- Produces: executable expectations for valid YAML, unsupported JSON paths, and the checked-in YAML template.

- [ ] **Step 1: Write the failing YAML-load and extension-rejection tests**

```python
import yaml

def test_config_loads_yaml_values_and_env_password(self):
    with tempfile.TemporaryDirectory() as tmpdir:
        config_path = Path(tmpdir) / "config.yaml"
        config_path.write_text(
            """redis:
  host: 127.0.0.1
  port: 6379
  password: ${TEST_REDIS_PASSWORD}
  stream: signals
  group: executors
  consumer: worker-1
  block_ms: 25
execution:
  buy_slippage_pct: 0.005
  max_attempts: 5
state_db: state.db
""",
            encoding="utf-8",
        )
        os.environ["TEST_REDIS_PASSWORD"] = "secret"
        config = load_config(config_path)
    self.assertEqual(config.redis.password, "secret")
    self.assertEqual(config.execution.max_attempts, 5)

def test_config_rejects_non_yaml_path(self):
    with tempfile.TemporaryDirectory() as tmpdir:
        config_path = Path(tmpdir) / "config.json"
        config_path.write_text('{"redis": {"host": "127.0.0.1"}}', encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "YAML"):
            load_config(config_path)

def test_example_yaml_is_a_mapping_without_comment_keys(self):
    raw = yaml.safe_load((Path(__file__).parents[1] / "config.example.yaml").read_text(encoding="utf-8"))
    self.assertIn("redis", raw)
    self.assertIn("execution", raw)
    self.assertNotIn("_comment", raw)
```

- [ ] **Step 2: Run the focused test module to verify the old JSON behavior fails**

Run: `python -m unittest tests.test_runtime -v`

Expected: the YAML template test errors because `config.example.yaml` does not exist, and the JSON-path test fails because the current loader accepts JSON.

### Task 2: Implement strict YAML configuration loading

**Files:**
- Modify: `qmt_follower/config.py`

**Interfaces:**
- Consumes: YAML root mapping from a `.yaml` or `.yml` file.
- Produces: unchanged `RuntimeConfig` objects or a `ValueError` explaining invalid extension/root shape.

- [ ] **Step 1: Replace the JSON import and parser with the YAML implementation**

```python
import os
from dataclasses import dataclass
from pathlib import Path

import yaml

def load_config(path: str | Path) -> RuntimeConfig:
    """从 YAML 配置文件加载运行参数。"""
    config_path = Path(path)
    if config_path.suffix.lower() not in {".yaml", ".yml"}:
        raise ValueError(f"配置文件必须使用 YAML 格式（.yaml 或 .yml）: {config_path}")
    with config_path.open("r", encoding="utf-8") as fh:
        raw = yaml.safe_load(fh)
    if not isinstance(raw, dict):
        raise ValueError(f"YAML 配置根节点必须是映射: {config_path}")
    # Keep the existing redis_raw/execution_raw/trading_raw construction below unchanged.
```

- [ ] **Step 2: Keep pre-existing untracked runtime tests out of the migration commit; use them only as a local compatibility check after changing their temporary paths to `config.yaml`.**

- [ ] **Step 3: Run focused runtime tests to verify existing configuration construction remains green**

Run: `python -m unittest tests.test_runtime -v`

Expected: all `RuntimeTests` pass, including YAML parsing, `${TEST_REDIS_PASSWORD}` expansion, template parsing, and JSON extension rejection.

### Task 3: Replace the example file and document the dependency

**Files:**
- Delete: `config.example.json`
- Create: `config.example.yaml`
- Modify: `README.md`

**Interfaces:**
- Consumes: existing configuration field names and defaults.
- Produces: a copyable, comments-first YAML template and an install command that includes `PyYAML`.

- [ ] **Step 1: Create `config.example.yaml` with all current runtime fields and native comments**

```yaml
# QMT 跟单助手示例配置。复制为 config.yaml 后按你的环境修改。
redis:
  # Redis Stream 配置。聚宽侧用 XADD 写入 stream，Windows 端用 consumer group 消费。
  host: YOUR_REDIS_HOST
  port: 6379
  password: ${REDIS_PASSWORD} # 本地无密码 Redis 可改为 null。
  stream: tidal_quant_signals # 必须与聚宽侧函数里的 stream 保持一致。
  group: qmt_executors
  consumer: win-qmt-01
  block_ms: 20 # 10-50ms 响应较快；数值越小 CPU 空轮询越多。

# 订单执行参数。修改后重启 python main.py 生效。
execution:
  buy_slippage_pct: 0.003
  sell_slippage_pct: 0.003
  order_timeout_sec: 3
  max_attempts: 3
  max_total_duration_sec: 15
  max_deviation_from_signal_price_pct: 0.02
  poll_interval_sec: 0.2
  pricing_mode: slippage # slippage=最新价加减固定滑点；book=按对手盘加减 tick。
  book_tick_offset: 2 # 股票 tick=0.01，ETF/基金 tick=0.001。

market_data:
  # 启动时预订阅的聚宽格式代码列表，例如 000001.XSHE、510300.XSHG。
  pre_subscribe_codes: []

trading:
  # false 时 QMT 适配器拒绝启动，防止误下单。
  enabled: false
  account_id: YOUR_ACCOUNT_ID
  miniqmt_path: 'C:\\path\\to\\userdata_mini'
  session_id: 0
  strategy_name: tidal_quant

state_db: data/qmt_follower.db
log_level: INFO # 文件日志始终记录 DEBUG 明细。
log_dir: logs
```

- [ ] **Step 2: Update README's dependency and copy instructions**

```text
pip install redis xtquant PyYAML
copy config.example.yaml config.yaml
```

- [ ] **Step 3: Run the template and runtime tests**

Run: `python -m unittest tests.test_runtime -v`

Expected: the native-comment template is parsed successfully without `_comment` mapping keys.

### Task 4: Align CLI and operator-facing references

**Files:**
- Modify: `qmt_follower/app.py`
- Modify: `qmt_follower/adapters/qmt.py`
- Modify: `AGENTS.md`
- Modify: `docs/windows-qmt-adapter-handoff.md`
- Modify: `docs/joinquant-community-promo.md`

**Interfaces:**
- Consumes: the new `config.example.yaml` and default `config.yaml` convention.
- Produces: consistent command-line help, safety errors, and setup instructions.

- [ ] **Step 1: Change the CLI default and help text**

```python
parser.add_argument("--config", default="config.yaml", help="YAML 配置文件路径")
```

- [ ] **Step 2: Replace operator configuration references without altering Redis signal JSON references**

```text
Copy-Item config.example.yaml config.yaml
python .\main.py --config config.yaml --workers 1
```

Keep descriptions of Redis Stream `payload` JSON unchanged, because only the local runtime configuration format is migrating.

- [ ] **Step 3: Update the QMT safety-gate error to name `config.yaml`**

```python
"trading.enabled is false. Set it to true in config.yaml to enable live trading."
```

- [ ] **Step 4: Run reference scans and the focused tests**

Run: `rg -n "config\.example\.json|config\.json|JSON 配置文件路径" README.md AGENTS.md docs qmt_follower tests`

Expected: remaining JSON mentions are limited to Redis Stream payloads and JoinQuant signal serialization; no runtime configuration instruction names `config.json`.

Run: `python -m unittest tests.test_runtime tests.test_main_entrypoint -v`

Expected: runtime configuration and CLI default tests pass.

### Task 5: Verify the completed migration

**Files:** all intended YAML migration files.

- [ ] **Step 1: Run the smallest complete verification set**

Run: `python -m unittest tests.test_runtime tests.test_main_entrypoint -v`

Expected: zero failures and zero errors.

Run: `python -m compileall qmt_follower tests`

Expected: exit code 0.

Run: `git diff --check`

Expected: no whitespace errors.

- [ ] **Step 2: Review scope before handoff**

Run: `git diff --name-only`

Expected: only the YAML loader, template, focused tests, and configuration documentation are changed by this migration; pre-existing unrelated changes remain untouched.

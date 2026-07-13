# Script Redis YAML Configuration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Move both signal sender scripts' Redis targets into one private YAML file and give them a shared, validated configuration loader.

**Architecture:** `scripts/redis_target_config.py` owns UTF-8 YAML loading and target validation. Both sender entrypoints import that loader directly, while `scripts/redis_targets.yaml` contains the local machine's real target values and is precisely ignored by Git.

**Tech Stack:** Python 3, PyYAML already used by the project, stdlib `pathlib` and `unittest`.

## Global Constraints

- Preserve both existing script commands, confirmation prompts, payloads, batch order, and send interval.
- Do not change the QMT runtime or Redis Stream contract.
- Do not print passwords.
- Do not add dependencies.
- Store real credentials only in `scripts/redis_targets.yaml` and ignore that exact file in Git.
- The current `scripts/` and tests contain pre-existing untracked work; do not create an implementation commit that would capture unrelated baseline content.

---

### Task 1: Shared YAML target loader

**Files:**
- Create: `scripts/redis_target_config.py`
- Create: `scripts/redis_targets.yaml`
- Modify: `.gitignore`
- Test: `tests/test_manual_signal_sender.py`

**Interfaces:**
- Produces: `DEFAULT_CONFIG_PATH: Path` and `load_redis_target(target_name: str, config_path: str | Path = DEFAULT_CONFIG_PATH) -> dict[str, str | int | None]`.
- Consumes: YAML root mapping with `targets.<name>.host`, `port`, `password`, and `stream`.

- [ ] **Step 1: Replace environment tests with failing temporary-YAML tests**

Use a temporary file containing:

```yaml
targets:
  remote_prod:
    host: redis.example.test
    port: 6380
    password: test-secret
    stream: test-signals
```

Assert exact parsed values. Add failures for a missing target and an out-of-range/non-integer port, and assert that `DEFAULT_CONFIG_PATH.name == "redis_targets.yaml"`.

- [ ] **Step 2: Run test to verify RED**

```bash
python -m unittest tests.test_manual_signal_sender -v
```

Expected: import or attribute failure because `scripts.redis_target_config` does not exist.

- [ ] **Step 3: Implement the minimal loader**

```python
DEFAULT_CONFIG_PATH = Path(__file__).with_name("redis_targets.yaml")

def load_redis_target(target_name, config_path=DEFAULT_CONFIG_PATH):
    path = Path(config_path)
    with path.open("r", encoding="utf-8") as fh:
        raw = yaml.safe_load(fh)
    if not isinstance(raw, dict) or not isinstance(raw.get("targets"), dict):
        raise ValueError(f"YAML targets 必须是映射: {path}")
    if target_name not in raw["targets"]:
        raise ValueError(f"Redis target 不存在: {target_name}")
    target = raw["targets"][target_name]
    if not isinstance(target, dict):
        raise ValueError(f"Redis target 必须是映射: {target_name}")
    host = str(target.get("host", "")).strip()
    stream = str(target.get("stream", "")).strip()
    try:
        port = int(target.get("port", 6379))
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Redis port 必须是整数: {target_name}") from exc
    password = target.get("password")
    if not host or not stream or not 1 <= port <= 65535:
        raise ValueError(f"Redis target 配置无效: {target_name}")
    if password is not None and not isinstance(password, str):
        raise ValueError(f"Redis password 必须是字符串或 null: {target_name}")
    return {"host": host, "port": port, "password": password, "stream": stream}
```

Every invalid state raises `ValueError` with the target/file context; missing files keep the native `FileNotFoundError` path.

- [ ] **Step 4: Create and protect the local YAML**

Add `scripts/redis_targets.yaml` with the current local and remote values, then append exactly:

```gitignore
scripts/redis_targets.yaml
```

Do not print or copy the password into tests, docs, logs, or the final response.

- [ ] **Step 5: Run test to verify GREEN**

```bash
python -m unittest tests.test_manual_signal_sender -v
```

Expected: all loader tests pass.

### Task 2: Make both scripts consume the shared loader

**Files:**
- Modify: `scripts/send_manual_signal.py`
- Modify: `scripts/send_batch_signals.py`
- Test: `tests/test_manual_signal_sender.py`
- Test: `tests/test_batch_signal_sender.py`

**Interfaces:**
- Consumes: `load_redis_target(TARGET)` from Task 1.
- Preserves: `TARGET`, manual signal constants, batch constants, `validate_batch`, `build_payloads`, and `publish_batch`.

- [ ] **Step 1: Add failing structure tests**

Assert both modules expose the imported `load_redis_target`, the batch script no longer imports `get_redis_target` from the manual script, and the manual script no longer exposes `get_redis_target` or imports `os`.

- [ ] **Step 2: Run tests to verify RED**

```bash
python -m unittest tests.test_manual_signal_sender tests.test_batch_signal_sender -v
```

Expected: failures because both scripts still use the old manual-script configuration helper.

- [ ] **Step 3: Clean the manual sender**

Remove `os`, `Mapping`, and `get_redis_target()`. Import:

```python
from scripts.redis_target_config import load_redis_target
```

Update its module docstring to point to `scripts/redis_targets.yaml`, and replace `get_redis_target(TARGET)` with `load_redis_target(TARGET)`.

- [ ] **Step 4: Clean the batch sender**

Import the same loader at module scope and remove the function-local dependency:

```python
from scripts.redis_target_config import load_redis_target
```

Replace `get_redis_target(TARGET)` with `load_redis_target(TARGET)`. Keep all batch logic unchanged.

- [ ] **Step 5: Run scoped verification**

```bash
python -m unittest tests.test_manual_signal_sender tests.test_batch_signal_sender -v
python -m compileall scripts tests/test_manual_signal_sender.py tests/test_batch_signal_sender.py
git diff --check -- .gitignore scripts/send_manual_signal.py scripts/send_batch_signals.py tests/test_manual_signal_sender.py tests/test_batch_signal_sender.py
```

Expected: sender tests pass, compilation exits 0, and diff check emits no errors. Confirm `git check-ignore -v scripts/redis_targets.yaml` points to the new exact ignore rule.

# 发信脚本 Redis YAML 配置设计

## 目标

整理 `scripts/send_manual_signal.py` 和 `scripts/send_batch_signals.py`，把 Redis 目标配置从 Python 代码移到同目录的 `scripts/redis_targets.yaml`，并消除批量脚本对手工脚本的配置依赖。

## 文件边界

- `scripts/redis_targets.yaml`：本机 Redis 目标数据，包含 `local` 和 `remote_prod` 的 `host`、`port`、`password`、`stream`。
- `scripts/redis_target_config.py`：唯一的 YAML 读取与校验入口。
- `scripts/send_manual_signal.py`：只保留单笔信号参数、人工确认和发送流程。
- `scripts/send_batch_signals.py`：只保留批次信号、安全校验、确认口令和顺序发送流程。
- `.gitignore`：精确忽略 `scripts/redis_targets.yaml`，避免真实密码进入 Git。

## YAML 结构

```yaml
targets:
  local:
    host: 127.0.0.1
    port: 6379
    password: null
    stream: tidal_quant_signals
  remote_prod:
    host: <remote-host>
    port: 6379
    password: <remote-password>
    stream: tidal_quant_signals
```

## 加载与错误规则

- 默认配置路径始终相对于脚本目录，不受当前工作目录影响。
- YAML 必须是映射，`targets` 必须是映射，所选 target 必须存在。
- `host` 和 `stream` 不能为空，`port` 必须是 1–65535 的整数，`password` 可为 `null` 或字符串。
- 配置不合法时直接抛出清晰错误，不默默回退到本地 Redis 或环境变量。
- 控制台仍只显示 host、port 和 stream，不显示 password。

## 兼容性

- 保持 `TARGET` 常量和两个脚本的现有运行命令。
- 保持单笔 `yes` 确认、批量动态确认口令、payload 字段、批量顺序和间隔。
- 不修改 QMT 执行端或 Redis Stream 合同。
- 不再使用 `QMT_REDIS_*` 环境变量；配置来源统一为 YAML。

## 测试

- 用临时 YAML 验证 local/remote target 读取，不读取真实密码。
- 验证目标缺失、host 为空、port 非法和 password 类型非法时失败。
- 保留现有单笔信号 ID、批次校验、payload 顺序和发送间隔测试。

"""Load Redis targets shared by the manual signal sender scripts."""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path

import yaml


DEFAULT_CONFIG_PATH = Path(__file__).with_name("redis_targets.yaml").resolve()


def load_redis_target(
    target_name: str,
    config_path: str | Path = DEFAULT_CONFIG_PATH,
) -> dict[str, str | int | None]:
    """Load and validate one Redis target from a UTF-8 YAML file."""
    path = Path(config_path)
    with path.open("r", encoding="utf-8") as fh:
        raw = yaml.safe_load(fh)

    if not isinstance(raw, Mapping):
        raise ValueError(f"YAML 根节点必须是映射: {path}")
    targets = raw.get("targets")
    if not isinstance(targets, Mapping):
        raise ValueError(f"YAML targets 必须是映射: {path}")
    if target_name not in targets:
        raise ValueError(f"Redis target 不存在: {target_name}")

    target = targets[target_name]
    if not isinstance(target, Mapping):
        raise ValueError(f"Redis target 必须是映射: {target_name}")

    host = target.get("host")
    if not isinstance(host, str) or not host.strip():
        raise ValueError(f"Redis host 不能为空: {target_name}")

    port_raw = target.get("port", 6379)
    if isinstance(port_raw, bool):
        raise ValueError(f"Redis port 必须是整数: {target_name}")
    try:
        port = int(port_raw)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Redis port 必须是整数: {target_name}") from exc
    if not 1 <= port <= 65535:
        raise ValueError(f"Redis port 必须在 1-65535 之间: {target_name}")

    password = target.get("password")
    if password is not None and not isinstance(password, str):
        raise ValueError(f"Redis password 必须是字符串或 null: {target_name}")

    stream = target.get("stream")
    if not isinstance(stream, str) or not stream.strip():
        raise ValueError(f"Redis stream 不能为空: {target_name}")

    return {
        "host": host.strip(),
        "port": port,
        "password": password,
        "stream": stream.strip(),
    }

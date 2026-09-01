"""按本机账户账本加单实例锁，防止同一账号启动两个跟单进程。"""

from __future__ import annotations

import os
from pathlib import Path
from typing import BinaryIO


class SingleInstanceLock:
    """对账本文件加排它锁，保证同一账号只启动一个跟单进程。"""

    def __init__(self, path: str | Path):
        """绑定账本文件路径；锁句柄延迟到 acquire() 时才创建。"""
        self.path = Path(path)
        self._file: BinaryIO | None = None

    def acquire(self) -> None:
        """尝试获取文件锁；已被占用则抛 RuntimeError，成功则持有到 release()。"""
        if self._file is not None:
            return
        self.path.parent.mkdir(parents=True, exist_ok=True)
        handle = self.path.open("a+b")
        handle.seek(0)
        if handle.read(1) == b"":
            handle.write(b"0")
            handle.flush()
        handle.seek(0)
        try:
            if os.name == "nt":
                import msvcrt

                msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl

                fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            handle.close()
            raise RuntimeError(
                f"已有交易进程使用同一账户账本，拒绝重复启动: {self.path}"
            ) from exc
        self._file = handle

    def release(self) -> None:
        """释放文件锁并关闭句柄；未持锁时安全跳过。"""
        handle = self._file
        if handle is None:
            return
        try:
            if os.name == "nt":
                import msvcrt

                handle.seek(0)
                msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                import fcntl

                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        finally:
            handle.close()
            self._file = None

    def __enter__(self) -> "SingleInstanceLock":
        """上下文管理入口，获取锁并返回自身。"""
        self.acquire()
        return self

    def __exit__(self, _exc_type, _exc, _tb) -> None:
        """上下文管理出口，无条件释放锁。"""
        self.release()

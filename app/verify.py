"""固件产物核对：相对路径校验、无符号链接读取、SHA-256 与读期间稳定性确认。

安全与正确性要点：
- 路径先归一化：拒绝绝对路径、空路径以及任何 ``..`` 越界分量，
  保证目标始终位于指定构建的工作目录之内。
- 打开过程从工作目录根开始逐分量 ``os.open(..., dir_fd=...)``，
  每一级都带 ``O_NOFOLLOW``（目录另加 ``O_DIRECTORY``）。路径上任何
  分量是符号链接都会失败（ELOOP，或目录分量上的 ENOTDIR——用 lstat
  复核区分），符号链接一律不参与核对。
- 读取前后各做一次 ``fstat`` 并比对关键元数据（dev/ino/mode/size/
  mtime/ctime），且实际读到的字节数必须等于读取前的文件大小；
  任一不一致即视为"读取期间文件发生变化"，调用方不得确认结果。
"""
from __future__ import annotations

import errno
import hashlib
import os
import posixpath
import stat
from pathlib import Path

_READ_CHUNK = 1024 * 1024

# 读取前后比对的元数据字段：任一变化都说明文件在读期间被动过
_STAT_ATTRS = ("st_dev", "st_ino", "st_mode", "st_size", "st_mtime_ns", "st_ctime_ns")


class SymlinkError(Exception):
    """路径任一分量是符号链接。"""


class NotRegularFileError(Exception):
    """目标不是普通文件（目录、设备等）。"""


class ChangedDuringRead(Exception):
    """读取期间文件内容或元数据发生变化。"""


def normalize_rel_path(raw: str) -> str:
    """把用户输入归一化为工作目录内的相对路径，越界/非法一律 ValueError。"""
    if not raw or not raw.strip():
        raise ValueError("路径不能为空")
    if raw != raw.strip():
        raise ValueError("路径首尾不允许空白字符")
    if "\x00" in raw:
        raise ValueError("路径不允许包含 NUL 字符")
    if raw.startswith("/") or posixpath.isabs(raw):
        raise ValueError("必须使用工作目录内的相对路径，不允许绝对路径")
    norm = posixpath.normpath(raw)
    if norm in (".", "..") or norm.startswith("../") or posixpath.isabs(norm):
        raise ValueError("路径越界：必须位于构建工作目录内，不允许 '..'")
    # normpath 已折叠 '.' 与重复分隔符；逐分量复查杜绝 '..' 残留
    parts = norm.split("/")
    if any(part in ("", ".", "..") for part in parts):
        raise ValueError("路径包含非法分量")
    return norm


def _open_component(name: str, dir_fd: int, flags: int) -> int:
    try:
        return os.open(name, flags, dir_fd=dir_fd)
    except OSError as exc:
        if exc.errno == errno.ELOOP:
            raise SymlinkError(f"路径分量是符号链接，不参与核对: {name}") from exc
        if exc.errno == errno.ENOTDIR:
            # Linux 上 O_DIRECTORY|O_NOFOLLOW 落在符号链接报 ENOTDIR 而非 ELOOP；
            # 用 lstat 区分"符号链接"与"中间分量本是普通文件"
            try:
                st = os.stat(name, dir_fd=dir_fd, follow_symlinks=False)
            except OSError:
                pass
            else:
                if stat.S_ISLNK(st.st_mode):
                    raise SymlinkError(
                        f"路径分量是符号链接，不参与核对: {name}"
                    ) from exc
        raise


def read_regular_file(workdir: Path, rel: str) -> tuple[int, str]:
    """读取 ``workdir/rel``（必须是不经任何符号链接的普通文件）。

    返回 ``(大小, sha256 十六进制)``。可能抛出：
    ``SymlinkError`` / ``NotRegularFileError`` / ``ChangedDuringRead``
    以及 ``OSError`` 家族（FileNotFoundError、NotADirectoryError 等）。
    """
    root_fd = os.open(workdir, os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC)
    fds = [root_fd]
    try:
        parts = rel.split("/")
        dir_fd = root_fd
        dir_flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC
        for part in parts[:-1]:
            nfd = _open_component(part, dir_fd, dir_flags)
            fds.append(nfd)
            dir_fd = nfd
        file_fd = _open_component(
            parts[-1], dir_fd, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC
        )
        fds.append(file_fd)

        before = os.fstat(file_fd)
        if not stat.S_ISREG(before.st_mode):
            raise NotRegularFileError(f"目标不是普通文件: {rel}")

        hasher = hashlib.sha256()
        total = 0
        while True:
            chunk = os.read(file_fd, _READ_CHUNK)
            if not chunk:
                break
            hasher.update(chunk)
            total += len(chunk)

        after = os.fstat(file_fd)
        if total != before.st_size or any(
            getattr(before, attr) != getattr(after, attr) for attr in _STAT_ATTRS
        ):
            raise ChangedDuringRead(f"文件在读取期间发生变化，未确认结果: {rel}")
        return total, hasher.hexdigest()
    finally:
        for fd in reversed(fds):
            try:
                os.close(fd)
            except OSError:
                pass

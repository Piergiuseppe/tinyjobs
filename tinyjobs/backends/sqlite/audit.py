from __future__ import annotations

import os
import re
import shutil
import sqlite3
from pathlib import Path

from ...models import Finding, concluded

MINIMUM_FREE_BYTES = 64 * 1024 * 1024

NETWORK_FILESYSTEMS = frozenset(
    {
        "nfs", "nfs4", "cifs", "smb3", "smbfs", "9p", "ncpfs", "ceph", "glusterfs",
        "lustre", "gpfs", "davfs", "fuse.sshfs", "fuse.glusterfs", "fuse.gcsfuse",
        "fuse.s3fs", "fuse.rclone", "fuse.juicefs", "fuse.davfs2", "fuse.blobfuse",
        "fuse.blobfuse2", "fuse.mountpoint-s3", "fuse.goofys", "fuse.ceph-fuse",
    }
)

LAYER = "the container's writable layer, gone when the container is removed"

VOLATILE_FILESYSTEMS = {
    "tmpfs": "memory that a reboot empties",
    "ramfs": "memory that a reboot empties",
    "overlay": LAYER,
    "fuse.fuse-overlayfs": LAYER,
    "aufs": LAYER,
}

ESCAPE = re.compile(r"\\([0-7]{3})")

MOUNTS = "/proc/self/mountinfo"

SYNCHRONOUS_LEVELS = {0: "OFF", 1: "NORMAL", 2: "FULL", 3: "EXTRA"}

Mount = tuple[str, str, str]


def audit(connection: sqlite3.Connection, database: str, uri: bool = False) -> list[Finding]:
    if not database:
        return [Finding(True, "an in-memory database: nothing persists, nothing to audit")]
    path = Path(database)
    return concluded(
        [
            *_journal(connection, uri),
            *_synchronous(connection, uri),
            *_filesystem(path, _mounts()),
            *_space(path),
        ]
    )


def _journal(connection: sqlite3.Connection, uri: bool) -> list[Finding]:
    mode = connection.execute("PRAGMA journal_mode").fetchone()[0]
    if mode == "wal":
        return [Finding(True, "journal mode WAL")]
    if uri:
        cause = (
            "tinyjobs leaves the pragmas of a database opened as a URI to you; run "
            "PRAGMA journal_mode = WAL on the file once"
        )
    else:
        cause = (
            "WAL could not be set, which happens on a filesystem without shared memory; "
            "keep the file on a local disk"
        )
    return [Finding(False, f"journal mode is {mode}, not WAL: {cause}")]


def _synchronous(connection: sqlite3.Connection, uri: bool) -> list[Finding]:
    level = int(connection.execute("PRAGMA synchronous").fetchone()[0])
    name = SYNCHRONOUS_LEVELS.get(level, str(level))
    if level == 1:
        return [Finding(True, "synchronous NORMAL, which holds across process crashes, not power loss")]
    if level > 1:
        return [Finding(True, f"synchronous {name}")]
    if uri:
        cause = (
            "tinyjobs leaves the pragmas of a database opened as a URI to you; set "
            "synchronous to NORMAL on every connection"
        )
    else:
        cause = "tinyjobs sets NORMAL on every connection, so something else reset it"
    return [Finding(False, f"synchronous is OFF: a crash loses jobs already committed; {cause}")]


def _filesystem(path: Path, mounts: list[Mount]) -> list[Finding]:
    if not mounts:
        return [Finding(True, f"filesystem not checked: no {MOUNTS}")]
    mount = mount_of(path, mounts, _device(path))
    if mount is None:
        return [Finding(True, "on a local disk")]
    point, kind, _ = mount
    if kind in NETWORK_FILESYSTEMS:
        return [
            Finding(
                False,
                f"the file is on {kind}, where SQLite's locking is unreliable and WAL "
                f"cannot work; keep it on a local disk",
            )
        ]
    if kind in VOLATILE_FILESYSTEMS:
        return [
            Finding(
                False,
                f"the file is on {kind}, {VOLATILE_FILESYSTEMS[kind]}: every acknowledged "
                f"job goes with it; keep it on a disk, a mounted volume in a container",
            )
        ]
    if point == str(path.resolve()):
        return [
            Finding(
                False,
                f"the file itself is a mount point, on {kind}: its -wal and -shm files land "
                f"outside the mount and vanish with the container; mount the directory, "
                f"not the file",
            )
        ]
    return [Finding(True, f"on {kind}")]


def _space(path: Path) -> list[Finding]:
    try:
        free = shutil.disk_usage(path.parent).free
    except OSError:
        return [Finding(True, "room not checked: the disk holding the file did not answer")]
    if free < MINIMUM_FREE_BYTES:
        return [
            Finding(
                False,
                f"the disk holding the file has {_megabytes(free)} free: SQLite refuses "
                f"every write with 'database or disk is full' when it fills",
            )
        ]
    return [Finding(True, f"{_megabytes(free)} free")]


def mount_of(path: Path, mounts: list[Mount], device: str | None = None) -> Mount | None:
    target = str(path.resolve())
    containing = [mount for mount in mounts if _under(target, mount[0])]
    exact = [mount for mount in containing if mount[0] == target]
    same_device = [mount for mount in containing if mount[2] == device]
    best = None
    for mount in exact or same_device or containing:
        if best is None or len(mount[0]) >= len(best[0]):
            best = mount
    return best


def _device(path: Path) -> str | None:
    for candidate in (path, path.parent):
        try:
            stat = os.stat(candidate)
        except OSError:
            continue
        return f"{os.major(stat.st_dev)}:{os.minor(stat.st_dev)}"
    return None


def _mounts() -> list[Mount]:
    try:
        with open(MOUNTS, encoding="utf-8", errors="surrogateescape") as handle:
            lines = handle.read().splitlines()
    except OSError:
        return []
    return parse_mounts(lines)


def parse_mounts(lines: list[str]) -> list[Mount]:
    mounts: list[Mount] = []
    for line in lines:
        parts = line.split()
        if "-" not in parts:
            continue
        separator = parts.index("-")
        if separator < 5 or len(parts) < separator + 2:
            continue
        mounts.append((_unescaped(parts[4]), parts[separator + 1], parts[2]))
    return mounts


def _under(target: str, point: str) -> bool:
    return point == "/" or target == point or target.startswith(point.rstrip("/") + "/")


def _unescaped(point: str) -> str:
    return ESCAPE.sub(lambda match: chr(int(match.group(1), 8)), point)


def _megabytes(size: int) -> str:
    if size >= 1024**3:
        return f"{size / 1024**3:.1f} GB"
    return f"{size // 1024**2} MB"

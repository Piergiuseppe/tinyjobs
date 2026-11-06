from __future__ import annotations

from ...models import Finding, concluded

SETTINGS = (
    "SELECT pg_catalog.current_setting('synchronous_commit', true) AS commit, "
    "pg_catalog.current_setting('fsync', true) AS fsync, "
    "pg_catalog.pg_is_in_recovery() AS standby, "
    "coalesce(pg_catalog.current_setting('neon.timeline_id', true), '') <> '' AS neon"
)


def audit(
    commit: str | None, fsync: str | None, standby: bool, neon: bool = False
) -> list[Finding]:
    return concluded([*_commit(commit), *_fsync(fsync, neon), *_standby(standby)])


def _commit(commit: str | None) -> list[Finding]:
    if commit == "off":
        return [
            Finding(
                False,
                "synchronous_commit is off: a crash loses the last commits, so a job "
                "acknowledged to its sender can vanish; set it to on",
            )
        ]
    return [_setting("synchronous_commit", commit)]


def _fsync(fsync: str | None, neon: bool) -> list[Finding]:
    if fsync == "off" and neon:
        return [Finding(True, "fsync off, with durability from Neon's storage")]
    if fsync == "off":
        return [
            Finding(
                False,
                "fsync is off: a crash can lose or corrupt committed jobs; set it to on",
            )
        ]
    return [_setting("fsync", fsync)]


def _standby(standby: bool) -> list[Finding]:
    if standby:
        return [
            Finding(
                False,
                "connected to a standby in recovery, which refuses every write; "
                "point the url at the primary",
            )
        ]
    return [Finding(True, "on the primary")]


def _setting(name: str, value: str | None) -> Finding:
    if value is None:
        return Finding(True, f"{name} not checked: the server has no such setting")
    return Finding(True, f"{name} {value}")

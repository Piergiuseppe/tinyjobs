from __future__ import annotations

from ...models import Finding, concluded

SETTINGS = (
    "SELECT pg_catalog.current_setting('synchronous_commit', true) AS commit, "
    "pg_catalog.current_setting('fsync', true) AS fsync, "
    "pg_catalog.pg_is_in_recovery() AS standby, "
    "coalesce(pg_catalog.current_setting('neon.timeline_id', true), '') <> '' AS neon, "
    "(SELECT reset_val FROM pg_catalog.pg_settings "
    "WHERE name OPERATOR(pg_catalog.=) 'synchronous_commit') AS default_commit"
)


def audit(
    commit: str | None,
    fsync: str | None,
    standby: bool,
    neon: bool = False,
    default_commit: str | None = None,
) -> list[Finding]:
    return concluded(
        [*_commit(commit, default_commit), *_fsync(fsync, neon), *_standby(standby)]
    )


def _commit(commit: str | None, default_commit: str | None) -> list[Finding]:
    if commit == "off":
        return [
            Finding(
                False,
                "synchronous_commit is off: a crash loses the last commits, so a job "
                "acknowledged to its sender can vanish; tinyjobs sets it to on on every "
                "connection it opens, so something else reset it",
            )
        ]
    if commit is None:
        return [_setting("synchronous_commit", commit)]
    if default_commit == "off":
        return [
            Finding(
                True,
                f"synchronous_commit {commit}, set by tinyjobs where the server's default "
                f"is off: a job enqueued through a connection of your own commits under "
                f"that default",
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

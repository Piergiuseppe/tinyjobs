from __future__ import annotations

from typing import Any, Mapping

from ...models import Finding, concluded

FULL_FRACTION = 0.9

WRITABLE_ROLES = frozenset({"master", "active-replica"})

NOT_CHECKED = Finding(
    True,
    "persistence and memory settings not checked: the server did not answer INFO",
)


def audit(info: Mapping[str, Any] | None) -> list[Finding]:
    if info is None:
        return [NOT_CHECKED]
    return concluded([*_role(info), *_persistence(info), *_eviction(info), *_memory(info)])


def _role(info: Mapping[str, Any]) -> list[Finding]:
    role = _text(info.get("role"))
    if role is None:
        return [Finding(True, "role not checked: INFO holds no role")]
    if role in WRITABLE_ROLES:
        return []
    return [
        Finding(
            False,
            f"connected to a replica (role {role}), which refuses every write; point the "
            f"url at the primary",
        )
    ]


def _persistence(info: Mapping[str, Any]) -> list[Finding]:
    enabled = _number(info.get("aof_enabled"))
    if enabled is None:
        findings = [Finding(True, "persistence not checked: INFO holds no aof_enabled")]
    elif not enabled:
        findings = [
            Finding(
                False,
                "appendonly is off: a crash loses every job since the last snapshot; "
                "set appendonly yes",
            )
        ]
    else:
        findings = [Finding(True, "appendonly on")]
    statuses = [("rdb_last_bgsave_status", "the last snapshot")]
    if enabled:
        statuses.insert(0, ("aof_last_write_status", "the last append-only write"))
    for field, what in statuses:
        status = _text(info.get(field))
        if status is None:
            findings.append(Finding(True, f"{what} not checked: INFO holds no {field}"))
        elif status == "ok":
            continue
        else:
            findings.append(
                Finding(
                    False,
                    f"{what} failed ({field} is {status}): Redis refuses writes with MISCONF "
                    f"while it does, or keeps running without persisting; check its disk and log",
                )
            )
    return findings


def _eviction(info: Mapping[str, Any]) -> list[Finding]:
    limit = _number(info.get("maxmemory"))
    policy = _text(info.get("maxmemory_policy"))
    if limit is None:
        return [Finding(True, "eviction not checked: INFO holds no maxmemory")]
    if not limit:
        return [Finding(True, "no maxmemory limit")]
    if policy is None:
        return [Finding(True, "eviction not checked: INFO holds no maxmemory_policy")]
    if not _evicting(policy):
        return [Finding(True, policy)]
    return [
        Finding(
            False,
            f"maxmemory-policy is {policy} with maxmemory set: Redis evicts jobs under "
            f"memory pressure without a word; set maxmemory-policy noeviction",
        )
    ]


def _evicting(policy: str | None) -> bool:
    return policy is not None and policy.startswith("allkeys")


def _memory(info: Mapping[str, Any]) -> list[Finding]:
    limit = _number(info.get("maxmemory"))
    used = _number(info.get("used_memory"))
    if used is None:
        return [Finding(True, "memory not checked: INFO holds no used_memory")]
    if not limit:
        return [Finding(True, f"{_size(used)} used")]
    share = f"{_size(used)} of {_size(limit)} used, {used * 100 // limit}%"
    if used >= limit * FULL_FRACTION:
        evicting = _evicting(_text(info.get("maxmemory_policy")))
        outcome = (
            "Redis is evicting or about to"
            if evicting
            else "enqueues fail with OOM when Redis fills"
        )
        return [Finding(False, f"{share}: {outcome}; raise maxmemory or prune with retention")]
    return [Finding(True, share)]


def _number(value: Any) -> int | None:
    if isinstance(value, bool):
        return int(value)
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _text(value: Any) -> str | None:
    if isinstance(value, bytes):
        value = value.decode("ascii", "replace")
    return value if isinstance(value, str) else None


def _size(size: int) -> str:
    if size >= 1024**3:
        return f"{size / 1024**3:.1f} GB"
    if size >= 1024**2:
        return f"{size / 1024**2:.0f} MB"
    return f"{size // 1024} KB"

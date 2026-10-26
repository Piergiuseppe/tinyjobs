from __future__ import annotations

from decimal import Decimal

UNITS = (("w", 604800), ("d", 86400), ("h", 3600), ("m", 60))


def format_duration(seconds: float) -> str:
    if seconds <= 0:
        return "forever"
    for suffix, size in UNITS:
        if seconds >= size and seconds % size == 0:
            return f"{int(seconds // size)}{suffix}"
    return format_seconds(seconds)


def format_seconds(seconds: float) -> str:
    if seconds >= 1:
        return f"{seconds:.3f}".rstrip("0").rstrip(".") + "s"
    return format(Decimal(f"{seconds:.3g}"), "f") + "s"


def format_age(seconds: float) -> str:
    whole = int(max(seconds, 0))
    if whole < 60:
        return f"{whole}s"
    minutes, whole = divmod(whole, 60)
    if minutes < 60:
        return f"{minutes}m{whole:02d}s"
    hours, minutes = divmod(minutes, 60)
    if hours < 48:
        return f"{hours}h{minutes:02d}m"
    return f"{hours // 24}d{hours % 24}h"

from __future__ import annotations

UNITS = (("w", 604800), ("d", 86400), ("h", 3600), ("m", 60))


def format_duration(seconds: float) -> str:
    if seconds <= 0:
        return "forever"
    for suffix, size in UNITS:
        if seconds >= size and seconds % size == 0:
            return f"{int(seconds // size)}{suffix}"
    return f"{seconds:g}s"

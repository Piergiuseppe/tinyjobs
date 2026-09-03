from __future__ import annotations

import argparse
import re

UNITS = {"s": 1.0, "m": 60.0, "h": 3600.0, "d": 86400.0, "w": 604800.0}
PATTERN = re.compile(r"^(\d+(?:\.\d+)?)([smhdw]?)$")


def parse_duration(text: str) -> float:
    match = PATTERN.match(text.strip().lower())
    if match is None:
        raise argparse.ArgumentTypeError(
            f"{text!r} is not a duration; write 30, 45s, 10m, 6h, 7d or 2w"
        )
    amount, unit = match.groups()
    return float(amount) * UNITS[unit or "s"]


def format_duration(seconds: float) -> str:
    if seconds <= 0:
        return "forever"
    for suffix, size in (("w", 604800), ("d", 86400), ("h", 3600), ("m", 60)):
        if seconds >= size and seconds % size == 0:
            return f"{int(seconds // size)}{suffix}"
    return f"{seconds:g}s"

from __future__ import annotations

from datetime import datetime, timezone
from typing import Sequence


def render_table(headers: Sequence[str], rows: Sequence[Sequence[str]]) -> str:
    if not rows:
        return "no matching jobs"
    widths = [len(header) for header in headers]
    for row in rows:
        for index, cell in enumerate(row):
            widths[index] = max(widths[index], len(cell))
    separator = "  "
    lines = [separator.join(h.ljust(w) for h, w in zip(headers, widths)).rstrip()]
    lines.append(separator.join("-" * w for w in widths))
    for row in rows:
        lines.append(
            separator.join(cell.ljust(w) for cell, w in zip(row, widths)).rstrip()
        )
    return "\n".join(lines)


def format_timestamp(value: float | None) -> str:
    if value is None:
        return "-"
    moment = datetime.fromtimestamp(value, tz=timezone.utc).astimezone()
    return moment.strftime("%Y-%m-%d %H:%M:%S")


def format_duration(seconds: float | None) -> str:
    if seconds is None:
        return "-"
    if seconds < 1:
        return f"{seconds * 1000:.0f}ms"
    return f"{seconds:.2f}s"


def shorten(value: str | None, width: int) -> str:
    if not value:
        return "-"
    collapsed = " ".join(value.split())
    if len(collapsed) <= width:
        return collapsed
    return collapsed[: width - 1] + "…"

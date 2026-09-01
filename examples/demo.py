from __future__ import annotations

import asyncio
import random
import time

from tinyjobs import Abandon, Retry, TinyJobs

app = TinyJobs("sqlite:///demo.db")


@app.task(name="demo.fast")
async def fast(label: str) -> str:
    await asyncio.sleep(0.05)
    return f"done {label}"


@app.task(name="demo.slow")
def slow(seconds: float) -> str:
    time.sleep(seconds)
    return f"slept {seconds}s"


@app.task(name="demo.flaky", max_attempts=4, retry_base=0.5, retry_jitter=False)
def flaky(order_id: int) -> str:
    if random.random() < 0.6:
        raise ConnectionError(f"upstream refused order {order_id}")
    return f"order {order_id} synced"


@app.task(name="demo.broken", max_attempts=1)
def broken() -> None:
    raise ValueError("this one never works")


_upstream_calls: dict[int, int] = {}


@app.task(name="demo.rate_limited", max_attempts=5, retry_base=60.0)
def rate_limited(page: int) -> str:
    _upstream_calls[page] = _upstream_calls.get(page, 0) + 1
    if _upstream_calls[page] < 3:
        raise Retry(in_=0.8)
    return f"page {page} fetched after {_upstream_calls[page]} calls"


@app.task(name="demo.permanent", max_attempts=5)
def permanent(document_id: int) -> None:
    raise Abandon(f"document {document_id} was deleted, retrying will not help")


@app.task(name="demo.timeout", timeout=0.2, max_attempts=1)
async def never_finishes() -> None:
    await asyncio.sleep(60)


@app.task(name="demo.rich_arguments")
def rich_arguments(when, amount, tags, path) -> str:
    return f"{when!r} {amount!r} {tags!r} {path!r}"


def seed() -> None:
    from datetime import datetime
    from decimal import Decimal
    from pathlib import Path

    jobs = [
        fast.enqueue("first"),
        fast.with_options(priority=10).enqueue("jumps the queue"),
        slow.enqueue(1.5),
        flaky.enqueue(1001),
        flaky.enqueue(1002),
        broken.enqueue(),
        rate_limited.enqueue(1),
        permanent.enqueue(77),
        never_finishes.enqueue(),
        fast.with_options(delay=5).enqueue("arrives in 5s"),
        fast.with_options(key="unique-work").enqueue("deduplicated"),
        fast.with_options(key="unique-work").enqueue("this one collapses"),
        rich_arguments.enqueue(
            datetime(2026, 9, 1, 10, 30),
            Decimal("19.99"),
            {"urgent", "billing"},
            Path("/tmp/invoice.pdf"),
        ),
    ]
    print(f"enqueued {len(jobs)} calls into demo.db")
    print(f"  the two 'unique-work' calls share one job: {jobs[-3].id == jobs[-2].id}")
    print()
    print("now run the worker and watch:")
    print("  python3.12 -m tinyjobs.cli worker --app examples.demo:app --concurrency 4")
    print()
    print("then inspect:")
    print("  python3.12 -m tinyjobs.cli jobs list --app examples.demo:app")
    print("  python3.12 -m tinyjobs.cli jobs list --app examples.demo:app --status failed")
    print("  python3.12 -m tinyjobs.cli jobs show <job-id> --app examples.demo:app")


if __name__ == "__main__":
    seed()

from __future__ import annotations

import time

from tinyjobs import TinyJobs

time.sleep(1.0)

app = TinyJobs("memory://")


@app.task(isolated=True, timeout=0.4, store_result=True)
def quick() -> str:
    return "quick"

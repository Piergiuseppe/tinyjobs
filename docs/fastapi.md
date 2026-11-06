# FastAPI

```python
from fastapi import FastAPI
from tasks import app as jobs, generate_pdf

api = FastAPI(lifespan=jobs.lifespan(worker=True))


@api.post("/documents/{document_id}/pdf")
async def render(document_id: int) -> dict:
    job = await generate_pdf.aenqueue(document_id)
    return {"job": job.id}
```

`uvicorn main:api` is the whole deployment. The lifespan connects when the server starts, runs a
worker inside it, and on shutdown lets in-flight jobs finish before closing. Plain `def` endpoints
can call `enqueue()` too, on every backend.

`worker=True` takes the options of `app.worker()`, such as `concurrency`, `queues` and
`shutdown_timeout`. Under `uvicorn --workers 4` each process runs its own worker on the same queue,
and cron still fires once. If you already have a lifespan, nest it:

```python
@asynccontextmanager
async def lifespan(api: FastAPI):
    async with jobs.serve(worker=True):
        yield
```

The process exits within `shutdown_timeout` (30 s), or 1.25 s when that is shorter, so that releasing
still has its second: a last moment, half a second at most, is kept for closing connections, and of
the rest running jobs and a claim still in flight get the first three quarters and releasing what is
still running the last, one second at least; a second Ctrl-C under a server does not cut it short.
Under `tinyjobs worker` it does: running jobs are cancelled and go back to the queue at once, even
when one blocks the event loop; a job that swallows every interrupt makes the process exit a second
later with status 130, leaving its job to the reaper. A `def` job whose
thread is still running is not released; the worker waits for it and records its outcome within a
quarter of the timeout, one second at least, after the thread returns. Keep it under your
orchestrator's grace period: `docker stop` allows 10 s, and past it the process is killed and its jobs
wait out their leases instead of going straight back to the queue. A worker that cannot reach its
backend when it starts, or whose setup fails, exits with status 1 as RQ does, so let the orchestrator
restart it.

Starlette takes the same `lifespan`. Leave out `worker=True` and the server only enqueues, with
`tinyjobs worker --app tasks:app` running the jobs elsewhere.

The worker shares the process with your API: an async task that blocks the loop stalls requests too,
and CPU-heavy `def` tasks compete for the GIL unless the build is free-threaded. That is the point
where a separate worker pays off. Numbers in [Measured](measured.md).

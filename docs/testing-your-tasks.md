# Testing your tasks

```python
app = TinyJobs("memory://", eager=True)

job = generate_pdf.enqueue(123)     # runs this job before enqueue returns
job.status                          # success
job.result()                        # '/tmp/123.pdf'
```

Or keep the queue and run it when you choose, with a clock you control:

```python
from tinyjobs import FrozenClock

clock = FrozenClock(time.time())
app = TinyJobs("memory://", clock=clock)

flaky.enqueue()
app.drain()             # runs everything due, highest priority first; returns how many ran
clock.advance(10)       # the retry's backoff
app.drain()
```

`memory://` is the SQLite backend on a private in-memory database, so tests run the same claim,
dedup and retries as production. Delays and backoff are respected in eager mode too: advance the clock
and drain. A failing task is recorded on the job, not raised, as it would be in a worker. An eager
call runs only the job it sent, ahead of any older job, and returns it finished. If a drain or a
worker claimed that job first, the call waits for it to finish. A dedup hit on a job sent
earlier comes back as it is, possibly `running`. A job not yet due comes back `queued`.

Earlier releases ran every due job on each eager call and returned the sent job after the older
ones. To run the rest of the queue, call `app.drain()`.

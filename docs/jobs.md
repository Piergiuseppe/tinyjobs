# Jobs

`enqueue` gives you back a snapshot of the row it just wrote, not a live handle, so the `Job` you are
holding keeps saying `queued`: the worker that runs your task is a different process writing to the
database, and nothing reaches back into your object. Ask for the current state when you want it:

```python
job = generate_pdf.enqueue(123)
print(job.id)          # 01a061bf-c08a-738d-...

job.refresh()          # updates this instance in place, and returns it
job.status             # queued -> running -> success
job.attempts           # 3, if it took three tries
job.last_error         # 'ConnectionError: refused', when it failed

app.get(job.id)        # or a fresh object, if you prefer not to mutate
```

Attribute access never does I/O on its own. `refresh()` is the one place a round trip happens, which
keeps `for job in jobs: print(job.status)` from quietly becoming a thousand queries.

From async code, which is the native path:

```python
job = await generate_pdf.aenqueue(123)
```

## Results

If the task returns something you want back, ask for it to be kept:

```python
@app.task(store_result=True)
async def generate_pdf(document_id: int) -> str:
    return f"/tmp/{document_id}.pdf"

job = generate_pdf.enqueue(123)
...
job.refresh().result()     # '/tmp/123.pdf'
```

Results are off by default because most jobs are run for their side effects, and storing return
values nobody reads is write amplification on the hot path. A stored result lives as long as its job
unless `result_ttl` says otherwise, and `max_result_bytes` keeps a big one out of the queue:

```python
app = TinyJobs("sqlite:///jobs.db", max_result_bytes=64 * 1024)

@app.task(store_result=True, result_ttl=3600)
async def generate_pdf(document_id: int) -> str: ...
```

Within a few minutes of the TTL a worker that registers the task drops the result and keeps the
job, so `jobs show` still tells what happened. A result over the cap fails the job with
`ResultTooLarge` and is not retried, since the function ran and would produce the same result
again; put the file in object storage and return its key.

## Waiting

If you want to block until a job is finished, `wait` polls for you:

```python
job = generate_pdf.enqueue(123)
print(job.wait(timeout=30).result())     # '/tmp/123.pdf'

await job.awaited(timeout=30)            # from async code
```

It returns the job whatever the outcome, rather than raising, so a failed job leaves you holding
`job.status`, `job.attempts` and `job.last_error` to look at. Only the timeout raises, as
`WaitTimeout`, which is also a `TimeoutError` if you would rather catch that.

The async twin is called `awaited` and not `await` for the boring reason that `await` is a keyword.

Waiting on a job from the request that created it means you did not need a background job. It is meant
for scripts, tests and glue code.

## Running a worker

```bash
tinyjobs worker --app tasks:app --concurrency 8
```

`tinyjobs` is installed as a command, but `python -m tinyjobs` does the same thing and is handy when
you have not activated a virtualenv:

```bash
python3.13 -m tinyjobs worker --app tasks:app --concurrency 8
```

## Looking at what happened

```bash
tinyjobs jobs list --app tasks:app
tinyjobs jobs list --app tasks:app --status failed
tinyjobs jobs show <job-id> --app tasks:app
```

`jobs show` prints every attempt, not just the last one:

```
attempts
    1  failed        4ms  laptop/8821/9389541d  ConnectionError
    2  failed        3ms  laptop/8821/9389541d  ConnectionError
    3  success      12ms  laptop/8821/9389541d
```

## Timeouts on sync tasks are advisory

You can't kill a running Python thread. When a threaded task exceeds its timeout the job is marked
failed and the worker moves on, but the thread keeps going until the function returns, and a warning
names the task. In a worker that thread counts against `concurrency` until it returns, so the worker
claims one job fewer and never holds a job it has no thread for. A drain or an eager call runs the
next job on a new thread beside it. The process exits without waiting for such threads. The timeout
counts from when the job's thread starts, so a job is never failed before it runs. Pass timeouts to
the library you are calling, write the task as `async def` where cancellation actually works, or
declare it [isolated](isolated-tasks.md), where the timeout is hard.

## Arguments are serialized as JSON

Extended with tags for `datetime`, `date`, `Decimal`, `UUID`, `Path`, `set`, `tuple`, `bytes` and
non-string dict keys. Pickle is available behind `TinyJobs(..., allow_pickle=True)` but it's off by
default, because deserializing a pickle runs arbitrary code and the Postgres and Redis backends reach
over a network. Dataclasses and enums need one line of registration:

```python
app.codec.register_dataclass(Money)
app.codec.register_enum(Priority)
```

## An example

There's a runnable example in `examples/demo.py` covering retries, timeouts, priorities, delays,
deduplication and cron:

```bash
python -m examples.demo
tinyjobs worker --app examples.demo:app --concurrency 4
```

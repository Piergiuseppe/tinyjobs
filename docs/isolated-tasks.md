# Isolated tasks

A `def` task that holds the GIL for seconds, leaks memory or calls a C library that can crash takes
the whole worker down with it. Declare it `isolated=True` and it runs in a job process of its own:

```python
@app.task(isolated=True, timeout=120, max_attempts=3)
def render(document_id: int) -> None:
    ...
```

The worker starts job processes as it needs them, up to `--processes` (`processes=` from Python,
by default `concurrency` or the CPU count, whichever is lower, and never fewer than `loops`), and
keeps each one for the next job.
Arguments and results travel through the job's codec, so the job's data is never pickled, and the
task is found by name in the module that registered it, which a job process must be able to
import. What changes:

- **Timeouts are hard.** At `timeout=` the process is killed and the job fails with `TIMEOUT`,
  leaving no thread behind.
- **A crash is a failure.** A process that exits or dies of a signal fails the job with
  `ProcessDied`, naming the exit status or the signal, and the job retries like any other failure on
  a fresh process.
- **Stopping kills the process.** A job still running when the shutdown budget ends is killed and
  handed back to the queue, where a threaded task would be left running. Each job process is its
  own process group, so a kill takes the subprocesses the task started with it.
- **`--jobs-per-process N`** (`jobs_per_process=`) replaces a process after N jobs, for code that leaks.
- **Exceptions come back as themselves.** A `KeyError` raised in the process is a `KeyError` to
  `retry_on`, with the traceback from the process; `Retry` and `Abandon` work as usual.
- **The timeout counts from the moment the task starts**, not from the process start, so a slow
  import is never charged to the first job.

A job process starts with whatever the task's module imports and nothing else: code that ran in the
worker's `__main__`, such as a codec adapter registered at startup, did not run there. Pass
`TinyJobs(..., process_setup=prepare)` for a function to call once in each process before its
first job, in workers, `app.drain()` and eager mode alike, or `--process-setup
myproject.setup:prepare` (`process_setup=prepare` on the worker) for one worker; under Django,
`django.setup()` runs on its own when `DJANGO_SETTINGS_MODULE` is set. The setup has ten minutes, separate from the task's timeout, before
the process is killed and the job fails. A job process ignores `SIGINT` and `SIGTERM`, so a Ctrl-C stops
the worker and the worker decides what happens to the job within its shutdown budget, and it exits
by itself if the worker dies under it. Isolated jobs beyond `processes` wait in their slot for a
free process, so size `processes` for the work and `concurrency` for everything else. Eager mode
and `app.drain()` run isolated tasks in a job process too, started for the call and stopped after
it, setup included. A task defined in a script works when
the script guards its entry point with `if __name__ == "__main__":`; one registered from
`python -c`, a REPL or a package's `__main__.py` is refused, since a job process cannot import
it from there. `async def` tasks cannot be isolated.

Measured on a MacBook (SQLite, `concurrency=4`): the first isolated job, process start included,
takes 65 ms on 3.12 and 80 ms on 3.14t; after that a no-op isolated task runs at 2,500 jobs/s
against 3,300 threaded on 3.12, 3,600 against 4,800 on 3.14t. For a task that spins the CPU
for a few milliseconds, 4 job processes on 3.12 do 730 jobs/s where 4 threads do 230, held by the
GIL; on 3.14t the threads do 910 and the processes 660, since the result still crosses a pipe.

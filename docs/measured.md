# Measured

Ten hours of steady load on a shared four-core ARM Linux VPS, run twice side by side on 0.7.0: once
on one SQLite file and once on Redis 7.4. Each run had its own container capped at 1.25 CPUs and
1.5 GB, three worker processes of eight slots, a deliberate 25 jobs per second, a graceful restart
every fifteen minutes and a `SIGKILL` every twenty.

```
SQLite   897,300 jobs   24.9/s sustained, zero enqueue failures
Redis    881,375 jobs   24.5/s sustained, zero enqueue failures
each     68 worker restarts, 39 by SIGTERM, 29 by SIGKILL, zero unexpected deaths
```

Sampled once a minute for the whole run, comparing the medians of the first hour and the last:

| | SQLite hour 0 | SQLite hour 9 | Redis hour 0 | Redis hour 9 |
| --- | --- | --- | --- | --- |
| worker RSS | 37.1 MB | 37.3 MB | 41.6 MB | 42.1 MB |
| threads per worker | 12 | 12 | 12 | 13 |
| open file descriptors | 12 | 12 | 17 | 17 |
| store size | 39 MB | 42 MB | 43 MB | 47 MB |
| end-to-end p95 | 402 ms | 399 ms | 392 ms | 403 ms |

Nothing drifted. The store size is the database file on SQLite and Redis `used_memory` on Redis;
with retention on, both stayed within a few MB for ten hours. The SQLite workers had 12 file descriptors open in all 597 samples.

A second run on the same VPS went the other way: 12.7 hours of aggressive load and failure on
Postgres 17, Redis 7.4 and one SQLite file at once, seven workers of 16 to 64 slots on each, on four
cores the host shares with two live services. It cycled through bursts at 20 to 60 times the
steady rate, a fill of about 800,000 Redis, 215,000 Postgres and 43,000 SQLite jobs drained flat out,
and fifteen minutes of failures an hour: Redis restarted 55 times and paused 105, Postgres killed or
restarted 78 times, its connections terminated, reset or frozen 148 times and its indexes dropped
12, the SQLite file locked 150 times, and 811 worker stops, 186 of them `SIGKILL`.

```
Redis      16,769,863 jobs   about 1,070/s draining the fill, 2,235/s at peak
Postgres    6,368,763 jobs   about 500/s, 595/s at peak
SQLite      2,462,458 jobs   about 70/s, 366/s at peak
```

No job with retries left was lost: the 280 that ended `failed` without running every attempt had one
to three, and an outage or a kill used them. 4,923 ran more than once, each after a kill, an outage
or a forced stop. None of the 104,340 jobs enqueued in a rolled-back transaction ran, and cron never
fired twice. Worker RSS grew 1.5 to 3.9% over the run and file descriptors stayed flat. 38 stops
took up to 2.2 s longer than their budget while the host ran at a load of 20 to 29; none hung. Six workers restarted into an outage exited with status 1 at startup.

A separate burst harness pushes the other end: 2,398 enqueues per second from 24 threads, 572 jobs
per second end-to-end with six workers, and a crash recovery check that kills every worker while 48
jobs are mid-flight and confirms all 48 come back and finish. Some of them run twice, which is what
at-least-once means; the check is that each one applied its side effect once, because the handlers
are idempotent.

Retention was also measured on its own, with the burst harness: with it off the database climbed
from 4.7 to 10.2 MB in three minutes and kept going, and with it on the same curve flattened at 6.9
MB and stayed there. Under a `SIGKILL` on a worker every thirty seconds it moved 0.6% across eight
minutes.

Redis was measured with the same kind of harness, on a laptop with Redis 7.4 in Docker: 20,000 jobs
over four workers of eight slots in 2.1 seconds, against 4 seconds on SQLite, each run exactly once.
Twelve `SIGKILL`s on workers during 4,000 jobs left none lost; 43 came back through an expired lease,
and 2 applied their side effect twice. Claiming stays at 0.7 ms with 200,000 jobs queued.

Postgres 17 was measured the same way on the same laptop: 20,000 jobs over four workers of eight slots
in 2.1 seconds, each run exactly once, and 6,850 enqueues a second from 24 threads. An idle worker
polling every five seconds started a new job 6.6 ms after it was sent (p95 8.4 ms). Claiming stays at
0.23 ms with 200,000 jobs queued. Twelve `SIGKILL`s on workers during 4,000 jobs left none lost and
none run twice; 88 came back through an expired lease. Postgres stopped three times during 6,000
jobs, once cleanly and twice with `-m immediate`, down three seconds each: the workers carried on,
and nothing was lost or ran twice.

Two machines sharing one Redis were measured too: a laptop and a Linux VPS 52 ms apart, two workers
of eight slots on each, a `SIGKILL` on one of them every 25 to 30 seconds, 3,000 jobs a run. With the
laptop's clock set five minutes behind and the VPS's fifteen minutes ahead, all 3,000 succeeded and
none applied its side effect twice; the 23 attempts cut short by the kills were picked up again. With
both clocks right the result was the same, but for one job killed between its side effect and its
completion. The same skewed run on 0.7.0, which still used each machine's own clock, ran 492 jobs
twice and failed 2, because the VPS took the laptop's live jobs for dead.

Cron was measured on its own. Ten thousand schedules all due on the same minute fire in about a
second from one scheduler, roughly 110 microseconds each. With four workers racing for them, 2,000
schedules a minute were fired 8,000 times in four minutes, each exactly once, p95 0.57 seconds late;
10,000 a minute, 40,000 times, each exactly once, p95 6.4 seconds late. Fifty-six schedulers racing on
one schedule for twelve minutes produced twelve jobs, and fifty `SIGKILL`s across four workers and a
standalone scheduler in ten minutes left no gap and no duplicate.

FastAPI was measured under uvicorn in Docker, with the worker embedded. 3,000 jobs through one
process on SQLite and 4,000 through `--workers 4` each ran exactly once, and cron fired once in each
of 13 minutes across the four processes. Three API containers on one Redis took 5,638 jobs while one
was `SIGKILL`ed; the eight it was running came back on the others when their leases expired, and
none ran twice. An 8-second Redis outage turned enqueues into 500s while it lasted, then everything
resumed; the one job that finished during it ran again, which no longer happens: with the outcome
retried, a job finishing as an 8-second outage began was recorded once. `docker stop` waited only
for the jobs in flight, 1.5 seconds.

Sharing the process costs the API little: `/ping` p95 went from 2.1 ms to 12.9 ms under eight
CPU-bound `def` tasks, and back to 1.6 ms with the worker moved out. Six `async def` tasks calling
`time.sleep(1)` froze it for six seconds.

Django was measured with a real project under uvicorn `--workers 2` and under `runserver`: 1,000
requests split between sync and async views, each enqueuing a task that writes a row, with two
`manage.py tinyjobs worker` processes on SQLite and then on Redis. Every task ran exactly once.
Enqueueing through `django.tasks` costs about what a direct `send` does, 6,000 a second against
6,600. A worker `SIGKILL`ed mid-task leaves that task `FAILED` once its lease expires, since a
Django task has one attempt unless it sets `max_attempts`.

Throughput was measured on the same four-core ARM VPS against one Redis 7.4 capped at one CPU, the
workers capped at three, 128 slots per loop or process, draining 80,000 no-op jobs; jobs a second
over the middle 80% of the drain:

| | GIL on | free-threaded 3.14t |
| --- | --- | --- |
| 1 process, `async def` | 3,626 | 3,491 |
| 1 process, 3 loops, `async def` | 5,473 | 7,190 |
| 3 processes, `async def` | 7,300 | 7,476 |
| 1 process, `def` | 4,269 | 3,951 |
| 1 process, 3 loops, `def` | 4,882 | 7,109 |
| 3 processes, `def` | 7,356 | 7,263 |

Without the GIL one process with three loops does what three processes do, in a third of the
memory; with it, the loops take turns and gain less than half as much. One loop runs a few percent
slower on 3.14t, the price of its per-object locks. Redis ran at 94 to 98% of its core at the top,
so that is its ceiling on this machine: its Lua scripts run on one thread, and `io-threads` on
Redis 8 or Valkey 8.1 changed nothing measurable. Past that ceiling the loops keep scaling: on a
ten-core laptop with two Redis instances and two apps, one process running eight loops reached
42,000 `async def` and 36,000 `def` jobs a second on 3.14t, 97% and 94% of what eight processes
did, while with the GIL the same threads flattened at 27,000 and 17,000. CPU-bound `def` tasks gain
the most: on that laptop, a worker of eight slots got through them 3.7 times faster on 3.14t.

The same VPS and the same Redis also ran Celery 5.6, to put these numbers beside something known.
Redis is the backend to do that on: it is the fastest of the three, so the libraries, not the
store, set the pace. Both drained 80,000 jobs on Python 3.12 with three worker processes capped at
three cores: Celery's default prefork pool, with no result backend and heartbeat, gossip and mingle
off, against three tinyjobs workers.

| jobs a second | Celery | tinyjobs |
| --- | --- | --- |
| no-op `def`: the library's own cost | 800 | 7,260 |
| about 1 ms of CPU: the job's cost | 610 | 840 |

Celery's other pools land nearby, 910 with `solo` and 760 with 64 threads, and `acks_late` or a
prefetch of 32 move it by a few percent: its ceiling is its own consumer process, which takes every
message apart and hands it to the children while Redis sits at 9%. On 3.14t one tinyjobs process
with three loops does the work of the three processes, 7,100 and 1,010. So the gap is what each
library adds to a job: it counts when jobs are many and small, and fades behind jobs that do real
work. Celery still has the canvas and RabbitMQ that tinyjobs does not.

Two caveats worth stating. The tasks are synthetic, with `sleep` standing in for real I/O. And 25
jobs per second was chosen as a comfortable rate to hold for ten hours, not as a ceiling.

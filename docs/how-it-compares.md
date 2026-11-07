# How it compares

[Huey](https://github.com/coleifer/huey) is the closest relative and worth a look: it also runs on
SQLite with no dependencies, and has cron, retries and a `django.tasks` backend. tinyjobs adds three
things. `async def` tasks run natively on the worker's loop, where Huey has no asyncio pipeline for
running tasks, only helpers to await their results. FastAPI and Starlette can run the worker inside
the server's lifespan. And cron fires from every worker without a leader, with timezones and misfire
policies, on SQLite, Postgres or Redis. [LiteQ](https://github.com/ddreamboy/liteq), at 0.2, makes much
the same pitch on SQLite.

[RQ](https://python-rq.org) and [arq](https://github.com/python-arq/arq) need Redis. RQ's cron
scheduler is still marked beta, and its retries take a fixed interval or a list of delays, with no
built-in exponential backoff. arq is asyncio-only and in maintenance mode, with security fixes only.
[Celery](https://docs.celeryq.dev) and [Dramatiq](https://dramatiq.io) need a broker; Celery is
still the answer for RabbitMQ routing, workflows and large fleets.

On Postgres, [Procrastinate](https://procrastinate.readthedocs.io) is the closest: async, with cron,
retries, `LISTEN`/`NOTIFY` and jobs deferred inside your own transaction, but Postgres only. tinyjobs
does the same on Postgres and on SQLite, and keeps Redis behind the same API. In Django,
[django-tasks-db](https://github.com/RealOrangeOne/django-tasks-db) keeps `django.tasks` tasks in
your database through the ORM, so an enqueue can share your transaction, and
[django-q2](https://github.com/GDay/django-q2) can use the ORM as its broker and has cron schedules.

As of October 2026, from each project's documentation.

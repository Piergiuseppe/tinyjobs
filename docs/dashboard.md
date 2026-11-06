# Dashboard

```
tinyjobs dashboard --app tasks:app
```

serves [the runbook](operating-it.md) as pages on `http://127.0.0.1:8070/`: an overview with the counts by
queue and status, the health findings and the queued jobs by task; the jobs, newest first, by
status and queue, with every attempt and its traceback on a job's page; and the schedules. Retry,
cancel, pause, resume and run are buttons that do what the CLI commands do. The pages refresh
every five seconds, and `/api/overview`, `/api/jobs`, `/api/jobs/<id>` and `/api/schedules` give
the same data as JSON, for `curl` and scripts. A page the store cannot answer within five
seconds is a 503 with the error, so a probe sees the store's trouble rather than a hung page.
Nothing to install: the dashboard is the standard library's HTTP server, like the metrics
exporter, and it is loaded only when the command runs or the pages are mounted.

`--host 0.0.0.0` exposes it, and `--port` moves it. There is no login and no CSRF protection, on
purpose: the dashboard retries and cancels jobs, so put it behind the reverse proxy that already
guards the rest of the deployment, and never on a public address. The same pages mount inside an
application you already serve, at any prefix:

```python
from tinyjobs.dashboard import asgi, wsgi
from werkzeug.middleware.dispatcher import DispatcherMiddleware

fastapi_app.mount("/tinyjobs", asgi(app))
flask_app.wsgi_app = DispatcherMiddleware(flask_app.wsgi_app, {"/tinyjobs": wsgi(app)})
```

`asgi(app)` awaits the app on the server's own event loop, so it fits beside `app.lifespan()`;
`wsgi(app)` runs each request through `app.run_sync`, in the server's thread. Both read the
prefix from the request (`root_path`, `SCRIPT_NAME`), so every link and form keeps working
wherever the pages are mounted.

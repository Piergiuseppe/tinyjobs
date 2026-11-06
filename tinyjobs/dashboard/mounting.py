from __future__ import annotations

from typing import Any, Awaitable, Callable, Iterable

from ..app import TinyJobs
from ..web import Request, parse_query
from .core import Dashboard

WSGIApplication = Callable[[dict[str, Any], Callable[..., Any]], Iterable[bytes]]
ASGIApplication = Callable[
    [
        dict[str, Any],
        Callable[[], Awaitable[dict[str, Any]]],
        Callable[[dict[str, Any]], Awaitable[None]],
    ],
    Awaitable[None],
]


def wsgi(app: TinyJobs, **options: Any) -> WSGIApplication:
    dashboard = Dashboard(app, **options)

    def application(environ: dict[str, Any], start_response: Callable[..., Any]) -> Iterable[bytes]:
        request = Request(
            environ["REQUEST_METHOD"],
            _decoded(environ.get("PATH_INFO", "")) or "/",
            parse_query(environ.get("QUERY_STRING", "")),
            _decoded(environ.get("SCRIPT_NAME", "")).rstrip("/"),
        )
        response = dashboard.respond(request)
        start_response(
            f"{response.status} {response.reason}",
            [
                ("Content-Type", response.content_type),
                ("Content-Length", str(len(response.body))),
                *response.headers,
            ],
        )
        return [] if request.method == "HEAD" else [response.body]

    return application


def asgi(app: TinyJobs, **options: Any) -> ASGIApplication:
    dashboard = Dashboard(app, **options)

    async def application(
        scope: dict[str, Any],
        receive: Callable[[], Awaitable[dict[str, Any]]],
        send: Callable[[dict[str, Any]], Awaitable[None]],
    ) -> None:
        if scope["type"] == "lifespan":
            await _lifespan(receive, send)
            return
        if scope["type"] != "http":
            return
        root = scope.get("root_path", "").rstrip("/")
        request = Request(
            scope["method"],
            _below(scope["path"], root),
            parse_query(scope.get("query_string", b"").decode("latin-1")),
            root,
        )
        response = await dashboard.handle(request)
        await send(
            {
                "type": "http.response.start",
                "status": response.status,
                "headers": [
                    (b"content-type", response.content_type.encode()),
                    (b"content-length", str(len(response.body)).encode()),
                    *((name.lower().encode(), value.encode()) for name, value in response.headers),
                ],
            }
        )
        await send(
            {
                "type": "http.response.body",
                "body": b"" if request.method == "HEAD" else response.body,
            }
        )

    return application


def _decoded(text: str) -> str:
    return text.encode("latin-1", "replace").decode("utf-8", "replace")


def _below(path: str, root: str) -> str:
    if root and (path == root or path.startswith(root + "/")):
        path = path[len(root):]
    return path or "/"


async def _lifespan(
    receive: Callable[[], Awaitable[dict[str, Any]]],
    send: Callable[[dict[str, Any]], Awaitable[None]],
) -> None:
    while True:
        message = await receive()
        if message["type"] == "lifespan.startup":
            await send({"type": "lifespan.startup.complete"})
        elif message["type"] == "lifespan.shutdown":
            await send({"type": "lifespan.shutdown.complete"})
            return

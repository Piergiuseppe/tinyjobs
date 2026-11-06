from __future__ import annotations

import logging
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Callable
from urllib.parse import unquote, urlsplit

from .web import Request, Response, address, families, parse_query, unreachable

Handle = Callable[[Request], Response]

logger = logging.getLogger("tinyjobs.serving")

BODY_LIMIT = 65536
DRAIN_LIMIT = 1048576


class Endpoint:
    def __init__(
        self,
        host: str,
        port: int,
        handle: Handle,
        *,
        name: str,
        what: str,
        logger: logging.Logger = logger,
    ) -> None:
        self._server = bind(host, port, handle, logger)
        self._thread = threading.Thread(target=self._server.serve_forever, name=name, daemon=True)
        self._what = what
        self._logger = logger

    @property
    def host(self) -> str:
        return self._server.server_address[0]

    @property
    def port(self) -> int:
        return self._server.server_address[1]

    @property
    def base_url(self) -> str:
        return f"http://{address(self.host, self.port)}"

    @property
    def url(self) -> str:
        return self.base_url

    def start(self) -> "Endpoint":
        self._thread.start()
        self._logger.info("serving %s at %s", self._what, self.url)
        return self

    def stop(self) -> None:
        if self._thread.is_alive():
            self._server.shutdown()
            self._thread.join()
        self._server.server_close()

    def __enter__(self) -> "Endpoint":
        return self.start()

    def __exit__(self, *exc_info: Any) -> None:
        self.stop()


def bind(host: str, port: int, handle: Handle, logger: logging.Logger) -> "_Server":
    found = families(host, port)
    for family in found[:-1]:
        try:
            return _Server((host, port), handle, family, logger)
        except OSError as error:
            if not unreachable(error):
                raise
    return _Server((host, port), handle, found[-1], logger)


class _Server(ThreadingHTTPServer):
    daemon_threads = True

    request_queue_size = 128

    def __init__(
        self, listen: tuple[str, int], handle: Handle, family: int, logger: logging.Logger
    ) -> None:
        self.address_family = family
        super().__init__(listen, _Handler)
        self.handle = handle
        self.logger = logger

    def handle_error(self, request: Any, client_address: Any) -> None:
        self.logger.debug("the request from %s failed", client_address, exc_info=True)


class _Handler(BaseHTTPRequestHandler):
    timeout = 30

    def do_GET(self) -> None:
        self._serve()

    def do_HEAD(self) -> None:
        self._serve()

    def do_OPTIONS(self) -> None:
        self._serve()

    def do_POST(self) -> None:
        try:
            length = int(self.headers.get("Content-Length") or 0)
            if length < 0:
                raise ValueError(length)
        except ValueError:
            self._answer(lambda: self.send_error(400, "bad Content-Length"))
            return
        if length > BODY_LIMIT:
            self._drain(min(length, DRAIN_LIMIT))
            self._answer(lambda: self.send_error(413, "the request body is larger than accepted"))
            return
        self.rfile.read(length)
        self._serve()

    do_PUT = do_PATCH = do_DELETE = do_POST

    def __getattr__(self, name: str) -> Any:
        if name.startswith("do_"):
            return self.do_POST
        raise AttributeError(name)

    def _drain(self, length: int) -> None:
        while length > 0:
            chunk = self.rfile.read(min(length, BODY_LIMIT))
            if not chunk:
                return
            length -= len(chunk)

    def _serve(self) -> None:
        target = urlsplit(self.path)
        request = Request(self.command, unquote(target.path) or "/", parse_query(target.query))
        try:
            response = self.server.handle(request)
        except Exception:
            self.server.logger.exception("answering %s %s failed", self.command, self.path)
            self._answer(lambda: self.send_error(500))
            return

        def write() -> None:
            self.send_response(response.status)
            self.send_header("Content-Type", response.content_type)
            self.send_header("Content-Length", str(len(response.body)))
            for name, value in response.headers:
                self.send_header(name, value)
            self.end_headers()
            if self.command != "HEAD":
                self.wfile.write(response.body)

        self._answer(write)

    def _answer(self, write: Callable[[], None]) -> None:
        try:
            write()
        except (BrokenPipeError, ConnectionResetError):
            self.server.logger.debug("the client %s went away before the answer", self.client_address)

    def log_message(self, format: str, *args: Any) -> None:
        self.server.logger.debug(format, *args)

from __future__ import annotations

import errno
import logging
import socket
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Callable

from .metrics import CONTENT_TYPE, Metrics
from .models import Stats

logger = logging.getLogger("tinyjobs.exporter")

STATS_UNAVAILABLE_MESSAGE = (
    "the store did not answer the stats query; serving the process counters only"
)


def address(host: str, port: int) -> str:
    return f"[{host}]:{port}" if ":" in host else f"{host}:{port}"


class MetricsServer:
    def __init__(
        self,
        metrics: Metrics,
        stats: Callable[[], Stats] | None,
        host: str = "127.0.0.1",
        port: int = 0,
    ) -> None:
        self._metrics = metrics
        self._stats = stats
        self._stats_failing = False
        self._server = _bind(host, port, self.render)
        self._thread = threading.Thread(
            target=self._server.serve_forever, name="tinyjobs-metrics", daemon=True
        )

    @property
    def host(self) -> str:
        return self._server.server_address[0]

    @property
    def port(self) -> int:
        return self._server.server_address[1]

    @property
    def url(self) -> str:
        return f"http://{address(self.host, self.port)}/metrics"

    def render(self) -> str:
        stats = None
        if self._stats is not None:
            try:
                stats = self._stats()
            except Exception:
                logger.log(
                    logging.DEBUG if self._stats_failing else logging.WARNING,
                    STATS_UNAVAILABLE_MESSAGE,
                    exc_info=True,
                )
                self._stats_failing = True
            else:
                self._stats_failing = False
        return self._metrics.render(stats)

    def start(self) -> "MetricsServer":
        self._thread.start()
        logger.info("serving metrics at %s", self.url)
        return self

    def stop(self) -> None:
        if self._thread.is_alive():
            self._server.shutdown()
            self._thread.join()
        self._server.server_close()

    def __enter__(self) -> "MetricsServer":
        return self.start()

    def __exit__(self, *exc_info: Any) -> None:
        self.stop()


def _families(host: str, port: int) -> list[int]:
    found = {family for family, *_ in socket.getaddrinfo(host, port, type=socket.SOCK_STREAM)}
    return sorted(found, key=lambda family: family != socket.AF_INET)


def unreachable(error: OSError) -> bool:
    return isinstance(error, socket.gaierror) or error.errno in (
        errno.EADDRNOTAVAIL,
        errno.EAFNOSUPPORT,
    )


def _bind(host: str, port: int, render: Callable[[], str]) -> "_Server":
    families = _families(host, port)
    for family in families[:-1]:
        try:
            return _Server((host, port), render, family)
        except OSError as error:
            if not unreachable(error):
                raise
    return _Server((host, port), render, families[-1])


class _Server(ThreadingHTTPServer):
    daemon_threads = True

    request_queue_size = 128

    def __init__(
        self, listen: tuple[str, int], render: Callable[[], str], family: int
    ) -> None:
        self.address_family = family
        super().__init__(listen, _Handler)
        self.render = render

    def handle_error(self, request: Any, client_address: Any) -> None:
        logger.debug("the metrics request from %s failed", client_address, exc_info=True)


class _Handler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        if self.path.split("?", 1)[0] != "/metrics":
            self.send_error(404)
            return
        try:
            body = self.server.render().encode("utf-8")
        except Exception:
            logger.exception("rendering the metrics failed")
            self._answer(lambda: self.send_error(500))
            return

        def write() -> None:
            self.send_response(200)
            self.send_header("Content-Type", CONTENT_TYPE)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        self._answer(write)

    def _answer(self, write: Callable[[], None]) -> None:
        try:
            write()
        except (BrokenPipeError, ConnectionResetError):
            logger.debug("the metrics client %s went away before the answer", self.client_address)

    def log_message(self, format: str, *args: Any) -> None:
        logger.debug(format, *args)

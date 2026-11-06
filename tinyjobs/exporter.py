from __future__ import annotations

import logging
from typing import Callable

from .metrics import CONTENT_TYPE, Metrics
from .models import Stats
from .serving import Endpoint
from .web import Request, Response

logger = logging.getLogger("tinyjobs.exporter")

STATS_UNAVAILABLE_MESSAGE = (
    "the store did not answer the stats query; serving the process counters only"
)


class MetricsServer(Endpoint):
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
        super().__init__(
            host, port, self.handle, name="tinyjobs-metrics", what="metrics", logger=logger
        )

    @property
    def url(self) -> str:
        return f"{self.base_url}/metrics"

    def handle(self, request: Request) -> Response:
        if request.path != "/metrics":
            return Response(404, b"not found\n")
        if request.method not in ("GET", "HEAD"):
            return Response(405, b"method not allowed\n", headers=(("Allow", "GET, HEAD"),))
        return Response(200, self.render().encode("utf-8"), CONTENT_TYPE)

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

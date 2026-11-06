from __future__ import annotations

from ..serving import Endpoint
from .core import DEFAULT_HOST, DEFAULT_PORT, Dashboard, logger


class DashboardServer(Endpoint):
    def __init__(
        self, dashboard: Dashboard, host: str = DEFAULT_HOST, port: int = DEFAULT_PORT
    ) -> None:
        super().__init__(
            host,
            port,
            dashboard.respond,
            name="tinyjobs-dashboard",
            what="the dashboard",
            logger=logger,
        )

    @property
    def url(self) -> str:
        return f"{self.base_url}/"

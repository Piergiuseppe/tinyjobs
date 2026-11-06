from __future__ import annotations

import errno
import socket
from dataclasses import dataclass, field
from http import HTTPStatus
from urllib.parse import parse_qsl

BIND_MESSAGE = "cannot serve {what} on {address}: {error}; {remedy}"
ADDRESS_REMEDY = "give {host_flag} an address of this machine, such as 127.0.0.1 or 0.0.0.0"
PORT_REMEDY = "pick a free {port_flag}, or 0 for any"
PRIVILEGED_REMEDY = "pick a {port_flag} above 1023, or 0 for any"


@dataclass(frozen=True, slots=True)
class Request:
    method: str
    path: str
    query: dict[str, str] = field(default_factory=dict)
    root: str = ""


@dataclass(frozen=True, slots=True)
class Response:
    status: int
    body: bytes = b""
    content_type: str = "text/plain; charset=utf-8"
    headers: tuple[tuple[str, str], ...] = ()

    @property
    def reason(self) -> str:
        return HTTPStatus(self.status).phrase


def address(host: str, port: int) -> str:
    return f"[{host}]:{port}" if ":" in host else f"{host}:{port}"


def families(host: str, port: int) -> list[int]:
    found = {family for family, *_ in socket.getaddrinfo(host, port, type=socket.SOCK_STREAM)}
    return sorted(found, key=lambda family: family != socket.AF_INET)


def unreachable(error: OSError) -> bool:
    return isinstance(error, socket.gaierror) or error.errno in (
        errno.EADDRNOTAVAIL,
        errno.EAFNOSUPPORT,
    )


def bind_problem(
    what: str, host: str, port: int, error: OSError, *, host_flag: str, port_flag: str
) -> str:
    if unreachable(error):
        remedy = ADDRESS_REMEDY
    elif error.errno == errno.EACCES:
        remedy = PRIVILEGED_REMEDY
    else:
        remedy = PORT_REMEDY
    return BIND_MESSAGE.format(
        what=what,
        address=address(host, port),
        error=error,
        remedy=remedy.format(host_flag=host_flag, port_flag=port_flag),
    )


def parse_query(text: str) -> dict[str, str]:
    return dict(parse_qsl(text, keep_blank_values=True))

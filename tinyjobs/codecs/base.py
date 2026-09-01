from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Protocol, runtime_checkable


@runtime_checkable
class Codec(Protocol):
    name: str

    def encode(self, value: Any) -> bytes: ...

    def decode(self, raw: bytes) -> Any: ...


class TypeAdapter(ABC):
    tag: str
    types: tuple[type, ...]

    @abstractmethod
    def pack(self, obj: Any) -> Any: ...

    @abstractmethod
    def unpack(self, data: Any) -> Any: ...

from __future__ import annotations

import base64
import dataclasses
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from enum import Enum
from pathlib import PurePath
from typing import Any
from uuid import UUID

from .base import TypeAdapter


class DateTimeAdapter(TypeAdapter):
    tag = "datetime"
    types = (datetime,)

    def pack(self, obj: datetime) -> str:
        return obj.isoformat()

    def unpack(self, data: str) -> datetime:
        return datetime.fromisoformat(data)


class DateAdapter(TypeAdapter):
    tag = "date"
    types = (date,)

    def pack(self, obj: date) -> str:
        return obj.isoformat()

    def unpack(self, data: str) -> date:
        return date.fromisoformat(data)


class TimeAdapter(TypeAdapter):
    tag = "time"
    types = (time,)

    def pack(self, obj: time) -> str:
        return obj.isoformat()

    def unpack(self, data: str) -> time:
        return time.fromisoformat(data)


class TimeDeltaAdapter(TypeAdapter):
    tag = "timedelta"
    types = (timedelta,)

    def pack(self, obj: timedelta) -> float:
        return obj.total_seconds()

    def unpack(self, data: float) -> timedelta:
        return timedelta(seconds=data)


class DecimalAdapter(TypeAdapter):
    tag = "decimal"
    types = (Decimal,)

    def pack(self, obj: Decimal) -> str:
        return str(obj)

    def unpack(self, data: str) -> Decimal:
        return Decimal(data)


class UUIDAdapter(TypeAdapter):
    tag = "uuid"
    types = (UUID,)

    def pack(self, obj: UUID) -> str:
        return str(obj)

    def unpack(self, data: str) -> UUID:
        return UUID(data)


class PathAdapter(TypeAdapter):
    tag = "path"
    types = (PurePath,)

    def pack(self, obj: PurePath) -> str:
        return str(obj)

    def unpack(self, data: str) -> Any:
        from pathlib import Path

        return Path(data)


class SetAdapter(TypeAdapter):
    tag = "set"
    types = (set,)

    def pack(self, obj: set) -> list:
        return list(obj)

    def unpack(self, data: list) -> set:
        return set(data)


class FrozenSetAdapter(TypeAdapter):
    tag = "frozenset"
    types = (frozenset,)

    def pack(self, obj: frozenset) -> list:
        return list(obj)

    def unpack(self, data: list) -> frozenset:
        return frozenset(data)


class BytesAdapter(TypeAdapter):
    tag = "bytes"
    types = (bytes,)

    def pack(self, obj: bytes) -> str:
        return base64.b64encode(obj).decode("ascii")

    def unpack(self, data: str) -> bytes:
        return base64.b64decode(data.encode("ascii"))


class ByteArrayAdapter(TypeAdapter):
    tag = "bytearray"
    types = (bytearray,)

    def pack(self, obj: bytearray) -> str:
        return base64.b64encode(bytes(obj)).decode("ascii")

    def unpack(self, data: str) -> bytearray:
        return bytearray(base64.b64decode(data.encode("ascii")))


class EnumAdapter(TypeAdapter):
    def __init__(self, enum_type: type[Enum], tag: str) -> None:
        self.tag = tag
        self.types = (enum_type,)
        self._enum_type = enum_type

    def pack(self, obj: Enum) -> Any:
        return obj.value

    def unpack(self, data: Any) -> Enum:
        return self._enum_type(data)


class DataclassAdapter(TypeAdapter):
    def __init__(self, dataclass_type: type, tag: str) -> None:
        self.tag = tag
        self.types = (dataclass_type,)
        self._dataclass_type = dataclass_type
        self._fields = tuple(f.name for f in dataclasses.fields(dataclass_type))

    def pack(self, obj: Any) -> dict:
        return {name: getattr(obj, name) for name in self._fields}

    def unpack(self, data: dict) -> Any:
        return self._dataclass_type(**data)


BUILTIN_ADAPTERS: tuple[TypeAdapter, ...] = (
    DateTimeAdapter(),
    DateAdapter(),
    TimeAdapter(),
    TimeDeltaAdapter(),
    DecimalAdapter(),
    UUIDAdapter(),
    PathAdapter(),
    SetAdapter(),
    FrozenSetAdapter(),
    BytesAdapter(),
    ByteArrayAdapter(),
)

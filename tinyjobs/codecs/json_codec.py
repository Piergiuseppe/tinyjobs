from __future__ import annotations

import dataclasses
import json
from enum import Enum
from typing import Any

from ..exceptions import SerializationError
from .adapters import BUILTIN_ADAPTERS, DataclassAdapter, EnumAdapter
from .base import TypeAdapter

TAG_KEY = "$t"
DATA_KEY = "$v"
TUPLE_TAG = "tuple"
MAP_TAG = "map"

_PRIMITIVES = (str, int, float, bool, type(None))


class JSONCodec:
    name = "json"

    def __init__(self, adapters: tuple[TypeAdapter, ...] = BUILTIN_ADAPTERS) -> None:
        self._adapters: list[TypeAdapter] = list(adapters)
        self._by_tag: dict[str, TypeAdapter] = {a.tag: a for a in self._adapters}
        self._by_type: dict[type, TypeAdapter | None] = {}
        self._rebuild_type_cache()

    def register(self, adapter: TypeAdapter) -> None:
        self._adapters.insert(0, adapter)
        self._by_tag[adapter.tag] = adapter
        self._rebuild_type_cache()

    def register_enum(self, enum_type: type[Enum], tag: str | None = None) -> None:
        self.register(EnumAdapter(enum_type, tag or enum_type.__name__.lower()))

    def register_dataclass(self, dataclass_type: type, tag: str | None = None) -> None:
        if not dataclasses.is_dataclass(dataclass_type):
            raise SerializationError(f"{dataclass_type!r} is not a dataclass")
        self.register(
            DataclassAdapter(dataclass_type, tag or dataclass_type.__name__.lower())
        )

    def encode(self, value: Any) -> bytes:
        return json.dumps(self.pack(value), separators=(",", ":")).encode("utf-8")

    def decode(self, raw: bytes) -> Any:
        return json.loads(raw.decode("utf-8"), object_hook=self._hook)

    def pack(self, value: Any) -> Any:
        if type(value) in _PRIMITIVES:
            return value
        if type(value) is list:
            return [self.pack(item) for item in value]
        if type(value) is tuple:
            return {TAG_KEY: TUPLE_TAG, DATA_KEY: [self.pack(item) for item in value]}
        if type(value) is dict:
            return self._pack_dict(value)
        adapter = self._resolve(type(value))
        if adapter is None:
            raise SerializationError(
                f"cannot serialize {type(value).__name__!r} with the json codec; "
                f"register it with app.codec.register_type(), register_dataclass() "
                f"or register_enum(), or pass identifiers instead of objects"
            )
        return {TAG_KEY: adapter.tag, DATA_KEY: self.pack(adapter.pack(value))}

    def _pack_dict(self, value: dict) -> Any:
        if self._needs_pair_form(value):
            pairs = [[self.pack(k), self.pack(v)] for k, v in value.items()]
            return {TAG_KEY: MAP_TAG, DATA_KEY: pairs}
        return {key: self.pack(item) for key, item in value.items()}

    def _needs_pair_form(self, value: dict) -> bool:
        return any(type(key) is not str for key in value) or TAG_KEY in value

    def _hook(self, obj: dict) -> Any:
        tag = obj.get(TAG_KEY)
        if tag is None or DATA_KEY not in obj or len(obj) != 2:
            return obj
        data = obj[DATA_KEY]
        if tag == TUPLE_TAG:
            return tuple(data)
        if tag == MAP_TAG:
            return {key: value for key, value in data}
        adapter = self._by_tag.get(tag)
        if adapter is None:
            raise SerializationError(f"no adapter registered for tag {tag!r}")
        return adapter.unpack(data)

    def _resolve(self, value_type: type) -> TypeAdapter | None:
        if value_type not in self._by_type:
            self._by_type[value_type] = self._search(value_type)
        return self._by_type[value_type]

    def _search(self, value_type: type) -> TypeAdapter | None:
        for adapter in self._adapters:
            if issubclass(value_type, adapter.types):
                return adapter
        return None

    def _rebuild_type_cache(self) -> None:
        self._by_type = {}
        for adapter in reversed(self._adapters):
            for adapter_type in adapter.types:
                self._by_type[adapter_type] = adapter

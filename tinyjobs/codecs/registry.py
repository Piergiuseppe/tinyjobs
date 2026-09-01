from __future__ import annotations

import logging
from enum import Enum
from typing import Any

from ..exceptions import ConfigurationError, UnknownCodecError
from .base import Codec, TypeAdapter
from .json_codec import JSONCodec
from .pickle_codec import PickleCodec

logger = logging.getLogger(__name__)

PICKLE_WARNING = (
    "pickle codec enabled: deserializing a job payload executes arbitrary code. "
    "Never enable it on a backend whose store is reachable by untrusted writers."
)


class CodecRegistry:
    def __init__(self, *, default: str = "json", allow_pickle: bool = False) -> None:
        self._json = JSONCodec()
        self._codecs: dict[str, Codec] = {self._json.name: self._json}
        self._allow_pickle = allow_pickle
        if allow_pickle:
            logger.warning(PICKLE_WARNING)
            self._codecs[PickleCodec.name] = PickleCodec()
        if default not in self._codecs:
            raise ConfigurationError(
                f"default codec {default!r} is not available"
                + ("; pass allow_pickle=True to enable it" if default == "pickle" else "")
            )
        self._default = default

    @property
    def default(self) -> str:
        return self._default

    @property
    def json(self) -> JSONCodec:
        return self._json

    def register(self, codec: Codec) -> None:
        self._codecs[codec.name] = codec

    def get(self, name: str | None = None) -> Codec:
        resolved = name or self._default
        if resolved == PickleCodec.name and not self._allow_pickle:
            raise UnknownCodecError(
                "the pickle codec is disabled; construct TinyJobs(allow_pickle=True) "
                "after reading the security note in the documentation"
            )
        if resolved not in self._codecs:
            raise UnknownCodecError(f"unknown codec {resolved!r}")
        return self._codecs[resolved]

    def register_type(self, adapter: TypeAdapter) -> None:
        self._json.register(adapter)

    def register_enum(self, enum_type: type[Enum], tag: str | None = None) -> None:
        self._json.register_enum(enum_type, tag)

    def register_dataclass(self, dataclass_type: type, tag: str | None = None) -> None:
        self._json.register_dataclass(dataclass_type, tag)

    def encode(self, value: Any, codec: str | None = None) -> tuple[bytes, str]:
        selected = self.get(codec)
        return selected.encode(value), selected.name

    def decode(self, raw: bytes, codec: str) -> Any:
        return self.get(codec).decode(raw)

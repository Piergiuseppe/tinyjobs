from __future__ import annotations

import pickle
from typing import Any


class PickleCodec:
    name = "pickle"

    def __init__(self, protocol: int = pickle.HIGHEST_PROTOCOL) -> None:
        self._protocol = protocol

    def encode(self, value: Any) -> bytes:
        return pickle.dumps(value, protocol=self._protocol)

    def decode(self, raw: bytes) -> Any:
        return pickle.loads(raw)

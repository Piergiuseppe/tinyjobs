from __future__ import annotations

from typing import Any

from ..codecs import CodecRegistry
from ..models import Job, JobPayload


class SerializationService:
    def __init__(self, codecs: CodecRegistry) -> None:
        self._codecs = codecs

    @property
    def codecs(self) -> CodecRegistry:
        return self._codecs

    def encode_payload(
        self, payload: JobPayload, codec: str | None = None
    ) -> tuple[bytes, str]:
        return self._codecs.encode(
            {"args": list(payload.args), "kwargs": payload.kwargs}, codec
        )

    def decode_payload(self, job: Job) -> JobPayload:
        raw = self._codecs.decode(job.payload, job.codec)
        return JobPayload(args=tuple(raw["args"]), kwargs=raw["kwargs"])

    def encode_result(self, value: Any, codec: str) -> bytes:
        return self._codecs.get(codec).encode(value)

    def decode_result(self, raw: bytes, codec: str) -> Any:
        return self._codecs.get(codec).decode(raw)

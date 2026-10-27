from __future__ import annotations

from dataclasses import fields

from ..backends.base import Backend
from ..clock import Clock, FollowingClock
from ..exceptions import UnsupportedCapabilityError
from ..ids import JobIdGenerator
from ..models import Job, JobPayload
from ..options import EnqueueOptions
from ..registry import TaskRegistry
from ..status import JobStatus
from .serialization import SerializationService


class DispatchService:
    def __init__(
        self,
        backend: Backend,
        serialization: SerializationService,
        clock: Clock,
        default_queue: str,
        registry: TaskRegistry,
    ) -> None:
        self._backend = backend
        self._serialization = serialization
        self._clock = clock
        self._default_queue = default_queue
        self._registry = registry
        self._ids = JobIdGenerator()

    async def dispatch_by_name(
        self, name: str, payload: JobPayload, options: EnqueueOptions
    ) -> Job:
        return await self._backend.enqueue(self.prepare(name, payload, options))

    def prepare(
        self,
        name: str,
        payload: JobPayload,
        options: EnqueueOptions,
        *,
        due_at: float | None = None,
    ) -> Job:
        return self.build(
            name, payload, self._with_task_defaults(name, options), due_at=due_at
        )

    def _with_task_defaults(
        self, name: str, options: EnqueueOptions
    ) -> EnqueueOptions:
        if name not in self._registry:
            return options
        declared = self._registry.get(name).options
        return declared.merged_with(
            **{field.name: getattr(options, field.name) for field in fields(options)}
        )

    def build(
        self,
        name: str,
        payload: JobPayload,
        options: EnqueueOptions,
        *,
        due_at: float | None = None,
    ) -> Job:
        self._verify_capabilities(options)
        now = self._clock.now()
        encoded, codec_name = self._serialization.encode_payload(
            payload, options.codec
        )
        return Job(
            id=self._ids(now if isinstance(self._clock, FollowingClock) else None),
            task=name,
            payload=encoded,
            codec=codec_name,
            queue=options.queue or self._default_queue,
            status=JobStatus.QUEUED,
            priority=options.priority or 0,
            attempts=0,
            max_attempts=options.max_attempts or 1,
            run_at=(
                options.scheduled_for(self._clock, now) if due_at is None else due_at
            ),
            created_at=now,
            dedup_key=options.key,
        )

    def _verify_capabilities(self, options: EnqueueOptions) -> None:
        capabilities = self._backend.capabilities
        if options.key is not None and not capabilities.dedup:
            raise UnsupportedCapabilityError(
                "this backend does not support deduplication keys"
            )
        if not capabilities.priority and options.priority:
            raise UnsupportedCapabilityError(
                "this backend does not support job priorities"
            )
        if not capabilities.delayed and (options.delay or options.run_at):
            raise UnsupportedCapabilityError(
                "this backend does not support delayed jobs"
            )

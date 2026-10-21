from __future__ import annotations


class TinyJobsError(Exception):
    pass


class ConfigurationError(TinyJobsError):
    pass


class UnknownBackendError(ConfigurationError):
    pass


class UnknownTaskError(TinyJobsError):
    pass


class TaskAlreadyRegisteredError(TinyJobsError):
    pass


class UnknownCodecError(ConfigurationError):
    pass


class SerializationError(TinyJobsError):
    pass


class UnsupportedCapabilityError(TinyJobsError):
    pass


class JobNotFoundError(TinyJobsError):
    pass


class SynchronousCallInsideEventLoopError(TinyJobsError):
    pass


class EventLoopStoppedError(TinyJobsError):
    pass


class WaitTimeout(TinyJobsError, TimeoutError):
    pass


class BackendNotReadyError(TinyJobsError):
    pass


class Retry(TinyJobsError):
    def __init__(self, in_: float | None = None, at: float | None = None) -> None:
        if in_ is not None and at is not None:
            raise ValueError("Retry accepts either in_ or at, not both")
        super().__init__(f"retry in {in_}s" if in_ is not None else f"retry at {at}")
        self.in_ = in_
        self.at = at


class Abandon(TinyJobsError):
    def __init__(self, reason: str = "") -> None:
        super().__init__(reason)
        self.reason = reason

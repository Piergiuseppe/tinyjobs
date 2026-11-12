from __future__ import annotations

import traceback
from datetime import datetime
from typing import Any


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


class ResultError(SerializationError):
    pass


class ResultTooLarge(ResultError):
    pass


class UnencodableResult(ResultError):
    pass


class UnsupportedCapabilityError(TinyJobsError):
    pass


class NewerStoreError(TinyJobsError, RuntimeError):
    pass


class JobNotFoundError(TinyJobsError):
    pass


class SynchronousCallInsideEventLoopError(TinyJobsError):
    pass


class EventLoopStoppedError(TinyJobsError):
    pass


class InterruptedCallError(TinyJobsError):
    pass


class ForcedInterrupt(KeyboardInterrupt):
    pass


class WaitTimeout(TinyJobsError, TimeoutError):
    pass


class BackendNotReadyError(TinyJobsError):
    pass


class AppClosedError(BackendNotReadyError):
    pass


class ExecutorClosedError(AppClosedError):
    pass


REMOTE_TRACEBACK = "_tinyjobs_traceback"


class ProcessDied(TinyJobsError):
    pass


class IsolatedTaskError(TinyJobsError):
    def __init__(self, message: str = "", type_name: str | None = None) -> None:
        super().__init__(message)
        self.type_name = type_name

    def __reduce__(self) -> tuple:
        return (type(self), (self.args[0] if self.args else "", self.type_name))


def error_type(error: BaseException) -> str:
    if isinstance(error, IsolatedTaskError) and error.type_name:
        return error.type_name
    return type(error).__name__


def error_message(error: BaseException) -> str:
    try:
        return str(error)
    except Exception:
        return f"<{type(error).__name__}: str() failed>"


def error_traceback(error: BaseException) -> str:
    remote = getattr(error, REMOTE_TRACEBACK, None)
    if remote:
        return remote
    return "".join(traceback.format_exception(type(error), error, error.__traceback__))


SHOWN_CHARACTERS = 24


def described(value: Any) -> str:
    if isinstance(value, (str, bytes)):
        return repr(_cut(value))
    try:
        return _cut(repr(value))
    except Exception:
        if isinstance(value, int):
            return f"an int of {value.bit_length()} bits"
        return f"a {type(value).__name__} that cannot be shown"


def _cut(text: Any) -> Any:
    if len(text) <= SHOWN_CHARACTERS:
        return text
    return text[:SHOWN_CHARACTERS] + ("..." if isinstance(text, str) else b"...")


class Retry(TinyJobsError):
    def __init__(
        self, in_: float | None = None, at: float | datetime | None = None
    ) -> None:
        if in_ is not None and at is not None:
            raise ValueError("Retry accepts either in_ or at, not both")
        super().__init__(f"retry in {in_}s" if in_ is not None else f"retry at {at}")
        self.in_ = in_
        self.at = at


class Abandon(TinyJobsError):
    def __init__(self, reason: str = "") -> None:
        super().__init__(reason)
        self.reason = reason

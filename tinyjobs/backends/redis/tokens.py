from __future__ import annotations

import itertools
import os
import secrets


class _Tokens:
    def __init__(self) -> None:
        self.renew()

    def renew(self) -> None:
        self._prefix = secrets.token_hex(8)
        self._counter = itertools.count()

    def next(self) -> str:
        return f"{self._prefix}-{next(self._counter)}"


_tokens = _Tokens()
os.register_at_fork(after_in_child=_tokens.renew)


def claim_token() -> str:
    return _tokens.next()

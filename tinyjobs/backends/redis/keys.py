from __future__ import annotations


class Keys:
    def __init__(self, prefix: str) -> None:
        self.root = "{" + prefix + "}"

    def __call__(self, *parts: str) -> str:
        return ":".join((self.root, *parts))

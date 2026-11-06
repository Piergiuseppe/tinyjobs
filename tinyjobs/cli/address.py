from __future__ import annotations

import argparse


def parse_port(value: str) -> int:
    number = int(value)
    if not 0 <= number <= 65535:
        raise argparse.ArgumentTypeError(f"a port is between 0 and 65535, not {value}")
    return number

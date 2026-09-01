from __future__ import annotations

import unittest
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from decimal import Decimal
from enum import Enum
from pathlib import Path
from uuid import uuid4

from tinyjobs.codecs import CodecRegistry
from tinyjobs.exceptions import SerializationError, UnknownCodecError


class Colour(Enum):
    RED = "red"
    BLUE = "blue"


@dataclass
class Money:
    amount: Decimal
    currency: str


class JSONCodecTests(unittest.TestCase):
    def setUp(self) -> None:
        self.registry = CodecRegistry()
        self.codec = self.registry.get()

    def round_trip(self, value):
        return self.codec.decode(self.codec.encode(value))

    def test_primitives_survive(self):
        for value in ("text", 42, 4.5, True, False, None):
            self.assertEqual(self.round_trip(value), value)

    def test_tuple_keeps_its_type(self):
        value = (1, (2, (3,)))
        result = self.round_trip(value)
        self.assertEqual(result, value)
        self.assertIsInstance(result, tuple)
        self.assertIsInstance(result[1], tuple)

    def test_list_stays_a_list(self):
        self.assertIsInstance(self.round_trip([1, 2]), list)

    def test_temporal_and_numeric_types(self):
        for value in (
            datetime(2026, 8, 31, 17, 34),
            date(2026, 8, 31),
            timedelta(hours=2),
            Decimal("19.99"),
            uuid4(),
            Path("/tmp/report.pdf"),
        ):
            self.assertEqual(self.round_trip(value), value)

    def test_binary_types(self):
        self.assertEqual(self.round_trip(b"\x00\xffbytes"), b"\x00\xffbytes")

    def test_sets_keep_their_type(self):
        self.assertEqual(self.round_trip({1, 2, 3}), {1, 2, 3})
        self.assertEqual(self.round_trip(frozenset({1})), frozenset({1}))

    def test_non_string_dict_keys_round_trip(self):
        value = {1: "one", (2, 3): "pair"}
        self.assertEqual(self.round_trip(value), value)

    def test_dict_containing_the_tag_key_is_not_corrupted(self):
        value = {"$t": "datetime", "$v": "not really"}
        self.assertEqual(self.round_trip(value), value)

    def test_unregistered_enum_is_refused_with_guidance(self):
        with self.assertRaises(SerializationError) as caught:
            self.codec.encode(Colour.RED)
        self.assertIn("register", str(caught.exception))

    def test_registered_enum_round_trips(self):
        self.registry.register_enum(Colour)
        self.assertIs(self.round_trip(Colour.BLUE), Colour.BLUE)

    def test_registered_dataclass_round_trips(self):
        self.registry.register_dataclass(Money)
        value = Money(Decimal("19.99"), "EUR")
        self.assertEqual(self.round_trip(value), value)

    def test_nested_registered_types(self):
        self.registry.register_dataclass(Money)
        value = {"cart": [Money(Decimal("1.50"), "EUR")], "at": datetime(2026, 1, 1)}
        self.assertEqual(self.round_trip(value), value)


class PickleGateTests(unittest.TestCase):
    def test_pickle_is_unavailable_by_default(self):
        with self.assertRaises(UnknownCodecError):
            CodecRegistry().get("pickle")

    def test_pickle_available_when_explicitly_enabled(self):
        registry = CodecRegistry(allow_pickle=True)
        codec = registry.get("pickle")
        self.assertEqual(codec.decode(codec.encode({1, 2})), {1, 2})

    def test_pickle_cannot_be_the_default_unless_enabled(self):
        with self.assertRaises(Exception):
            CodecRegistry(default="pickle")


if __name__ == "__main__":
    unittest.main()

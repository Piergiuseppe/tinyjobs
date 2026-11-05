from __future__ import annotations

import json
import logging
import sys
import unittest
from datetime import datetime, timezone

from tinyjobs.exceptions import REMOTE_TRACEBACK, IsolatedTaskError
from tinyjobs.logs import JSONFormatter, TextFormatter, failure_fields, job_fields
from tinyjobs.models import Job
from tinyjobs.status import JobStatus


def record(message: str, *args, **attributes) -> logging.LogRecord:
    made = logging.LogRecord(
        "tinyjobs.worker", logging.INFO, __file__, 1, message, args, attributes.pop("exc_info", None)
    )
    for name, value in attributes.items():
        setattr(made, name, value)
    return made


class JSONFormatterTests(unittest.TestCase):
    def test_a_record_becomes_one_json_object_carrying_its_fields(self):
        line = JSONFormatter().format(
            record("job %s succeeded", "abc", job_id="abc", attempt=1, duration_ms=1.5)
        )
        self.assertNotIn("\n", line)
        entry = json.loads(line)
        self.assertEqual(entry["message"], "job abc succeeded")
        self.assertEqual(
            (entry["level"], entry["logger"], entry["job_id"], entry["attempt"], entry["duration_ms"]),
            ("INFO", "tinyjobs.worker", "abc", 1, 1.5),
        )
        self.assertTrue(entry["time"].endswith("Z"))
        datetime.fromisoformat(entry["time"])
        for internal in ("args", "msg", "levelno", "pathname", "created", "process", "taskName"):
            self.assertNotIn(internal, entry)

    def test_an_exception_is_written_as_a_traceback(self):
        try:
            raise ValueError("boom")
        except ValueError:
            line = JSONFormatter().format(record("failed", exc_info=sys.exc_info()))
        entry = json.loads(line)
        self.assertIn("ValueError: boom", entry["traceback"])
        self.assertEqual(entry["message"], "failed")

    def test_values_that_are_not_json_are_written_as_text(self):
        when = datetime(2026, 11, 5, 9, 0, tzinfo=timezone.utc)
        entry = json.loads(JSONFormatter().format(record("at", when=when)))
        self.assertEqual(entry["when"], str(when))

    def test_a_value_json_cannot_write_is_written_as_text_and_the_others_kept(self):
        class Awkward:
            def __str__(self) -> str:
                raise RuntimeError("no")

        entry = json.loads(
            JSONFormatter().format(
                record("at", job_id="abc", attempt=1, ratio=float("nan"), odd=Awkward())
            )
        )
        self.assertEqual((entry["job_id"], entry["attempt"], entry["ratio"]), ("abc", 1, "nan"))
        self.assertIn("Awkward", entry["odd"])

    def test_a_value_that_cannot_even_be_repr_d_names_its_type(self):
        class Mute:
            def __repr__(self) -> str:
                raise RuntimeError("no")

            __str__ = __repr__

        entry = json.loads(
            JSONFormatter().format(record("at", job_id="abc", odd=Mute(), huge=10**5000))
        )
        self.assertEqual(entry["job_id"], "abc")
        self.assertEqual(entry["odd"], "<unrepresentable Mute>")
        self.assertEqual(entry["huge"], "<unrepresentable int>")

    def test_the_base_fields_cannot_be_overwritten_by_an_extra(self):
        entry = json.loads(JSONFormatter().format(record("kept", level="spoof", logger="other")))
        self.assertEqual((entry["level"], entry["logger"]), ("INFO", "tinyjobs.worker"))

    def test_a_traceback_field_is_written_as_is(self):
        entry = json.loads(JSONFormatter().format(record("failed", traceback="Traceback:\n  x\n")))
        self.assertEqual(entry["traceback"], "Traceback:\n  x\n")

    def test_a_traceback_field_wins_over_exc_info(self):
        try:
            raise ValueError("local")
        except ValueError:
            made = record("failed", exc_info=sys.exc_info(), traceback="Traceback (remote)\n")
        self.assertEqual(json.loads(JSONFormatter().format(made))["traceback"], "Traceback (remote)\n")


class TextFormatterTests(unittest.TestCase):
    def test_a_traceback_field_follows_the_line(self):
        text = TextFormatter("%(levelname)s %(message)s").format(
            record("job %s failed", "abc", traceback="Traceback:\n  x\n")
        )
        self.assertEqual(text, "INFO job abc failed\nTraceback:\n  x")

    def test_a_record_without_a_traceback_is_one_line(self):
        text = TextFormatter("%(levelname)s %(message)s").format(record("job %s failed", "abc"))
        self.assertEqual(text, "INFO job abc failed")

    def test_the_traceback_field_wins_over_exc_info_and_is_printed_once(self):
        try:
            raise ValueError("local")
        except ValueError:
            made = record("failed", exc_info=sys.exc_info(), traceback="Traceback (remote)\n")
        text = TextFormatter("%(asctime)s %(message)s").format(made)
        self.assertTrue(text.endswith(" failed\nTraceback (remote)"))
        self.assertNotIn("local", text)

    def test_the_stack_block_follows_the_traceback_field(self):
        made = record("failed", traceback="Traceback (remote)\n", stack_info="Stack:\n  s")
        text = TextFormatter("%(message)s").format(made)
        self.assertEqual(text, "failed\nTraceback (remote)\nStack:\n  s")

    def test_exc_info_alone_is_printed_the_standard_way(self):
        try:
            raise ValueError("local")
        except ValueError:
            text = TextFormatter("%(message)s").format(record("failed", exc_info=sys.exc_info()))
        self.assertIn("ValueError: local", text)


class FailureFieldsTests(unittest.TestCase):
    def test_a_retried_failure_names_the_error_without_a_traceback(self):
        self.assertEqual(
            failure_fields(ValueError("boom"), True),
            {"error": "ValueError", "error_message": "boom", "will_retry": True},
        )

    def test_a_final_failure_adds_the_traceback(self):
        try:
            raise ValueError("boom")
        except ValueError as error:
            fields = failure_fields(error, False)
        self.assertFalse(fields["will_retry"])
        self.assertIn("raise ValueError", fields["traceback"])

    def test_an_isolated_error_keeps_the_child_type_and_traceback(self):
        error = IsolatedTaskError("gone", "Custom")
        setattr(error, REMOTE_TRACEBACK, "Traceback (child)\n")
        fields = failure_fields(error, False)
        self.assertEqual((fields["error"], fields["traceback"]), ("Custom", "Traceback (child)\n"))

    def test_an_error_whose_str_fails_is_named_by_type(self):
        class Mute(Exception):
            def __str__(self):
                raise RuntimeError("no")

        self.assertEqual(failure_fields(Mute(), True)["error_message"], "<Mute: str() failed>")


class JobFieldsTests(unittest.TestCase):
    def test_job_fields_name_the_job(self):
        job = Job(
            id="j1", task="mail.send", payload=b"{}", codec="json", queue="mail",
            status=JobStatus.RUNNING, attempts=2,
        )
        self.assertEqual(
            job_fields(job, worker_id="w"),
            {"job_id": "j1", "task": "mail.send", "queue": "mail", "attempt": 2, "worker_id": "w"},
        )

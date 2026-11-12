from __future__ import annotations

import json
import os
import re
import sqlite3
import subprocess
import sys
import tempfile
import unittest
import uuid
from pathlib import Path

import tinyjobs

PREVIOUS = os.environ.get("TINYJOBS_PREVIOUS_PYTHON")
POSTGRES_URL = os.environ.get("TINYJOBS_POSTGRES_URL")
REDIS_URL = os.environ.get("TINYJOBS_REDIS_URL")
REPOSITORY = str(Path(tinyjobs.__file__).parents[1])
RACED = 300
STEP_SECONDS = 120

DRIVER = '''
import json
import os
import sys

import tinyjobs
from tinyjobs import TinyJobs

url, prefix, action = sys.argv[1:4]
app = TinyJobs(url, **({"prefix": prefix} if prefix else {}))


@app.task(name="upgrade.add", store_result=True)
def add(number, other):
    with open(os.environ["LEDGER"], "a") as ledger:
        ledger.write(f"{number}\\n")
    return number + other


def versions():
    from importlib import import_module

    found = {}
    for store, module in (
        ("sqlite", "tinyjobs.backends.sqlite.schema"),
        ("postgres", "tinyjobs.backends.postgres.schema"),
        ("redis", "tinyjobs.backends.redis.backend"),
    ):
        try:
            loaded = import_module(module)
        except ImportError:
            continue
        found[store] = [
            loaded.SCHEMA_VERSION,
            getattr(loaded, "COMPATIBLE_SINCE", loaded.SCHEMA_VERSION),
        ]
    return {"release": tinyjobs.__version__, "source": tinyjobs.__file__, "stores": found}


try:
    if action == "versions":
        print(json.dumps(versions()))
    elif action == "send":
        first, last = int(sys.argv[4]), int(sys.argv[5])
        print(json.dumps([add.enqueue(number, 1).id for number in range(first, last)]))
    elif action == "drain":
        print(json.dumps(app.drain()))
    elif action == "results":
        found = [app.get(job_id) for job_id in json.loads(sys.argv[4])]
        print(json.dumps([[str(job.status), job.result()] for job in found]))
    elif action == "counts":
        print(json.dumps(app.counts()))
finally:
    app.close()
'''


def numbered(release: str) -> tuple[int, ...]:
    return tuple(int(part) for part in re.findall(r"\d+", release)[:3])


class Release:
    def __init__(self, python: str, root: Path, ledger: Path, *, source: str | None) -> None:
        self.python = python
        self.root = root
        self.environment = {
            name: value for name, value in os.environ.items() if name != "PYTHONPATH"
        }
        self.environment["LEDGER"] = str(ledger)
        self.environment["PYTHONDONTWRITEBYTECODE"] = "1"
        if source is not None:
            self.environment["PYTHONPATH"] = source

    def start(self, url: str, prefix: str, action: str, *arguments: object) -> subprocess.Popen:
        return subprocess.Popen(
            [self.python, "driver.py", url, prefix, action, *map(str, arguments)],
            cwd=self.root,
            env=self.environment,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )

    def ends(self, running: subprocess.Popen) -> tuple[int, str, str]:
        try:
            output, errors = running.communicate(timeout=STEP_SECONDS)
        except subprocess.TimeoutExpired:
            running.kill()
            running.communicate()
            raise
        return running.returncode, output, errors

    def run(self, url: str, prefix: str, action: str, *arguments: object):
        code, output, errors = self.ends(self.start(url, prefix, action, *arguments))
        if code != 0:
            raise AssertionError(f"{action} left with {code}: {errors[-2000:]}")
        return json.loads(output)

    def refused(self, url: str, prefix: str, action: str, *arguments: object) -> str:
        code, _, errors = self.ends(self.start(url, prefix, action, *arguments))
        if code == 0:
            raise AssertionError(f"{action} was expected to be refused and it ran")
        return errors


@unittest.skipUnless(
    PREVIOUS,
    "set TINYJOBS_PREVIOUS_PYTHON to an interpreter with the previous release installed",
)
class UpgradeTests(unittest.TestCase):
    def setUp(self) -> None:
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)
        (self.root / "driver.py").write_text(DRIVER)
        self.ledger = self.root / "ledger"
        self.ledger.touch()
        self.old = Release(PREVIOUS, self.root, self.ledger, source=None)
        self.new = Release(sys.executable, self.root, self.ledger, source=REPOSITORY)
        self.old_versions = self.old.run("memory://", "", "versions")
        self.new_versions = self.new.run("memory://", "", "versions")
        self.assertFalse(self.old_versions["source"].startswith(REPOSITORY))
        self.assertTrue(self.new_versions["source"].startswith(REPOSITORY))
        self.assertLess(
            numbered(self.old_versions["release"]), numbered(self.new_versions["release"])
        )

    def ran(self) -> list[int]:
        return sorted(int(line) for line in self.ledger.read_text().split())

    def works_beside(self, store: str) -> bool:
        understood, _ = self.old_versions["stores"][store]
        _, since = self.new_versions["stores"][store]
        return since <= understood

    def succeeded(self, release: Release, url: str, prefix: str, ids: list[str], first: int):
        found = release.run(url, prefix, "results", json.dumps(ids))
        self.assertEqual(
            found, [["success", number + 1] for number in range(first, first + len(ids))]
        )

    def upgrade(self, store: str, url: str, prefix: str, stored) -> None:
        old, new = self.old, self.new
        before = old.run(url, prefix, "send", 0, 20)
        self.assertEqual(old.run(url, prefix, "drain"), 20)
        waiting = old.run(url, prefix, "send", 20, 40)
        self.assertEqual(stored(), self.old_versions["stores"][store][0])
        sent = new.run(url, prefix, "send", 40, 60)
        self.assertEqual(stored(), self.new_versions["stores"][store][0])
        self.succeeded(new, url, prefix, before, 0)
        self.assertEqual(new.run(url, prefix, "counts"), {"queued": 40, "success": 20})
        if not self.works_beside(store):
            self.assertIn("newer than this tinyjobs", old.refused(url, prefix, "counts"))
            self.assertIn("newer than this tinyjobs", old.refused(url, prefix, "send", 0, 1))
            self.assertEqual(new.run(url, prefix, "drain"), 40)
            self.succeeded(new, url, prefix, waiting + sent, 20)
            self.assertEqual(self.ran(), list(range(60)))
            return
        self.assertEqual(old.run(url, prefix, "drain"), 40)
        self.succeeded(new, url, prefix, waiting + sent, 20)
        late = old.run(url, prefix, "send", 60, 80)
        self.assertEqual(new.run(url, prefix, "drain"), 20)
        self.succeeded(old, url, prefix, late, 60)
        raced = new.run(url, prefix, "send", 80, 80 + RACED // 2)
        raced += old.run(url, prefix, "send", 80 + RACED // 2, 80 + RACED)
        racers = [release.start(url, prefix, "drain") for release in (old, new, old, new)]
        drained = []
        for release, running in zip((old, new, old, new), racers):
            code, output, errors = release.ends(running)
            self.assertEqual(code, 0, errors[-2000:])
            drained.append(json.loads(output))
        self.assertEqual(sum(drained), RACED)
        self.succeeded(old, url, prefix, raced, 80)
        self.assertEqual(self.ran(), list(range(80 + RACED)))
        self.assertEqual(old.run(url, prefix, "counts"), {"success": 80 + RACED})
        self.assertEqual(new.run(url, prefix, "counts"), {"success": 80 + RACED})

    def sqlite_version(self, prefix: str) -> int:
        with sqlite3.connect(self.root / "jobs.db") as connection:
            [(stored,)] = connection.execute(
                f"SELECT value FROM {prefix}_meta WHERE key = 'schema_version'"
            )
        connection.close()
        return int(stored)

    def test_a_sqlite_file_of_the_previous_release_is_upgraded_and_shared(self):
        url = f"sqlite:///{self.root / 'jobs.db'}"
        self.upgrade("sqlite", url, "", lambda: self.sqlite_version("tinyjobs"))

    def test_a_sqlite_file_under_a_prefix_is_upgraded_and_shared(self):
        url = f"sqlite:///{self.root / 'jobs.db'}"
        self.upgrade("sqlite", url, "shop", lambda: self.sqlite_version("shop"))

    @unittest.skipUnless(POSTGRES_URL, "set TINYJOBS_POSTGRES_URL to run the postgres tests")
    def test_a_postgres_store_of_the_previous_release_is_upgraded_and_shared(self):
        import psycopg

        prefix = f"tinyjobs_test_{uuid.uuid4().hex[:16]}"

        def drop() -> None:
            with psycopg.connect(POSTGRES_URL, autocommit=True) as connection:
                tables = connection.execute(
                    "SELECT tablename FROM pg_tables WHERE schemaname = current_schema() "
                    "AND left(tablename, %s) = %s",
                    (len(prefix) + 1, f"{prefix}_"),
                ).fetchall()
                for (table,) in tables:
                    connection.execute(f'DROP TABLE IF EXISTS "{table}" CASCADE')

        def stored() -> int:
            with psycopg.connect(POSTGRES_URL, autocommit=True) as connection:
                [(found,)] = connection.execute(
                    f"SELECT value FROM \"{prefix}_meta\" WHERE key = 'schema_version'"
                ).fetchall()
            return int(found)

        self.addCleanup(drop)
        self.upgrade("postgres", POSTGRES_URL, prefix, stored)

    @unittest.skipUnless(REDIS_URL, "set TINYJOBS_REDIS_URL to run the redis tests")
    def test_a_redis_store_of_the_previous_release_is_upgraded_and_shared(self):
        import redis

        prefix = f"tinyjobs-test-{uuid.uuid4().hex}"

        def drop() -> None:
            client = redis.Redis.from_url(REDIS_URL)
            try:
                keys = list(client.scan_iter(match=f"{{{prefix}}}:*", count=10_000))
                if keys:
                    client.delete(*keys)
            finally:
                client.close()

        def stored() -> int:
            client = redis.Redis.from_url(REDIS_URL)
            try:
                return int(client.get(f"{{{prefix}}}:schema_version"))
            finally:
                client.close()

        self.addCleanup(drop)
        self.upgrade("redis", REDIS_URL, prefix, stored)


if __name__ == "__main__":
    unittest.main()

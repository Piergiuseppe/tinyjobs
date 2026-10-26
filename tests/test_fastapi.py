from __future__ import annotations

import os
import tempfile
import time
import unittest
import uuid
from pathlib import Path

from tinyjobs import JobStatus, TinyJobs
from tests.test_redis import forget

REDIS_URL = os.environ.get("TINYJOBS_REDIS_URL")

try:
    from fastapi import FastAPI
    from fastapi.testclient import TestClient
except ImportError:
    FastAPI = None

SETTLE_TIMEOUT = 10.0


@unittest.skipIf(FastAPI is None, "pip install fastapi httpx to run the fastapi tests")
class FastAPITests(unittest.TestCase):
    def make_jobs(self) -> TinyJobs:
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        return TinyJobs(f"sqlite:///{Path(self.directory.name) / 'jobs.db'}")

    def setUp(self) -> None:
        self.jobs = self.make_jobs()
        self.addCleanup(self.jobs.close)

        @self.jobs.task(store_result=True)
        async def double(value: int) -> int:
            return value * 2

        api = FastAPI(lifespan=self.jobs.lifespan(worker=True, poll_maximum=0.05))

        @api.post("/async/{value}")
        async def from_async(value: int) -> dict:
            return {"id": (await double.aenqueue(value)).id}

        @api.post("/sync/{value}")
        def from_sync(value: int) -> dict:
            return {"id": double.enqueue(value).id}

        @api.get("/jobs/{job_id}")
        async def show(job_id: str) -> dict:
            job = await self.jobs.aget(job_id)
            return {"status": job.status.value, "result": job.result() if job.status is JobStatus.SUCCESS else None}

        self.api = api

    def settle(self, client: TestClient, job_id: str) -> dict:
        deadline = time.monotonic() + SETTLE_TIMEOUT
        while time.monotonic() < deadline:
            body = client.get(f"/jobs/{job_id}").json()
            if body["status"] == JobStatus.SUCCESS.value:
                return body
            time.sleep(0.02)
        return body

    def test_async_and_sync_endpoints_enqueue_and_the_embedded_worker_runs_them(self):
        with TestClient(self.api) as client:
            ids = [
                client.post(f"/{kind}/{value}").json()["id"]
                for kind in ("async", "sync")
                for value in range(5)
            ]
            results = [self.settle(client, job_id)["result"] for job_id in ids]
        self.assertEqual(results, [0, 2, 4, 6, 8] * 2)


@unittest.skipIf(FastAPI is None, "pip install fastapi httpx to run the fastapi tests")
@unittest.skipUnless(REDIS_URL, "set TINYJOBS_REDIS_URL to run the redis tests")
class FastAPIRedisTests(FastAPITests):
    def make_jobs(self) -> TinyJobs:
        self.prefix = f"tinyjobs-test-{uuid.uuid4().hex}"
        return TinyJobs(REDIS_URL, prefix=self.prefix)

    def tearDown(self) -> None:
        self.jobs.start()
        self.jobs.run_sync(lambda: forget(self.jobs, self.prefix), name="forget")
        self.jobs.close()


if __name__ == "__main__":
    unittest.main()

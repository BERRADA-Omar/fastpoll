from typing import AsyncGenerator
import pytest
import asyncio
from fastpoll.impl.fastpoll_mongo import FastPollMongo
import mongomock_motor
import pytest_asyncio
from fastpoll.fastpoll_models import FPJobStatus

pytestmark = pytest.mark.asyncio

@pytest_asyncio.fixture(scope="function")
async def fastpoll_mongo() -> FastPollMongo:
    # Use mongomock_motor for in-memory MongoDB
    client = mongomock_motor.AsyncMongoMockClient()
    db = client["testdb"]
    collection = db["jobs"]
    fastpoll_mongo = await FastPollMongo.create(heartbeat_interval_seconds=0.01, mongo_collection=collection)
    return fastpoll_mongo


async def dummy_job(x, y, report_progress=None):
    if report_progress:
        await report_progress(50, "Halfway done")
    await asyncio.sleep(0.01)
    return x + y


@pytest.mark.asyncio
async def test_create_and_complete_job(fastpoll_mongo: FastPollMongo):
    job_id = await fastpoll_mongo.run_job(dummy_job, 2, 3)
    # Poll until job is completed or timeout
    timeout = 1.0  # seconds
    interval = 0.01
    elapsed = 0
    while elapsed < timeout:
        status = await fastpoll_mongo.get_job_status(job_id)
        if status.status == "completed":
            break
        await asyncio.sleep(interval)
        elapsed += interval
    assert status.status == "completed"
    assert status.result == 5


@pytest.mark.asyncio
async def test_fail_job(fastpoll_mongo: FastPollMongo):
    async def fail_job(report_progress=None):
        raise ValueError("fail!")

    job_id = await fastpoll_mongo.run_job(fail_job)
    timeout = 1.0
    interval = 0.01
    elapsed = 0
    while elapsed < timeout:
        status = await fastpoll_mongo.get_job_status(job_id)
        if status.status == "failed":
            break
        await asyncio.sleep(interval)
        elapsed += interval
    assert status.status == "failed"
    assert "fail!" in status.error


@pytest.mark.asyncio
async def test_report_progress(fastpoll_mongo: FastPollMongo):
    async def progress_job(report_progress=None):
        await report_progress(42, "step")
        await asyncio.sleep(0.01)
        return 1

    job_id = await fastpoll_mongo.run_job(progress_job)
    timeout = 1.0
    interval = 0.01
    elapsed = 0
    while elapsed < timeout:
        status = await fastpoll_mongo.get_job_status(job_id)
        if status.status == "completed":
            break
        await asyncio.sleep(interval)
        elapsed += interval
    assert status.status == "completed"
    assert status.progress == 100
    assert status.result == 1


@pytest.mark.asyncio
async def test_lock_prolong(fastpoll_mongo: FastPollMongo):
    job_id = await fastpoll_mongo.run_job(dummy_job, 1, 2)
    timeout = 1.0
    interval = 0.01
    elapsed = 0
    while elapsed < timeout:
        status = await fastpoll_mongo.get_job_status(job_id)
        if status.status == "completed":
            break
        await asyncio.sleep(interval)
        elapsed += interval
    await fastpoll_mongo._prolong_lock_duration(job_id)
    status = await fastpoll_mongo.get_job_status(job_id)
    assert status.locked_until is not None


@pytest.mark.asyncio
async def test_job_not_found(fastpoll_mongo: FastPollMongo):
    with pytest.raises(ValueError):
        await fastpoll_mongo.get_job_status("000000000000000000000000")


@pytest.mark.asyncio
async def test_progress_zero_and_hundred(fastpoll_mongo: FastPollMongo):
    async def progress_job(report_progress=None):
        await report_progress(0, "start")
        await asyncio.sleep(0.01)
        await report_progress(100, "done")
        return 42
    job_id = await fastpoll_mongo.run_job(progress_job)
    timeout = 1.0
    interval = 0.01
    elapsed = 0
    while elapsed < timeout:
        status = await fastpoll_mongo.get_job_status(job_id)
        if status.status == "completed":
            break
        await asyncio.sleep(interval)
        elapsed += interval
    assert status.status == "completed"
    assert status.progress == 100
    assert status.result == 42


@pytest.mark.asyncio
async def test_no_progress_reporting(fastpoll_mongo: FastPollMongo):
    async def no_progress_job():
        await asyncio.sleep(0.01)
        return 7
    job_id = await fastpoll_mongo.run_job(no_progress_job)
    timeout = 1.0
    interval = 0.01
    elapsed = 0
    while elapsed < timeout:
        status = await fastpoll_mongo.get_job_status(job_id)
        if status.status == "completed":
            break
        await asyncio.sleep(interval)
        elapsed += interval
    assert status.status == "completed"
    assert status.result == 7
    assert status.progress in (None, 100)


@pytest.mark.asyncio
async def test_fail_job_with_different_error_types(fastpoll_mongo: FastPollMongo):
    async def fail_type_error(report_progress=None):
        raise TypeError("type error!")
    job_id = await fastpoll_mongo.run_job(fail_type_error)
    timeout = 1.0
    interval = 0.01
    elapsed = 0
    while elapsed < timeout:
        status = await fastpoll_mongo.get_job_status(job_id)
        if status.status == "failed":
            break
        await asyncio.sleep(interval)
        elapsed += interval
    assert status.status == "failed"
    assert "type error!" in status.error


@pytest.mark.asyncio
async def test_lock_prolong_on_non_running_job(fastpoll_mongo: FastPollMongo):
    async def quick_job():
        return 1
    job_id = await fastpoll_mongo.run_job(quick_job)
    timeout = 1.0
    interval = 0.01
    elapsed = 0
    while elapsed < timeout:
        status = await fastpoll_mongo.get_job_status(job_id)
        if status.status == "completed":
            break
        await asyncio.sleep(interval)
        elapsed += interval
    # Try to prolong lock on completed job (should not raise)
    await fastpoll_mongo._prolong_lock_duration(job_id)
    status2 = await fastpoll_mongo.get_job_status(job_id)
    assert status2.locked_until is not None


@pytest.mark.asyncio
async def test_all_job_fields(fastpoll_mongo: FastPollMongo):
    async def job_with_args(a, b, report_progress=None):
        await report_progress(10, "ten percent")
        return a * b
    job_id = await fastpoll_mongo.run_job(job_with_args, 3, 4)
    timeout = 1.0
    interval = 0.01
    elapsed = 0
    while elapsed < timeout:
        status = await fastpoll_mongo.get_job_status(job_id)
        if status.status == "completed":
            break
        await asyncio.sleep(interval)
        elapsed += interval
    assert status.id == job_id
    assert status.status == "completed"
    assert status.created_at is not None
    assert status.started_at is not None
    assert status.updated_at is not None
    assert status.locked_until is not None
    assert status.input_args is not None
    assert status.input_kwargs is not None
    assert status.result == 12
    assert status.progress == 100

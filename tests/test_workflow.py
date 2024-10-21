import asyncio
import pytest


@pytest.mark.asyncio
async def test_one_job(scheduler, client):
    expected_run_time = 4
    requests_cpu = 2.0
    requests_memory = 200
    job_id = client.post('/jobs', json={
        "expected_run_time": expected_run_time,
        "requests_cpu": requests_cpu,
        "requests_memory": requests_memory
    }).json()['id']
    created_job = client.get(f'/jobs/{job_id}').json()
    assert created_job['id'] == job_id
    assert created_job['status'] == "new"

    jobs_capacity = 20
    cpu_capacity = 2.0
    memory_capacity = 1000
    assert client.post('/nodes', json={
        "jobs_capacity": jobs_capacity,
        "cpu_capacity": cpu_capacity,
        "memory_capacity": memory_capacity
    }).status_code == 201

    try:
        async with asyncio.timeout(2):
            await scheduler.run()
    except TimeoutError:
        pass

    job = client.get(f'/jobs/{job_id}').json()
    assert job['id'] == job_id
    assert job['status'] == "running"

    try:
        async with asyncio.timeout(3):
            await scheduler.run()
    except TimeoutError:
        pass

    job = client.get(f'/jobs/{job_id}').json()
    assert job['id'] == job_id
    assert job['status'] == "completed"

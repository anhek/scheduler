from fastapi import status


def test_empty_on_start(client):
    assert client.get('/jobs').json() == []


def test_job_add(client):
    expected_run_time = 3
    requests_cpu = 1.0
    requests_memory = 100
    job_id = client.post('/jobs', json={
        "expected_run_time": expected_run_time,
        "requests_cpu": requests_cpu,
        "requests_memory": requests_memory
    }).json()['id']
    created_job = client.get(f'/jobs/{job_id}').json()
    assert created_job['id'] == job_id
    assert created_job['expected_run_time'] == expected_run_time
    assert created_job['requests_cpu'] == requests_cpu
    assert created_job['requests_memory'] == requests_memory
    assert created_job['status'] == "new"
    assert client.get('/jobs').json()[0]['id'] == job_id


def test_job_deletion(client):
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
    assert client.delete(f'/jobs/{job_id}').status_code == status.HTTP_200_OK
    assert client.get(f'/jobs/{job_id}').status_code == status.HTTP_404_NOT_FOUND
    assert client.get('/jobs').json() == []

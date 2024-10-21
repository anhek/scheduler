from fastapi import status


def test_empty_on_start(client):
    assert client.get('/nodes').json() == []


def test_node_add(client):
    jobs_capacity = 10
    cpu_capacity = 4.0
    memory_capacity = 2000
    node_id = client.post('/nodes', json={
        "jobs_capacity": jobs_capacity,
        "cpu_capacity": cpu_capacity,
        "memory_capacity": memory_capacity
    }).json()['id']
    created_node = client.get(f'/nodes/{node_id}').json()
    assert created_node['id'] == node_id
    assert created_node['jobs_capacity'] == jobs_capacity
    assert created_node['cpu_capacity'] == cpu_capacity
    assert created_node['memory_capacity'] == memory_capacity
    assert created_node['jobs_allocated'] == 0
    assert created_node['cpu_allocated'] == 0
    assert created_node['memory_allocated'] == 0
    assert client.get('/nodes').json()[0]['id'] == node_id


def test_node_deletion(client):
    jobs_capacity = 20
    cpu_capacity = 2.0
    memory_capacity = 1000
    node_id = client.post('/nodes', json={
        "jobs_capacity": jobs_capacity,
        "cpu_capacity": cpu_capacity,
        "memory_capacity": memory_capacity
    }).json()['id']
    created_node = client.get(f'/nodes/{node_id}').json()

    assert created_node['id'] == node_id
    assert client.delete(f'/nodes/{node_id}').status_code == status.HTTP_200_OK
    assert client.get(f'/nodes/{node_id}').status_code == status.HTTP_404_NOT_FOUND
    assert client.get('/nodes').json() == []

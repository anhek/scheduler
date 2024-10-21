import pytest
from fastapi.testclient import TestClient
from fastapi import FastAPI
from webserver import register_urls
from scheduler import Scheduler
from storage import StorageType


@pytest.fixture(scope="function")
def scheduler():
    return Scheduler(StorageType.MEMORY)


@pytest.fixture(scope="function")
def client(scheduler):
    app = FastAPI()
    register_urls(app, scheduler)
    client = TestClient(app)
    return client

from enum import StrEnum, Enum
from typing import Optional

from pydantic import BaseModel


class ActionStatus(Enum):
    OK = 0
    NOT_FOUND = 1


class JobStatus(StrEnum):
    NEW = 'new'
    RUNNING = 'running'
    COMPLETED = 'completed'
    TERMINATED = 'terminated'


Id = str


class NewJob(BaseModel):
    expected_run_time: int
    requests_cpu: float
    requests_memory: int


class Job(BaseModel):
    id: Id
    status: JobStatus
    expected_run_time: int
    requests_cpu: float
    requests_memory: int
    created_at: float
    started_at: Optional[float]


class NewNode(BaseModel):
    jobs_capacity: int
    cpu_capacity: float
    memory_capacity: int


class Node(BaseModel):
    id: Id
    jobs_capacity: int
    jobs_allocated: Optional[int]
    cpu_capacity: float
    cpu_allocated: Optional[float]
    memory_capacity: int
    memory_allocated: Optional[int]

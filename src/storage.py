from abc import ABC, abstractmethod
from enum import StrEnum
from typing import List

from entity import Id, Job, Node, ActionStatus


class StorageType(StrEnum):
    MEMORY = 'memory'
    POSTGRESQL = 'postgresql'
    REDIS = 'redis'


class Storage(ABC):
    @abstractmethod
    def __init__(self):
        pass

    @abstractmethod
    def close(self):
        pass

    @abstractmethod
    async def add_node(self, node: Node):
        pass

    @abstractmethod
    async def get_node(self, node_id: Id) -> Node | None:
        pass

    @abstractmethod
    async def update_node(self, node: Node) -> ActionStatus:
        pass

    @abstractmethod
    async def delete_node(self, node_id: Id) -> ActionStatus:
        pass

    @abstractmethod
    async def get_nodes(self) -> List[Node]:
        pass

    @abstractmethod
    async def add_job(self, job: Job):
        pass

    @abstractmethod
    async def get_job(self, job_id: Id) -> Job | None:
        pass

    @abstractmethod
    async def update_job(self, job: Job) -> ActionStatus:
        pass

    @abstractmethod
    async def delete_job(self, job_id: Id) -> ActionStatus:
        pass

    @abstractmethod
    async def get_jobs(self) -> List[Job]:
        pass


class MemoryStorage(Storage):
    def __init__(self):
        self.jobs = {}
        self.nodes = {}

    def close(self):
        del self.nodes
        del self.jobs

    async def add_node(self, node: Node):
        self.nodes.update({node.id: node})

    async def get_node(self, node_id: Id) -> Node | None:
        return self.nodes.get(node_id, None)

    async def update_node(self, node: Node) -> ActionStatus:
        if node.id in self.nodes:
            self.nodes[node.id] = node
            return ActionStatus.OK
        else:
            return ActionStatus.NOT_FOUND

    async def delete_node(self, node_id: Id) -> ActionStatus:
        if node_id in self.nodes:
            del self.nodes[node_id]
            return ActionStatus.OK
        else:
            return ActionStatus.NOT_FOUND

    async def get_nodes(self) -> List[Node]:
        return list(self.nodes.values())

    async def add_job(self, job: Job):
        self.jobs.update({job.id: job})

    async def get_job(self, job_id: Id) -> Job | None:
        return self.jobs.get(job_id, None)

    async def update_job(self, job: Job) -> ActionStatus:
        if job.id in self.jobs:
            self.jobs[job.id] = job
            return ActionStatus.OK
        else:
            return ActionStatus.NOT_FOUND

    async def delete_job(self, job_id: Id):
        if job_id in self.jobs:
            del self.jobs[job_id]
            return ActionStatus.OK
        else:
            return ActionStatus.NOT_FOUND

    async def get_jobs(self) -> List[Job]:
        return list(self.jobs.values())


def get_storage(storage_type: StorageType) -> Storage:
    match storage_type:
        case StorageType.MEMORY:
            return MemoryStorage()
        case StorageType.POSTGRESQL:
            raise NotImplementedError(f"Storage {storage_type} not implemented")
        case StorageType.REDIS:
            raise NotImplementedError(f"Storage {storage_type} not implemented")
        case _:
            raise Exception(f"Unexpected storage type {storage_type}")

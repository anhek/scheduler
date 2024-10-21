import asyncio
import signal
import logging
from typing import List
from time import time

from storage import get_storage
from entity import NewJob, Job, NewNode, Node, ActionStatus, JobStatus, Id

logger = logging.getLogger("scheduler")

SCHEDULING_INTERVAL = 60


def fit_available(job: Job, nodes: List[Node]) -> List[Node]:
    result = []
    for node in nodes:
        if (node.jobs_allocated < node.jobs_capacity) and (
                (node.cpu_capacity - node.cpu_allocated) >= job.requests_cpu) and (
                (node.memory_capacity - node.memory_allocated) >= job.requests_memory):
            result.append(node)
    return result


def recalc_allocated_resources(node: Node, running_jobs: List[Job]):
    node.jobs_allocated = 0
    node.cpu_allocated = 0
    node.memory_allocated = 0
    for job in running_jobs:
        node.jobs_allocated += 1
        node.cpu_allocated += job.requests_cpu
        node.memory_allocated += job.requests_memory
    return node


class Scheduler:
    def __init__(self, storage_type):
        signal.signal(signal.SIGINT, self._shutdown)
        storage = get_storage(storage_type)
        self._storage = storage
        self.next_job_id = 1
        self.next_node_id = 1
        self.node_jobs = {}
        self.jobs_nodes = {}
        self.pending_jobs = []
        self.lock = asyncio.Lock()
        self.next_schedule_time = time()

    def _shutdown(self, _sig, _frame):
        self._storage.close()

    async def run(self):
        while True:
            await self._tick()
            await asyncio.sleep(1)

    async def _tick(self):
        if time() >= self.next_schedule_time:
            async with self.lock:
                await self._complete_running_jobs()
                await self._schedule_jobs()

    async def _complete_running_jobs(self):
        nodes = await self._storage.get_nodes()
        next_schedule_time = time() + SCHEDULING_INTERVAL
        if self.next_schedule_time < time():
            self.next_schedule_time = next_schedule_time
        for node in nodes:
            completed_jobs = []
            running_jobs = []
            node_jobs = self.node_jobs[node.id]
            for job_id in node_jobs:
                job = await self._storage.get_job(job_id)
                job_completion_time = job.started_at + job.expected_run_time
                if job_completion_time < time():
                    completed_jobs.append(job_id)
                    job.status = JobStatus.COMPLETED
                    await self._storage.update_job(job)
                    del self.jobs_nodes[job_id]
                    logger.info(f"Completed job {job_id} on node {node.id}")
                else:
                    if job_completion_time < next_schedule_time:
                        next_schedule_time = job_completion_time
                    running_jobs.append(job)
            for job_id in completed_jobs:
                node_jobs.remove(job_id)
            node = recalc_allocated_resources(node, running_jobs)
            await self._storage.update_node(node)
        if self.next_schedule_time > next_schedule_time:
            self.next_schedule_time = next_schedule_time

    async def _schedule_jobs(self):
        nodes = await self._storage.get_nodes()
        next_schedule_time = time() + SCHEDULING_INTERVAL
        if self.next_schedule_time < time():
            self.next_schedule_time = next_schedule_time
        assigned_jobs = []
        for job_id in self.pending_jobs:
            job = await self._storage.get_job(job_id)
            if not job:
                continue
            available_nodes = fit_available(job, nodes)
            if len(available_nodes) > 0:
                node = available_nodes[0]
                node = await self._storage.get_node(node.id)
                if not node:
                    continue

                self.jobs_nodes[job_id] = node.id
                job.status = JobStatus.RUNNING
                job.started_at = time()
                await self._storage.update_job(job)

                self.node_jobs[node.id].append(job_id)
                node.jobs_allocated += 1
                node.cpu_allocated += job.requests_cpu
                node.memory_allocated += job.requests_memory
                await self._storage.update_node(node)

                job_completion_time = job.started_at + job.expected_run_time
                if job_completion_time < next_schedule_time:
                    next_schedule_time = job_completion_time
                assigned_jobs.append(job_id)
                logger.info(f"Job {job_id} assigned to node {node.id}")
        for job_id in assigned_jobs:
            self.pending_jobs.remove(job_id)
        if self.next_schedule_time > next_schedule_time:
            self.next_schedule_time = next_schedule_time

    async def get_jobs(self) -> List[Job]:
        return await self._storage.get_jobs()

    async def get_nodes(self) -> List[Node]:
        return await self._storage.get_nodes()

    async def get_job(self, job_id) -> Job | None:
        return await self._storage.get_job(job_id)

    async def get_node(self, node_id) -> Node | None:
        return await self._storage.get_node(node_id)

    async def new_job(self, new_job: NewJob) -> Id:
        async with self.lock:
            job_id = str(self.next_job_id)
            self.next_job_id += 1
            job = Job(
                id=job_id,
                status=JobStatus.NEW,
                expected_run_time=new_job.expected_run_time,
                requests_cpu=new_job.requests_cpu,
                requests_memory=new_job.requests_memory,
                created_at=time(),
                started_at=None
            )
            self.pending_jobs.append(job_id)
            await self._storage.add_job(job)
            self.next_schedule_time = time()
            return job_id

    async def delete_job(self, job_id) -> ActionStatus:
        async with self.lock:
            if job_id in self.pending_jobs:
                self.pending_jobs.remove(job_id)
                self.next_schedule_time = time()
            if job_id in self.jobs_nodes:
                node_id = self.jobs_nodes[job_id]
                del self.jobs_nodes[job_id]
                self.node_jobs[node_id].remove(job_id)
                self.next_schedule_time = time()
            return await self._storage.delete_job(job_id)

    async def terminate_job(self, job_id) -> ActionStatus:
        async with self.lock:
            if job_id in self.jobs_nodes:
                node_id = self.jobs_nodes[job_id]
                del self.jobs_nodes[job_id]
                self.node_jobs[node_id].remove(job_id)
                self.next_schedule_time = time()
                job = await self._storage.get_job(job_id)
                job.status = JobStatus.TERMINATED
                await self._storage.update_job(job)
                return ActionStatus.OK
            else:
                return ActionStatus.NOT_FOUND

    async def get_node_jobs(self, node_id) -> List[Job] | None:
        if node_id not in self.node_jobs:
            return None
        result = []
        job_ids = self.node_jobs[node_id]
        for job_id in job_ids:
            job = await self._storage.get_job(job_id)
            if job:
                result.append(job)
        return result

    async def add_node(self, new_node: NewNode) -> Id:
        async with self.lock:
            node_id = str(self.next_node_id)
            self.next_node_id += 1
            node = Node(
                id=node_id,
                jobs_capacity=new_node.jobs_capacity,
                jobs_allocated=0,
                cpu_capacity=new_node.cpu_capacity,
                cpu_allocated=0,
                memory_capacity=new_node.memory_capacity,
                memory_allocated=0
            )
            await self._storage.add_node(node)
            self.node_jobs[node_id] = []
            self.next_schedule_time = time()
            return node_id

    async def delete_node(self, node_id: Id) -> ActionStatus:
        async with self.lock:
            interrupted_jobs = self.node_jobs[node_id]
            if len(interrupted_jobs) > 0:
                for job_id in interrupted_jobs:
                    job = await self._storage.get_job(job_id)
                    job.status = JobStatus.NEW
                    job.started_at = None
                    await self._storage.update_job(job)
                    del self.jobs_nodes[job_id]
                interrupted_jobs.extend(self.pending_jobs)
                self.pending_jobs = interrupted_jobs
                del self.node_jobs[node_id]
                self.next_schedule_time = time()
            return await self._storage.delete_node(node_id)

import logging
import sys
from typing import Literal, List
from pydantic import BaseModel
import click
import asyncio
from fastapi import FastAPI, Response, status
import uvicorn

from entity import ActionStatus, NewJob, NewNode, Job, Node, Id
from scheduler import Scheduler
from storage import StorageType


class ResponseModel(BaseModel):
    status: Literal['ok', 'error']


class CreateResponseModel(BaseModel):
    status: Literal['ok', 'error']
    id: Id


def register_urls(app: FastAPI, scheduler: Scheduler):
    @app.get('/jobs')
    async def get_jobs() -> List[Job]:
        return await scheduler.get_jobs()

    @app.post('/jobs', status_code=status.HTTP_201_CREATED)
    async def new_job(job: NewJob) -> CreateResponseModel:
        id = await scheduler.new_job(job)
        return CreateResponseModel(status='ok', id=id)

    @app.delete('/jobs/{job_id}', responses={status.HTTP_404_NOT_FOUND: {}})
    async def delete_job(job_id: Id, response: Response) -> ResponseModel:
        result = await scheduler.delete_job(job_id)
        match result:
            case ActionStatus.OK:
                return ResponseModel(status='ok')
            case ActionStatus.NOT_FOUND:
                response.status_code = status.HTTP_404_NOT_FOUND
                return ResponseModel(status='error')

    @app.post('/jobs/{job_id}/status')
    async def update_job(job_id: Id, action: str, response: Response) -> ResponseModel:
        match action:
            case "terminate":
                result = await scheduler.terminate_job(job_id)
                match result:
                    case ActionStatus.OK:
                        return ResponseModel(status='ok')
                    case ActionStatus.NOT_FOUND:
                        response.status_code = status.HTTP_404_NOT_FOUND
                        return ResponseModel(status='error')
            case _:
                response.status_code = status.HTTP_400_BAD_REQUEST
                return ResponseModel(status='error')

    @app.get('/jobs/{job_id}', responses={
        status.HTTP_200_OK: {"model": Job},
        status.HTTP_404_NOT_FOUND: {"model": ResponseModel}
    })
    async def get_job(job_id: Id, response: Response):
        job = await scheduler.get_job(job_id)
        if job:
            return job
        else:
            response.status_code = status.HTTP_404_NOT_FOUND
            return ResponseModel(status='error')

    @app.get('/nodes')
    async def get_nodes() -> List[Node]:
        return await scheduler.get_nodes()

    @app.get('/nodes/{node_id}', responses={
        status.HTTP_200_OK: {"model": Node},
        status.HTTP_404_NOT_FOUND: {"model": ResponseModel}
    })
    async def get_node(node_id: Id, response: Response):
        node = await scheduler.get_node(node_id)
        if node:
            return node
        else:
            response.status_code = status.HTTP_404_NOT_FOUND
            return ResponseModel(status='error')

    @app.get('/nodes/{node_id}/jobs', responses={
        status.HTTP_200_OK: {"model": List[Job]},
        status.HTTP_404_NOT_FOUND: {"model": ResponseModel}
    })
    async def get_node_jobs(node_id: Id, response: Response):
        jobs = await scheduler.get_node_jobs(node_id)
        if type(jobs) is list:
            return jobs
        else:
            response.status_code = status.HTTP_404_NOT_FOUND
            return ResponseModel(status='error')

    @app.post('/nodes', status_code=status.HTTP_201_CREATED)
    async def add_node(new_node: NewNode) -> CreateResponseModel:
        id = await scheduler.add_node(new_node)
        return CreateResponseModel(status='ok', id=id)

    @app.delete('/nodes/{node_id}')
    async def delete_node(node_id: Id, response: Response) -> ResponseModel:
        result = await scheduler.delete_node(node_id)
        match result:
            case ActionStatus.OK:
                return ResponseModel(status='ok')
            case ActionStatus.NOT_FOUND:
                response.status_code = status.HTTP_404_NOT_FOUND
                return ResponseModel(status='error')


def setup_logger():
    logger = logging.getLogger("scheduler")
    logger.setLevel(logging.INFO)
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
    logger.addHandler(stream_handler)


@click.command()
@click.option('--host', default='127.0.0.1', help='host to start webserver on')
@click.option('--port', default=8080, help='webserver port to start on')
@click.option('--storage', default='memory', type=click.Choice(StorageType))
def run(host, port, storage):
    setup_logger()
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    scheduler = Scheduler(storage_type=storage)
    loop.create_task(scheduler.run())

    app = FastAPI(root_path='/api/v1')
    register_urls(app, scheduler)

    config = uvicorn.Config(app, host=host, port=port, loop='asyncio')
    server = uvicorn.Server(config)
    loop.run_until_complete(server.serve())


if __name__ == '__main__':
    run()

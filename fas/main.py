import logging

from fastapi import FastAPI, Depends
from starlette.requests import Request

from .database import *
from .model import list_organizations

logger = logging.getLogger(__name__)

app = FastAPI()


@app.on_event('startup')
async def open_database_connection_pool_():
    await open_database_connection_pool()


@app.on_event('shutdown')
async def close_database_connection_pool_():
    await close_database_connection_pool()


async def db_connection(request: Request):
    conn = getattr(request.state, 'db_conn', None)
    if not conn:
        conn = request.state.db_conn = await acquire_database_connection(timeout=3)
    return conn


@app.middleware('http')
async def release_db_connection(request: Request, call_next):
    try:
        response = await call_next(request)
    finally:
        conn = getattr(request.state, 'db_conn', None)
        if conn:
            try:
                await release_database_connection(conn, timeout=1)
            finally:
                logger.exception(f'cannot close database connection: {conn}')
    return response


@app.get("/")
def read_root():
    return {"Hello": "World"}


@app.get("/organizations")
async def list_organizations_(db_conn: Connection = Depends(db_connection)):
    organizations = await list_organizations(db_conn)
    logging.info(f'{organizations}')
    return organizations

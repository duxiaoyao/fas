import logging

from dynaconf import settings
from fastapi import FastAPI
from starlette.requests import Request

from fas.util.database import open_database_connection_pool, close_database_connection_pool, release_database_connection
from . import organization

LOGGER = logging.getLogger(__name__)

app = FastAPI(debug=settings.DEBUG)


@app.on_event('startup')
async def open_database_connection_pool_():
    await open_database_connection_pool(**settings.DB)


@app.on_event('shutdown')
async def close_database_connection_pool_():
    await close_database_connection_pool()


@app.middleware('http')
async def release_db_connection(request: Request, call_next):
    request.state.db = None
    try:
        return await call_next(request)
    finally:
        conn = request.state.db
        if conn:
            try:
                await release_database_connection(conn)
            except Exception:
                LOGGER.exception(f'cannot close database connection: {conn}')


@app.get('/')
def read_root():
    return {"Hello": "World"}


app.include_router(organization.router, prefix='/organizations', tags=['organizations'])

from dynaconf import settings
from fastapi import FastAPI
from starlette.requests import Request

from . import organization
from .db import open_db_connection_pool, close_db_connection_pool, release_db

app = FastAPI(debug=settings.DEBUG)


@app.on_event('startup')
async def open_database_connection_pool():
    await open_db_connection_pool()


@app.on_event('shutdown')
async def close_database_connection_pool():
    await close_db_connection_pool()


@app.middleware('http')
async def release_database_connection(request: Request, call_next):
    request.state.db = None
    try:
        return await call_next(request)
    finally:
        if request.state.db:
            await release_db(request.state.db)


@app.get('/')
def read_root():
    return {'Hello': 'World'}


app.include_router(organization.router, prefix='/organizations', tags=['organizations'])

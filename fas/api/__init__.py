import logging

from dynaconf import settings
from fastapi import FastAPI
from starlette.requests import Request

from fas.util.database import DBPool
from . import organization, operator

LOGGER = logging.getLogger(__name__)

_pool: DBPool = DBPool(**settings.DB)

app = FastAPI(debug=True)


@app.on_event('startup')
async def open_database_connection_pool():
    LOGGER.info(f'Current ENV: {settings.ENV_FOR_DYNACONF}')
    await _pool.open()


@app.on_event('shutdown')
async def close_database_connection_pool():
    await _pool.close()


@app.middleware('http')
async def inject_database_connection_to_request(request: Request, call_next):
    try:
        request.state.db = _pool.acquire(acquire_timeout=3, release_timeout=3)
        return await call_next(request)
    finally:
        if request.state.db:
            try:
                await request.state.db.release()
            except Exception:
                pass


@app.middleware('http')
async def add_custom_headers(request: Request, call_next):
    response = await call_next(request)
    response.headers['X-Content-Type-Options'] = 'nosniff'
    return response


@app.get('/')
def read_root():
    return {'Hello': 'World'}


app.include_router(operator.router, prefix='/operators', tags=['operators'])
app.include_router(organization.router, prefix='/organizations', dependencies=[operator.require_auth(is_admin=True)],
                   tags=['organizations'])

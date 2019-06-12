from dynaconf import settings
from starlette.requests import Request

from fas.util.database import DBPool, DBClient

_pool: DBPool = DBPool(**settings.DB)


async def open_db_connection_pool() -> None:
    await _pool.open()


async def close_db_connection_pool() -> None:
    await _pool.close()


async def acquire_db(request: Request) -> DBClient:
    if not request.state.db:
        request.state.db = await _pool.acquire(acquire_timeout=3, release_timeout=3)
    return request.state.db


async def release_db(db: DBClient):
    try:
        await db.release()
    except:
        pass

import asyncio

import pytest
from dynaconf import settings

from fas.util.database import DBPool, DBClient


@pytest.fixture(scope='session')
def event_loop():
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


@pytest.mark.asyncio
@pytest.fixture(scope='session')
async def db() -> DBClient:
    async with DBPool(**settings.DB) as pool:
        async with pool.acquire() as db:
            yield db


@pytest.mark.asyncio
@pytest.fixture(autouse=True)
async def rollback_db(db: DBClient):
    tr = await db.transaction()
    await tr.start()
    try:
        yield
    finally:
        await tr.rollback()

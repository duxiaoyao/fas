import asyncio
import subprocess

import pytest
from dynaconf import settings

from fas.environment import ENV
from fas.util.database import DBPool, DBClient


@pytest.fixture(scope='session')
def event_loop():
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


@pytest.fixture(scope='session', autouse=True)
def migrate_database():
    subprocess.run(['invoke', 'db.migrate'], cwd=ENV.root_dir)
    yield


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

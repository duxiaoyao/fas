import pytest
from dynaconf import settings

from fas.util.database import DBPool, DBClient


@pytest.mark.asyncio
async def test_pool_and_db_async_with():
    async with DBPool(**settings.DB) as pool:
        async with pool.acquire() as db:
            assert 1 == await db.get_scalar('SELECT 1::INT')


@pytest.mark.asyncio
async def test_pool_await():
    pool = DBPool(**settings.DB)
    await pool.open()
    try:
        async with pool.acquire() as db:
            assert 1 == await db.get_scalar('SELECT 1::INT')
    finally:
        await pool.close()


@pytest.mark.asyncio
async def test_db_await():
    async with DBPool(**settings.DB) as pool:
        db = pool.acquire()
        try:
            assert 1 == await db.get_scalar('SELECT 1::INT')
        finally:
            await db.release()


@pytest.mark.asyncio
async def test_db_constructor():
    async with DBPool(**settings.DB) as pool:
        async with DBClient(pool) as db:
            assert 1 == await db.get_scalar('SELECT 1::INT')

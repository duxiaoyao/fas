import pytest
from dynaconf import settings

from fas.util.database import DBPool, DBClient, transactional


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


@pytest.mark.asyncio
async def test_transaction():
    name = 'ORG#1'
    new_name = 'NEW-ORG#1'

    @transactional
    async def func(db_: DBClient, *, raise_exception: bool = False) -> None:
        await db_.insert('organization', conflict_target='(name)', conflict_action='DO NOTHING', name=name)
        if raise_exception:
            raise Exception
        await db_.execute('UPDATE organization SET name=:new_name WHERE name=:name', name=name, new_name=new_name)

    async with DBPool(**settings.DB) as pool:
        async with pool.acquire() as db:
            await func(db)
            assert not await db.exists('SELECT name FROM organization WHERE name=:name', name=name)
            assert await db.exists('SELECT name FROM organization WHERE name=:name', name=new_name)
            await db.execute('DELETE FROM organization WHERE name=:name', name=new_name)

            with pytest.raises(Exception):
                await func(db, raise_exception=True)
            assert not await db.exists('SELECT name FROM organization WHERE name=:name', name=name)
            assert not await db.exists('SELECT name FROM organization WHERE name=:name', name=new_name)

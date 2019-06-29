import pytest
from dynaconf import settings

from fas.util.database import DBPool, DBClient, transactional


@pytest.mark.asyncio
async def test_transaction_async_with(db: DBClient):
    async with await db.transaction():
        await _func(db)
    await verify_committed(db)

    with pytest.raises(Exception):
        async with await db.transaction():
            await _func(db, raise_exception=True)
    await verify_rolled_back(db)


@pytest.mark.asyncio
async def test_transaction_await(db: DBClient):
    tr = await db.transaction()

    await tr.start()
    try:
        await _func(db)
    except Exception:
        await tr.rollback()
        raise
    else:
        await tr.commit()
    await verify_committed(db)

    with pytest.raises(Exception):
        await tr.start()
        try:
            await _func(db, raise_exception=True)
        except Exception:
            await tr.rollback()
            raise
        else:
            await tr.commit()
    await verify_rolled_back(db)


@pytest.mark.asyncio
async def test_transactional_decorator():
    @transactional
    async def func(db_: DBClient, *, raise_exception: bool = False) -> None:
        await _func(db_, raise_exception=raise_exception)

    async with DBPool(**settings.DB) as pool:
        async with pool.acquire() as db:
            await func(db)
            await verify_committed(db)
            with pytest.raises(Exception):
                await func(db, raise_exception=True)
            await verify_rolled_back(db)


name = 'ORG#1'
new_name = 'NEW-ORG#1'


async def _func(db_: DBClient, *, raise_exception: bool = False) -> None:
    await db_.insert('organization', conflict_target='(name)', conflict_action='DO NOTHING', name=name)
    if raise_exception:
        raise Exception
    await db_.execute('UPDATE organization SET name=:new_name WHERE name=:name', name=name, new_name=new_name)


async def verify_committed(db):
    assert not await db.exists('SELECT name FROM organization WHERE name=:name', name=name)
    assert await db.exists('SELECT name FROM organization WHERE name=:name', name=new_name)
    await db.execute('DELETE FROM organization WHERE name=:name', name=new_name)


async def verify_rolled_back(db):
    assert not await db.exists('SELECT name FROM organization WHERE name=:name', name=name)
    assert not await db.exists('SELECT name FROM organization WHERE name=:name', name=new_name)

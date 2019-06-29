import pytest

from fas.model.organization import Organization
from fas.util.database import DBClient


@pytest.mark.asyncio
@pytest.fixture(autouse=True)
async def rollback_db(db: DBClient):
    tr = await db.transaction()
    await tr.start()
    try:
        yield
    finally:
        await tr.rollback()


@pytest.mark.asyncio
@pytest.fixture(autouse=True)
async def test_data(db: DBClient):
    await db.list("INSERT INTO organization (name) VALUES ('Org#1'), ('Org#2')")


@pytest.mark.asyncio
async def test_execute(db: DBClient):
    name = 'Org#2'
    new_name = 'NewOrg#2'
    assert 1 == await db.execute('UPDATE organization SET name=:new_name WHERE name=:name', name=name,
                                 new_name=new_name)
    assert await db.get_scalar('SELECT name FROM organization WHERE name=:name', name=new_name)


@pytest.mark.asyncio
async def test_executemany(db: DBClient):
    name1 = 'Org#1'
    new_name1 = 'NewOrg#1'
    name2 = 'Org#2'
    new_name2 = 'NewOrg#2'
    await db.executemany('UPDATE organization SET name=:new_name WHERE name=:name',
                         [dict(name=name1, new_name=new_name1), dict(name=name2, new_name=new_name2)])
    assert await db.get_scalar('SELECT name FROM organization WHERE name=:name', name=new_name1)
    assert await db.get_scalar('SELECT name FROM organization WHERE name=:name', name=new_name2)


@pytest.mark.asyncio
async def test_exists(db: DBClient):
    assert await db.exists('SELECT name FROM organization WHERE name=:name', name='Org#2') is True
    assert await db.exists('SELECT name FROM organization WHERE name=:name', name='not exist') is False


@pytest.mark.asyncio
async def test_list(db: DBClient):
    name1 = 'Org#1'
    name2 = 'Org#2'
    organizations = await db.list('SELECT * FROM organization WHERE name=:name', to_cls=Organization, name=name1)
    assert 1 == len(organizations)
    assert name1 == organizations[0].name
    organizations = await db.list('SELECT * FROM organization WHERE name LIKE :name_pattern ORDER BY name',
                                  to_cls=Organization, name_pattern='Org%')
    assert [name1, name2] == [org.name for org in organizations]
    organizations = await db.list('SELECT * FROM organization WHERE name=ANY(:names::TEXT[]) ORDER BY name',
                                  to_cls=Organization, names=(name1, name2))
    assert [name1, name2] == [org.name for org in organizations]


@pytest.mark.asyncio
async def test_list_scalar(db: DBClient):
    name = 'Org#2'
    names = await db.list_scalar('SELECT name FROM organization WHERE name=:name', name=name)
    assert 1 == len(names)
    assert name == names[0]


@pytest.mark.asyncio
async def test_get(db: DBClient):
    name = 'Org#2'
    organization = await db.get('SELECT * FROM organization WHERE name=:name', to_cls=Organization, name=name)
    assert name == organization.name


@pytest.mark.asyncio
async def test_get_scalar(db: DBClient):
    name = 'Org#2'
    assert name == await db.get_scalar('SELECT name FROM organization WHERE name=:name', name=name)


@pytest.mark.asyncio
async def test_insert(db: DBClient):
    prefix = 'Org#'
    id = 0
    id = 1 + await db.insert('organization', return_id=True, name=f'{prefix}{id}')
    assert f'{prefix}{id}' == await db.insert('organization', return_id=('name',), name=f'{prefix}{id}')
    id += 1
    name = f'{prefix}{id}'
    org = Organization(name=f'{prefix}{id}')
    assert name == await db.insert('organization', return_id=('name',), **org)
    id += 1
    org = await db.insert('organization', return_record=True, to_cls=Organization, id=id, name=f'{prefix}{id}')
    assert id == org.id
    assert f'{prefix}{id}' == org.name
    id += 1
    org = Organization(id=id, name=f'{prefix}{id}')
    assert org == await db.insert('organization', return_record=True, to_cls=Organization, **org)


@pytest.mark.asyncio
async def test_insert_objects(db: DBClient):
    name3 = 'Org#3'
    name4 = 'Org#4'
    assert [name3, name4] == await db.insert('organization',
                                             [Organization(id=3, name=name3), Organization(id=4, name=name4)],
                                             return_id=('name',), include_attrs=('name',))
    name5 = 'Org#5'
    assert [name5] == await db.insert('organization', [Organization(id=5, name=name5)], return_id=('name',),
                                      exclude_attrs=('id',))
    name6 = 'Org#6'
    assert [name6] == await db.insert('organization', [Organization(name=name6)], return_id=('name',))


@pytest.mark.asyncio
async def test_iter(db: DBClient):
    i: int = 1
    async for org in db.iter('SELECT * FROM organization WHERE name LIKE :name_pattern ORDER BY name',
                             to_cls=Organization, name_pattern='Org%'):
        assert f'Org#{i}' == org.name
        i += 1


@pytest.mark.asyncio
async def test_iter_scalar(db: DBClient):
    i: int = 1
    async for name in db.iter_scalar('SELECT name FROM organization WHERE name LIKE :name_pattern ORDER BY name',
                                     name_pattern='Org%'):
        assert f'Org#{i}' == name
        i += 1


@pytest.mark.asyncio
async def test_json(db: DBClient):
    data = {'a': 'ab', 'b': 1}
    assert data == await db.get_scalar('SELECT :data::JSON', data=data)
    assert data == await db.get_scalar('SELECT :data::JSONB', data=data)

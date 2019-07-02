from typing import List

from fas.util.database import DBClient
from fas.util.model import Entity

__all__ = ['Organization', 'list_organizations', 'create_organization', 'get_organization', 'update_organization',
           'delete_organization']


class Organization(Entity):
    id: int = 0
    name: str


async def list_organizations(db: DBClient) -> List[Organization]:
    return await db.list('SELECT * FROM organization', to_cls=Organization)


async def create_organization(db: DBClient, name: str) -> int:
    return await db.insert('organization', return_id=True, name=name)


async def get_organization(db: DBClient, id: int) -> Organization:
    return await db.get('SELECT * FROM organization WHERE id=:id', to_cls=Organization, id=id)


async def update_organization(db: DBClient, id: int, name: str) -> int:
    return await db.execute('UPDATE organization SET name=:name WHERE id=:id', id=id, name=name)


async def delete_organization(db: DBClient, id: int) -> int:
    return await db.execute('DELETE FROM organization WHERE id=:id', id=id)

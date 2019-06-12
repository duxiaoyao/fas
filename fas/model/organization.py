from typing import List

from pydantic import BaseModel

from fas.util.database import DBClient


class Organization(BaseModel):
    id: int
    name: str


async def list_organizations(db: DBClient) -> List[Organization]:
    return await db.list('SELECT * FROM organization', to_cls=Organization)


async def create_organization(db: DBClient, name: str) -> Organization:
    return await db.insert('organization', name=name, returns_record=True, to_cls=Organization)


async def get_organization(db: DBClient, id: int) -> Organization:
    return await db.get('SELECT * FROM organization WHERE id=:id', id=id)


async def update_organization(db: DBClient, id: int, name: str) -> int:
    return await db.execute('UPDATE organization SET name=:name WHERE id=:id', id=id, name=name)


async def delete_organization(db: DBClient, id: int) -> int:
    return await db.execute('DELETE FROM organization WHERE id=:id', id=id)

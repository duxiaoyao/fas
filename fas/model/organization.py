from typing import List

from fas.util.database import DBClient, transactional
from fas.util.model import Entity
from fas.util.web import hash_password
from .operator import Operator, create_operator

__all__ = ['Organization', 'list_organizations', 'create_organization', 'get_organization', 'update_organization',
           'delete_organization', 'add_organization']


class Organization(Entity):
    id: int = 0
    name: str


async def list_organizations(db: DBClient) -> List[Organization]:
    return await db.list('SELECT * FROM organization', to_cls=Organization)


async def create_organization(db: DBClient, name: str) -> Organization:
    return await db.insert('organization', return_record=True, to_cls=Organization, name=name)


async def get_organization(db: DBClient, id: int) -> Organization:
    return await db.get('SELECT * FROM organization WHERE id=:id', to_cls=Organization, id=id)


async def update_organization(db: DBClient, id: int, name: str) -> int:
    return await db.execute('UPDATE organization SET name=:name WHERE id=:id', id=id, name=name)


async def delete_organization(db: DBClient, id: int) -> int:
    return await db.execute('DELETE FROM organization WHERE id=:id', id=id)


@transactional
async def add_organization(db: DBClient, name: str, admin_name: str, admin_mobile: str, admin_password: str):
    org = await create_organization(db, name)
    password_hash = hash_password(admin_password)
    admin = Operator(organization_id=org.id, name=admin_name, mobile=admin_mobile, password_hash=password_hash,
                     is_admin=True, active=True)
    admin = await create_operator(db, admin)
    return org, admin

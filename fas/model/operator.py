from fas.util.database import DBClient
from fas.util.model import Entity

__all__ = ['Operator', 'create_operator', 'get_operator_by_mobile', 'get_operator_by_id']


class Operator(Entity):
    id: int = 0
    organization_id: int
    name: str
    mobile: str
    password_hash: str
    is_admin: bool
    active: bool


async def create_operator(db: DBClient, operator: Operator) -> Operator:
    return await db.insert('operator', return_record=True, to_cls=Operator, **operator)


async def get_operator_by_mobile(db: DBClient, organization_id: int, mobile: str) -> Operator:
    return await db.get('''
        SELECT * FROM operator WHERE organization_id=:organization_id AND mobile=:mobile ORDER BY active DESC LIMIT 1
        ''', to_cls=Operator, organization_id=organization_id, mobile=mobile)


async def get_operator_by_id(db: DBClient, id: int) -> Operator:
    return await db.get('SELECT * FROM operator WHERE id=:id', to_cls=Operator, id=id)

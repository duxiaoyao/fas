from typing import List

from pydantic import BaseModel


class Organization(BaseModel):
    id: int
    name: str


async def list_organizations(db) -> List[Organization]:
    return await db.list('SELECT * FROM organization', to_cls=Organization)

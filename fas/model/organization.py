from pydantic import BaseModel


class Organization(BaseModel):
    id: int
    name: str


async def list_organizations(db):
    rows = await db.fetch('SELECT * FROM organization')
    return [Organization(**row) for row in rows]

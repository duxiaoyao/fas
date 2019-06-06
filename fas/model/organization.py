from pydantic import BaseModel


class Organization(BaseModel):
    id: int
    name: str


async def list_organizations(db_conn):
    return await db_conn.fetch('SELECT * FROM organization')

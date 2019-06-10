from starlette.requests import Request

from fas.util.database import Connection, acquire_database_connection


async def get_db(request: Request) -> Connection:
    if not request.state.db:
        request.state.db = await acquire_database_connection()
    return request.state.db

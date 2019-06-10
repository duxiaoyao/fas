from starlette.requests import Request

from fas.util.database import acquire_database_connection


async def get_db(request: Request):
    if not request.state.db:
        request.state.db = await acquire_database_connection()
    return request.state.db

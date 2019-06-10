from starlette.requests import Request

from fas.database import acquire_database_connection


async def db_connection(request: Request):
    if not request.state.db_conn:
        request.state.db_conn = await acquire_database_connection()
    return request.state.db_conn

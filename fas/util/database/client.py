import logging

from asyncpg import Connection

LOGGER = logging.getLogger(__name__)


class Database:
    def __init__(self, conn: Connection):
        self.conn = conn

    def list(self, sql, **kwargs):
        return self._query(sql, **kwargs)

    def list_scalar(self, sql, **kwargs):
        rows = self._query(sql, returns_dict_object=False, **kwargs)
        if rows and len(rows[0]) > 1:
            raise Exception('More than one columns returned: sql is %(sql)s and kwargs are %(kwargs)s', {
                'sql': sql,
                'kwargs': kwargs
            })
        return [row[0] for row in rows]

    def get(self, sql, **kwargs):
        rows = self._query(sql, **kwargs)
        if not rows:
            LOGGER.debug('No rows returned: sql is %(sql)s and kwargs are %(kwargs)s', {
                'sql': sql,
                'kwargs': kwargs
            })
            return None
        if len(rows) > 1:
            LOGGER.warning('More than one rows returned: sql is %(sql)s and kwargs are %(kwargs)s', {
                'sql': sql,
                'kwargs': kwargs
            })
        return rows[0]

    def get_scalar(self, sql, **kwargs):
        rows = self._query(sql, returns_dict_object=False, **kwargs)
        if not rows:
            LOGGER.debug('No rows returned: sql is %(sql)s and kwargs are %(kwargs)s', {
                'sql': sql,
                'kwargs': kwargs
            })
            return None
        if len(rows) > 1:
            LOGGER.warning('More than one rows returned: sql is %(sql)s and kwargs are %(kwargs)s', {
                'sql': sql,
                'kwargs': kwargs
            })
        if len(rows[0]) > 1:
            raise Exception('More than one columns returned: sql is %(sql)s and kwargs are %(kwargs)s', {
                'sql': sql,
                'kwargs': kwargs
            })
        return rows[0][0]

    def _query(self, sql, **kwargs):
        return self.conn.fetch(sql, **kwargs)


if __name__ == '__main__':
    from dynaconf import settings
    from fas.util.database.pool import *


    async def main():
        try:
            await open_database_connection_pool(**settings.DB)
            conn = await acquire_database_connection()
            try:
                organizations = await conn.fetch('SELECT * FROM organization')
            finally:
                await release_database_connection(conn)
        finally:
            await close_database_connection_pool()
        print(organizations)


    asyncio.run(main())

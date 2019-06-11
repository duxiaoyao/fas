from __future__ import annotations

import asyncio
import logging
from types import TracebackType
from typing import Any, Dict, Type, Optional, Generator

import asyncpg

LOGGER = logging.getLogger(__name__)


class DBPool:
    def __init__(self, dsn: str = None, *, close_timeout: float = None, min_size: int = 10, max_size: int = 10,
                 setup: Any = None, init: Any = None, **connect_kwargs: Any) -> None:
        self._options: Dict = dict(dsn=dsn, min_size=min_size, max_size=max_size, setup=setup, init=init,
                                   **connect_kwargs)
        self._close_timeout: float = close_timeout
        self._pool: Optional[asyncpg.pool.Pool] = None

    @property
    def is_open(self) -> bool:
        return self._pool is not None

    async def open(self) -> None:
        assert self._pool is None, 'Connection pool is already opened'
        try:
            self._pool = await asyncpg.create_pool(**self._options)
        except Exception:
            LOGGER.critical(f'Cannot open connection pool: options={self._options}', exc_info=True)
            raise
        else:
            LOGGER.debug(f'Opened connection pool: options={self._options}, pool={self._pool}')

    async def close(self) -> None:
        assert self._pool is not None, 'Connection pool is not opened'
        try:
            if self._close_timeout:
                await asyncio.wait_for(self._pool.close(), timeout=self._close_timeout)
            else:
                await self._pool.close()
        except Exception:
            LOGGER.exception(
                f'Cannot close connection pool: pool={self._pool}, close_timeout={self._close_timeout}')
            raise
        else:
            LOGGER.debug(f'Closed connection pool: pool={self._pool}, close_timeout={self._close_timeout}')
        finally:
            self._pool = None

    async def __aenter__(self) -> DBPool:
        await self.open()
        return self

    async def __aexit__(self, exc_type: Type[BaseException] = None, exc_value: BaseException = None,
                        traceback: TracebackType = None) -> None:
        await self.close()

    def acquire(self, acquire_timeout: float = None, release_timeout: float = None) -> DBConnection:
        """Acquire a database connection from the pool.

        :param float acquire_timeout: A timeout for acquiring a Connection.
        :param float release_timeout: A timeout for releasing a Connection.
        :return: An instance of :class:`~DBConnection`.

        Can be used in an ``await`` expression or with an ``async with`` block.

        .. code-block:: python

            async with pool.acquire() as con:
                await con.execute(...)

        Or:

        .. code-block:: python

            con = await pool.acquire()
            try:
                await con.execute(...)
            finally:
                await pool.release(con)
        """
        return DBConnection(self, acquire_timeout=acquire_timeout, release_timeout=release_timeout)

    async def _acquire(self, *, timeout: float = None) -> asyncpg.Connection:
        assert self._pool is not None, 'Connection pool is not opened'
        try:
            conn = await self._pool.acquire(timeout=timeout)
        except Exception:
            LOGGER.critical(f'Cannot acquire connection: pool={self._pool}, acquire_timeout={timeout}', exc_info=True)
            raise
        else:
            LOGGER.debug(f'Acquired connection: conn={conn}, pool={self._pool}, acquire_timeout={timeout}')
            return conn

    async def _release(self, conn: asyncpg.Connection, *, timeout: float = None) -> None:
        assert self._pool is not None, 'Connection pool is not opened'
        try:
            await self._pool.release(conn, timeout=timeout)
        except Exception:
            LOGGER.exception(f'Cannot release connection: conn={conn}, pool={self._pool}, release_timeout={timeout}')
            raise
        else:
            LOGGER.debug(f'Released connection: conn={conn}, pool={self._pool}, release_timeout={timeout}')


class DBConnection:
    def __init__(self, pool: DBPool, acquire_timeout: float = None, release_timeout: float = None) -> None:
        self._pool: DBPool = pool
        self._acquire_timeout: float = acquire_timeout
        self._release_timeout: float = release_timeout
        self._conn: Optional[asyncpg.Connection] = None

    async def list(self, sql, to_cls=None, **kwargs):
        rows = await self._query(sql, **kwargs)
        return [to_cls(**row) for row in rows] if to_cls else rows

    async def _query(self, sql, **kwargs):
        return await self._conn.fetch(sql, **kwargs)

    @property
    def is_connected(self) -> bool:
        return self._conn is not None

    async def acquire(self) -> DBConnection:
        assert self._conn is None, 'Connection is already acquired'
        self._conn = await self._pool._acquire(timeout=self._acquire_timeout)
        return self

    async def release(self) -> None:
        assert self._conn is not None, 'Connection is not acquired'
        try:
            await self._pool._release(self._conn, timeout=self._release_timeout)
        finally:
            self._conn = None

    async def __aenter__(self) -> DBConnection:
        """
        Called when entering `async with db_pool.acquire()`
        """
        await self.acquire()
        return self

    async def __aexit__(self, exc_type: Type[BaseException] = None, exc_value: BaseException = None,
                        traceback: TracebackType = None) -> None:
        """
        Called when exiting `async with db_pool.acquire()`
        """
        await self.release()

    def __await__(self) -> Generator:
        """
        Called if using `db_connection = await db_pool.acquire()`
        """
        return self.acquire().__await__()


if __name__ == '__main__':
    async def main():
        from dynaconf import settings

        pool = DBPool(**settings.DB)
        await pool.open()
        try:
            async with DBConnection(pool) as conn:
                print(await conn.list('SELECT 1111 FROM organization'))

            conn = await pool.acquire()
            try:
                print(await conn.list('SELECT 1112 FROM organization'))
            finally:
                await conn.release()

            async with pool.acquire() as conn:
                print(await conn.list('SELECT 1113 FROM organization'))
        finally:
            await pool.close()

        async with DBPool(**settings.DB) as pool:
            async with DBConnection(pool) as conn:
                print(await conn.list('SELECT 2211 FROM organization'))

            conn = await pool.acquire()
            try:
                print(await conn.list('SELECT 2212 FROM organization'))
            finally:
                await conn.release()

            async with pool.acquire() as conn:
                print(await conn.list('SELECT 2213 FROM organization'))


    asyncio.run(main())

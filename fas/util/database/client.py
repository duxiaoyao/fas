from __future__ import annotations

import asyncio
import functools
import logging
from types import TracebackType
from typing import Any, Dict, Type, Optional, Union, Callable

import asyncpg

from .interface import DBInterface

LOGGER = logging.getLogger(__name__)


class DBPool:
    __slots__ = ('_options', '_close_timeout', '_pool')

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

    def acquire(self, acquire_timeout: float = None, release_timeout: float = None) -> DBClient:
        """Acquire a database connection from the pool.

        :param float acquire_timeout: A timeout for acquiring a Connection.
        :param float release_timeout: A timeout for releasing a Connection.
        :return: An instance of :class:`~DBClient`.

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
        return DBClient(self, acquire_timeout=acquire_timeout, release_timeout=release_timeout)

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


class DBClient(DBInterface):
    __slots__ = ('_pool', '_acquire_timeout', '_release_timeout', '_conn')

    def __init__(self, pool: DBPool, acquire_timeout: float = None, release_timeout: float = None) -> None:
        self._pool: DBPool = pool
        self._acquire_timeout: float = acquire_timeout
        self._release_timeout: float = release_timeout
        self._conn: Optional[asyncpg.Connection] = None

    @property
    def conn(self) -> asyncpg.Connection:
        return self._conn

    @property
    def is_connected(self) -> bool:
        return self._conn is not None

    async def acquire(self) -> DBClient:
        assert self._conn is None, 'Connection is already acquired'
        self._conn = await self._pool._acquire(timeout=self._acquire_timeout)
        return self

    async def release(self) -> None:
        assert self._conn is not None, 'Connection is not acquired'
        try:
            await self._pool._release(self._conn, timeout=self._release_timeout)
        finally:
            self._conn = None

    async def __aenter__(self) -> DBClient:
        """
        Called when entering `async with db_pool.acquire()`
        """
        return self  # acquire connection lazily, check `_acquire_if_not_connected`

    async def __aexit__(self, exc_type: Type[BaseException] = None, exc_value: BaseException = None,
                        traceback: TracebackType = None) -> None:
        """
        Called when exiting `async with db_pool.acquire()`
        """
        if self.is_connected:
            await self.release()


def transactional(isolation: Union[str, Callable] = 'read_committed', readonly: bool = False, deferrable: bool = False):
    if callable(isolation):
        func = isolation
        return TransactionDecorator()(func)
    else:
        return TransactionDecorator(isolation=isolation, readonly=readonly, deferrable=deferrable)


class TransactionDecorator:
    __slots__ = ('isolation', 'readonly', 'deferrable')

    def __init__(self, *, isolation: str = 'read_committed', readonly: bool = False, deferrable: bool = False):
        self.isolation: str = isolation
        self.readonly: bool = readonly
        self.deferrable: bool = deferrable

    def __call__(self, func: Callable) -> Callable:
        @functools.wraps(func)
        async def wrapper(db: DBClient, *args: Any, **kwargs: Any) -> Any:
            if db.is_in_transaction:
                return await func(db, *args, **kwargs)
            else:
                await db._acquire_if_necessary()
                async with db._transaction_after_connected(isolation=self.isolation, readonly=self.readonly,
                                                           deferrable=self.deferrable):
                    return await func(db, *args, **kwargs)

        return wrapper

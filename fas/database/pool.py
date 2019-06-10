import asyncio
import logging

from asyncpg import create_pool, Connection
from asyncpg.pool import Pool
from dynaconf import settings

LOGGER = logging.getLogger(__name__)

_conn_pool = None


async def open_database_connection_pool() -> Pool:
    global _conn_pool

    if not _conn_pool:
        try:
            _conn_pool = await create_pool(timeout=1, **settings.DB_CONN)
        except Exception:
            LOGGER.exception(f'cannot open database connection pool: config={settings.DB_CONN}')
            raise
        else:
            LOGGER.debug(f'opened database connection pool: config={settings.DB_CONN}, pool={_conn_pool}')
    return _conn_pool


async def close_database_connection_pool():
    global _conn_pool

    if not _conn_pool:
        return

    try:
        await asyncio.wait_for(_conn_pool.close(), timeout=7)
    except Exception:
        LOGGER.exception(f'cannot close database connection pool: pool={_conn_pool}')
        raise
    else:
        LOGGER.debug(f'closed database connection pool: pool={_conn_pool}')
    finally:
        _conn_pool = None


async def acquire_database_connection(*, timeout=1) -> Connection:
    try:
        conn = await _conn_pool.acquire(timeout=timeout)
    except Exception:
        LOGGER.exception(f'cannot acquire database connection from pool: pool={_conn_pool}')
        raise
    else:
        LOGGER.debug(f'acquired database connection from pool: conn={conn}, pool={_conn_pool}')
    return conn


async def release_database_connection(conn, *, timeout=3):
    try:
        await _conn_pool.release(conn, timeout=timeout)
    except Exception:
        LOGGER.exception(f'cannot release database connection back pool: conn={conn}, pool={_conn_pool}')
        raise
    else:
        LOGGER.debug(f'released database connection back pool: conn={conn}, pool={_conn_pool}')

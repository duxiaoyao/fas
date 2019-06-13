import asyncio

from dynaconf import settings

from fas.util.database.client import DBPool, DBClient

if __name__ == '__main__':
    print(f'Current ENV: {settings.ENV_FOR_DYNACONF}')


    async def main():

        pool = DBPool(**settings.DB)
        await pool.open()
        try:
            async with DBClient(pool) as conn:
                print(await conn.list('SELECT 1111, * FROM organization'))

            conn = await pool.acquire()
            try:
                print(await conn.list('SELECT 1112, * FROM organization'))
            finally:
                await conn.release()

            async with pool.acquire() as conn:
                print(await conn.list('SELECT 1113, * FROM organization'))
        finally:
            await pool.close()

        async with DBPool(**settings.DB) as pool:
            async with DBClient(pool) as conn:
                print(await conn.list('SELECT 2211, * FROM organization'))

            conn = await pool.acquire()
            try:
                print(await conn.list('SELECT 2212, * FROM organization'))
            finally:
                await conn.release()

            async with pool.acquire() as conn:
                print(await conn.list('SELECT 2213, * FROM organization'))
                print(await conn.list('SELECT 2213, * FROM organization WHERE id=:id', id=2))
                ret = await conn.execute('UPDATE organization SET name=:name WHERE id=:id', id=2, name='ttt')
                print(ret)


    asyncio.run(main())

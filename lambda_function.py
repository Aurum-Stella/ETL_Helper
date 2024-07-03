import sys
import time
import asyncio
from src.etl.extract import Extract
from src.utils.utils import get_periods_days, get_periods_month, WriteDataToFile
from src.config.connected import Settings
import asyncpg
import psycopg


class Properties:
    def __init__(self):
        if sys.platform == "win32":
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
        self._extract = None
        self.facade_pool = None
        self.dwh_pool = None
        self.mautic_pool = None
        self.semaphore = asyncio.Semaphore(10)
        self.periods = get_periods_month('2024-01-01', 1)
        self.placeholders = {'contract_numbers': [[1, 2, 3]]}

    @property
    def extract(self):
        if not self._extract:
            self._extract = Extract
        return self._extract

    @staticmethod
    async def get_pool(db_name):
        settings = Settings
        return await asyncpg.create_pool(dsn=settings(db_name).database_url,
                                         min_size=5,
                                         max_size=10)

    async def created_facade_pool(self):
        self.facade_pool = await self.get_pool('FACADE')

    async def created_dwh_pool(self):
        self.dwh_pool = await self.get_pool('DWH')

    async def created_mautic_pool(self):
        self.mautic_pool = await self.get_pool('MAUTIC')

    async def pool_close(self):
        pools = [self.facade_pool, self.dwh_pool, self.mautic_pool]
        for pool in pools:
            if pool:
                await pool.close()


class RunAsync(Properties):

    async def run(self):
        pass

    async def create_temp_table(self):
        temp_sql_files = ['create_temporary_table_.sql']
        tasks = [self.extract(sql_file=temp_sql_file, pool=self.dwh_pool).run_sql_query_async() for temp_sql_file
                 in temp_sql_files]
        await asyncio.gather(*tasks)
        drop_temp_table_query = "DROP TABLE IF EXISTS temporary_table;"

    async def main(self):

        try:
            await self.created_dwh_pool()
            await self.create_temp_table()
        finally:
            await self.pool_close()


class RunDefault(Properties):
    extract = Extract

    def run(self, connected):
        pass

    def main(self):
        settings = Settings

        connected = psycopg.connect(settings('FACADE').database_connect)
        self.run(connected)


if __name__ == '__main__':
    start = time.time()
    run_async = RunAsync()
    asyncio.run(run_async.main())
    print('Total time work async... - ', round(time.time() - start), 'seconds.')

    # run_default = RunDefault()
    # run_default.main()
    # print('Total time work default... - ', round(time.time() - start), 'seconds.')

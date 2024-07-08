import sys
import time
import asyncio
from src.utils.utils import get_periods_month, read_sql_file
from src.config.connected import Settings
import asyncpg
from contextlib import AsyncExitStack


class Properties:
    def __init__(self):
        if sys.platform == "win32":
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
        self.dwh_pool = None
        self.facade_pool = None
        self.mautic_pool = None
        self.periods = get_periods_month(1, 2)
        self.sql_files = {
            'create_temporary_table.sql': read_sql_file('create_temporary_table.sql'),
            'create_index_temporary.sql': read_sql_file('create_index_temporary.sql'),
        }
        self.placeholders = {'contract_numbers': [[1, 2, 3]]}
        self.from_csv_path = 'src/utils/from_csv/'


class RunAsync(Properties):

    async def run(self):
        pass

    async def main(self):
        settings = Settings

        async with AsyncExitStack() as stack:
            self.dwh_pool = await stack.enter_async_context(asyncpg.create_pool(dsn=settings('DWH').database_url))
            self.facade_pool = await stack.enter_async_context(asyncpg.create_pool(dsn=settings('FACADE').database_url))
            self.facade_pool = await stack.enter_async_context(asyncpg.create_pool(dsn=settings('MAUTIC').database_url))

            tasks = [self.run()]
            await asyncio.gather(*tasks)




if __name__ == '__main__':
    async def run():
        start = time.time()
        try:
            run_async = RunAsync()
            await asyncio.create_task(run_async.main())
        finally:
            print('Total time work async... - ', round(time.time() - start), 'seconds.')


    asyncio.run(run())

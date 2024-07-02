import time
from src.utils.utils import read_sql_file
import os


class Extract:

    def __init__(self, sql_file=None, placeholder=None, semaphore=None, pool=None, connected=None):
        self.sql_query = read_sql_file(sql_file)[0]
        self.name_base = read_sql_file(sql_file)[1]
        self.placeholder = placeholder
        self.semaphore = semaphore
        self.pool = pool
        self.connected = connected
        self.project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))

    def run_sql_query_default(self):
        start = time.time()
        result = self.connected.execute(self.sql_query, self.placeholder)
        print('Time query work... - ', round(time.time() - start), 'seconds.')
        if result.rowcount == 0:
            print("No results found.")
        else:
            rows = result.fetchall()
            return rows

    async def run_sql_query_async(self):
        start = time.time()
        rows = []
        async with self.pool.acquire() as aconn:
            if self.placeholder:
                results = await aconn.fetch(self.sql_query, *self.placeholder)
            else:
                results = await aconn.fetch(self.sql_query)
            for result in results:
                rows.extend(result)
        print('Time query work... - ', round(time.time() - start), 'seconds.')
        return rows

    async def run_sql_query_and_write_to_file(self, date, number=0):
        start = time.time()
        name_file = f'_file_{number}.csv'
        file_path = f'{self.project_root}/src/utils/csv/'
        if date:
            prefix_file = self.placeholder[0].strftime('%Y-%m-%d') + '_' + self.placeholder[1].strftime('%Y-%m-%d')
            name_file = f'{prefix_file}_file_{number}.csv'
        # try:
        async with self.pool.acquire() as aconn:
            await aconn.copy_from_query(self.sql_query,
                                        *self.placeholder,
                                        output=file_path + name_file,
                                        format='csv')

        print('Time query work... - ', round(time.time() - start), 'seconds.')

    async def write_records_to_table(self, table_to: str, records: list):
        start = time.time()
        # try:
        async with self.pool.acquire() as aconn:
            async with aconn.transaction():
                await aconn.copy_records_to_table(table_to,
                                                  records)

        print('Time query work... - ', round(time.time() - start), 'seconds.')

    async def write_records_to_table_from_file(self, table_to: str, source: str):
        start = time.time()
        # try:
        async with self.pool.acquire() as aconn:
            async with aconn.transaction():
                await aconn.copy_to_table(table_to,
                                          source)

        print('Time query work... - ', round(time.time() - start), 'seconds.')
import os
from datetime import datetime, date, timedelta
from dateutil.relativedelta import relativedelta
import csv
import openpyxl

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))


def read_sql_file(sql_file_name):
    # Функція для зчитування SQL файлів

    with open(f'{project_root}/src/etl/sql/{sql_file_name}', 'r', encoding='utf-8') as file:
        db_name = file.readline()[2:-1]
        sql_query = file.read()

        return sql_query, db_name


def get_periods_days(start_period, step_day, end_period=str(date.today())):
    if isinstance(start_period, int):
        start_period = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) - relativedelta(
            days=start_period)
    else:
        start_period = datetime.strptime(start_period, '%Y-%m-%d')
    end_period = datetime.strptime(end_period, '%Y-%m-%d')

    periods = []
    second_period = start_period
    while True:
        first_period = second_period.replace(hour=0, minute=0, second=0, microsecond=0)
        second_period = (first_period + timedelta(days=step_day)) - timedelta(microseconds=1)

        periods.append({'start_period': first_period,
                        'end_period': second_period})
        second_period += timedelta(days=1)

        if second_period.date() > end_period.date():
            periods[-1]['end_period'] = end_period + timedelta(days=1) - timedelta(microseconds=1)
            break

    [print(i) for i in periods]
    # Если нужно возвращать список кортежей
    list_of_periods = [tuple(d[key] for key in d) for d in periods]
    return list_of_periods


def get_periods_month(start_period, step_month, end_period=str(date.today())):
    if isinstance(start_period, int):
        start_period = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) - relativedelta(
            months=start_period)
    else:
        start_period = datetime.strptime(start_period, '%Y-%m-%d')
    print(start_period)
    end_period = datetime.strptime(end_period, '%Y-%m-%d')

    # print(start_period, end_period)
    periods = [{'start_period': start_period}]

    while periods[-1]['start_period'].replace(day=1) + relativedelta(months=step_month) <= end_period.replace(day=1):
        periods.append({'start_period': periods[-1]['start_period'].replace(day=1) +
                                        relativedelta(months=step_month)})
    for i in periods:
        i['end_period'] = i['start_period'].replace(day=1) + relativedelta(months=step_month) - timedelta(
            microseconds=1)
    periods[-1]['end_period'] = end_period + relativedelta(days=1) - timedelta(microseconds=1)

    [print(i) for i in periods]
    # Если нужно возвращать список кортежей
    list_of_periods = [tuple(d[key] for key in d) for d in periods]
    return list_of_periods


class WriteDataToFile:
    def __init__(self, data, parameters=None, name='name_file'):
        self.name = name
        self.data = data
        self.parameters = parameters
        self.num_rows = 0
        self.limit_row_in_file = 15000
        self.file_name = None

    def give_name_file(self, format_file):

        if self.parameters:
            name_file = f"{self.name}_{self.parameters[self.num_rows]['start_period'].strftime('%Y-%m-%d')}__{self.parameters[self.num_rows]['end_period'].strftime('%Y-%m-%d')}_({self.num_rows + 1}).{format_file}"
        else:
            name_file = f"{self.name}_({self.num_rows + 1}).{format_file}"
        return name_file

    def write_to_csv(self):
        def open_file(rows):
            self.file_name = self.give_name_file(format_file='csv')

            with open(f'{project_root}/src/utils/csv/{self.file_name}', 'a', newline='') as file:
                file_writer = csv.writer(file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
                [file_writer.writerow(row) for row in rows]

        for rows in self.data:
            open_file(rows)
            self.num_rows += 1

    def write_to_excel(self):
        def open_file(rows):
            self.file_name = self.give_name_file()

            with open(f'{project_root}/src/utils/csv/{self.file_name}', 'a', newline='') as file:
                file_writer = csv.writer(file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
                [file_writer.writerow(row) for row in rows]

        for rows in self.data:
            open_file(rows)
            self.num_rows += 1


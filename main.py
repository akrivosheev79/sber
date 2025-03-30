import json
import os
from datetime import datetime
from uuid import UUID

from utils.dates import Month, get_all_days_of_month

import clickhouse_connect

ch_client = clickhouse_connect.get_client(
    host='rc1a-1l8tq272lsp1ltff.mdb.yandexcloud.net',
    port=8443,
    username='admin',
    password=',FZc\\wTwA!GC',
    verify = False,
)

categories = {
    1: "Restaurant",
    2: "Travel",
    3: "Health&Farms",
    4: "Food",
    5: "Transport",
    6: "Nastya",
    7: "Utility",
    8: "Beauty",
    9: "Entertainment"
}

year, month = 2025, 3
month_name = Month.get_month_name(month).lower()


def check_operations_for_date(operations_dates: list) -> None:
    all_days_of_month = get_all_days_of_month(year, month)
    missing_days = all_days_of_month - set(operations_dates)
    if all_days_of_month.issubset(operations_dates):
        print(f"All days of {month_name} in operations list")
    else:
        print(f"\033[38;5;208mWARNING: Not all dates of month are available {
            [
                missing_day.strftime("%d.%m.%Y") for missing_day in sorted(missing_days)
            ]
        }\033[0m")


if __name__ == '__main__':

    operations: dict = {}

    for file in os.listdir(f'./{month_name}'):
        json_ops: list = json.load(open(f'./{month_name}/{file}'))['body']['operations']
        for operation in json_ops:
            operations[operation['uohId']] = operation

    check_operations_for_date(
        [
            datetime.strptime(dt['date'], "%d.%m.%YT%H:%M:%S").date() for dt in operations.values()
        ]
    )

    for uuid, operation in operations.items():
        # cat = input(f"What category for {operation['correspondent']}: {categories}")
        # operations[uuid]['category'] = cat
        ch_client.insert(
            'bills.operations',
            [
                (
                    UUID(uuid),
                    datetime.strptime(operation['date'], "%d.%m.%YT%H:%M:%S"),
                    operation['description'],
                    operation['correspondent'],
                    operation['operationAmount']['amount'],
                    operation.get('commission')['amount'] if operation.get('commission') else None,
                    1,
                    categories[1],
                )
            ],
        )

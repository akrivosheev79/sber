import copy
import json
import os
from dataclasses import dataclass
from datetime import datetime, date
import pathlib
import pickle
import sys
from uuid import UUID

from utils.dates import (
    Month,
    get_all_days_of_month,
    get_all_days_between,
    get_last_day_of_month,
    get_date_from_string,
)

import clickhouse_connect

ch_client = clickhouse_connect.get_client(
    host='rc1a-1l8tq272lsp1ltff.mdb.yandexcloud.net',
    port=8443,
    username='admin',
    password=',FZc\\wTwA!GC',
    verify = False,
)

@dataclass
class Category:
    id: int
    name: str
    mcc: list[int]
    correspondents: list[str] = None


categories = {
    1: Category(id=1, name="Restaurant", mcc=[5814, 5812, 7299, 5813]),
    2: Category(id=2, name="Travel", mcc=[]),
    3: Category(id=3, name="Health&Farms", mcc=[5912]),
    4: Category(id=4, name="Food", mcc=[5411, 5499, 5921]),
    5: Category(id=5, name="Transport", mcc=[7512, 4121, 4111, 7999, 5541, 4789, 5541]),
    6: Category(id=6, name="Nastya", mcc=[], correspondents=['Анастасия Андреевна К.', 'Людмила Эдуардовна П']),
    7: Category(id=7, name="Utility", mcc=[4900, 3000000002, 3000000014, 4814, 3000000013]),
    8: Category(id=8, name="Beauty", mcc=[7230]),
    9: Category(id=9, name="Entertainment", mcc=[7832]),
    10: Category(id=10, name="Subscribe", mcc=[7841, 4816, 3990]),
    11: Category(id=11, name="Another", mcc=[]),
    12: Category(id=12, name="Credit", mcc=[6012, 99992351]),
    13: Category(id=13, name="Rent apartments", mcc=[]),
    14: Category(id=14, name="Theatre", mcc=[7922, 7991]),
    15: Category(id=15, name="Salary", mcc=[4070000016, 4070000001, 4070000009]),
    16: Category(id=16, name="Charity", mcc=[8398]),
    17: Category(id=17, name="Gifts", mcc=[5193, 5992]),
    18: Category(id=18, name="Electronics", mcc=[]),
    19: Category(id=19, name="Household", mcc=[5942, 5921, 99992349, 5999, 5200, 5251, 5331]),
    20: Category(id=20, name="Debt", mcc=[]),
}

year, month = 2025, 3
all_days_of_month = get_all_days_of_month(year, month)
last_day = get_last_day_of_month(year, month)
holidays = get_all_days_between(date(year, month, 1), date(year, month, 2))
month_name = Month.get_month_name(month).lower()

operations: dict = {}

total_payments = 0
from_another_period_payments = 0
processed_payments = []
without_write_off_payments = 0
inserted_payments = 0
inserted_earlier_payments = 0

processed_file = os.path.join(pathlib.Path(__file__).parent.resolve(), f'{month_name}/processed')


def check_operations_for_date(operations_dates: list) -> None:
    missing_days = all_days_of_month - set(operations_dates)
    if all_days_of_month.issubset(operations_dates):
        print(f"All days of {month_name} in operations list")
    else:
        print(f'\033[38;5;208mWARNING: Not all dates of month are available \
              {[missing_day.strftime("%d.%m.%Y") for missing_day in sorted(missing_days)]} \
              \033[0m')


def pickle_init_processed():
    if os.path.isfile(processed_file) and os.path.getsize(processed_file) > 0:
        with open (processed_file, 'rb') as fp:
            global processed_payments
            processed_payments = pickle.load(fp)


def pickle_save_processed():
    with open(processed_file, 'wb') as fp:
        pickle.dump(processed_payments, fp)


def init_processed():
    query_result = ch_client.query(
        f'''
        SELECT uuid 
        FROM bills.operations 
        WHERE timestamp >= makeDate({year}, {month}, 1) and timestamp <= makeDate({year}, {month}, {last_day})
        '''
    )
    rows = query_result.result_rows
    global inserted_earlier_payments
    inserted_earlier_payments= len(rows)
    return [str(rows[i][0]) for i in range(len(rows))]


def get_category_by_mcc(mcc_code):
    for _, category in categories.items():
        if mcc_code in category.mcc:
            return category
    return None


def get_category_by_correspondent(correspondent):
    for _, category in categories.items():
        if category.correspondents and correspondent in category.correspondents:
            return category
    return None


def get_category_by_name(name):
    for _, category in categories.items():
        if category.name == name:
            return category
    return None


if __name__ == '__main__':

    processed_payments = init_processed()

    for file in os.listdir(f'./{month_name}'):
        if file.endswith('.json'):
            json_ops: list = json.load(open(f'./{month_name}/{file}'))['body']['operations']
            for operation in json_ops:
                if not operations.get(operation['uohId']):
                    total_payments += 1
                if get_date_from_string(operation['date']) in all_days_of_month:
                    operations[operation['uohId']] = operation
                else:
                    from_another_period_payments += 1

    check_operations_for_date(
        [
            get_date_from_string(dt['date']) for dt in operations.values()
        ]
    )

    for uuid, operation in operations.items():
        if uuid in processed_payments:
            print(f'Payment {uuid} has been processed. Skipped')
            continue
        try:
            if not (operation.get('operationAmount') and operation['operationAmount']['amount']) \
                or operation['form'] == 'UfsTransferSelf' \
                or operation['state']['category'] != 'executed':
                    print(f'Operation {uuid} {operation["description"]} for {operation["correspondent"]} do not have write-off. Will be skipped')
                    processed_payments.append(uuid)
                    without_write_off_payments += 1
                    continue

            selected_category = None
            mcc_code = operation.get('classificationCode')
            recommended_category: Category = get_category_by_mcc(mcc_code)
            if not recommended_category and operation.get('correspondent'):
                recommended_category = get_category_by_correspondent(operation['correspondent'])
            if datetime.strptime(operation['date'], "%d.%m.%YT%H:%M:%S").date() in holidays:
                recommended_category = get_category_by_name('Travel')
            if not recommended_category:
                cat_id = input(
                    f'''
                    Payment {len(operations.keys()) - len(processed_payments)}. What category for: 
                        CORRESPONDENT: {operation['correspondent']}
                        OPERATIONS: {operation['description']}
                        DATE: {operation['date']}
                        AMOUNT: {operation['operationAmount']['amount']}
                        MCC: {mcc_code}
                    '''
                )

                if cat_id == '0' or cat_id == '':
                    break
                else:
                    selected_category = categories[int(cat_id)]
            else:
                selected_category = recommended_category
                
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
                        selected_category.id,
                        selected_category.name,
                    )
                ],
            )
            print(f"Payment CORRESPONDENT: {operation['correspondent']} OPERATIONS: {operation['description']} "
                  f"DATE: {operation['date']} AMOUNT: {operation['operationAmount']['amount']} MCC: {mcc_code} "
                  f"was detected as {selected_category.name}")
            processed_payments.append(uuid)
            inserted_payments += 1
        except Exception as ex:
            print(f'\033[91mERROR: {ex} for operation {operation}\033[0m')

    print(
        f'''
            It was total\t\t\t{total_payments} payments.
            From another period\t\t{from_another_period_payments} payments.
            Payments for process\t{len(operations)} payments. 
            Inserted\t\t\t\t{inserted_payments} payments.
            Was skipped\t\t\t\t{without_write_off_payments} without write-off payments.
            Was inserted earlier\t{inserted_earlier_payments} payments
        '''
    )
import copy
import json
import os
from datetime import datetime
import pathlib
import pickle
import sys
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
    9: "Entertainment",
    10: "Subscribe",
    11: "Another",
    12: "Credit",
    13: "Rent apartments",
    14: "Theatre",
    15: "Salary",
    16: "Charity",
    17: "Gifts",
    18: "Electronics",
    19: "Household",
    20: "Debt"
}

mcc = {
    4: [5814, 5411, 5499, 5921],
    5: [7512, 4121, 4111, 7999, 5541, 4789, 5541],
    16: [8398],
    15: [4070000016, 4070000001, 4070000009],
    8: [7230],
    10: [7841, 4816, 3990],
    7: [4900, 3000000002],
    14: [7922, 7991],
    3: [5912],
    12: [6012],
    1: [5812, 7299],
    9: [7832],
    19: [5942, 5921, 99992349, 5999, 5200],
    17: [5193]
}

year, month = 2025, 3
month_name = Month.get_month_name(month).lower()

processed_payments = []
processed_file = os.path.join(pathlib.Path(__file__).parent.resolve(), f'{month_name}/processed')


def check_operations_for_date(operations_dates: list) -> None:
    all_days_of_month = get_all_days_of_month(year, month)
    missing_days = all_days_of_month - set(operations_dates)
    if all_days_of_month.issubset(operations_dates):
        print(f"All days of {month_name} in operations list")
    else:
        print(f'\033[38;5;208mWARNING: Not all dates of month are available \
              {[missing_day.strftime("%d.%m.%Y") for missing_day in sorted(missing_days)]} \
              \033[0m')

def init_processed():
    if os.path.getsize(processed_file) > 0: 
        with open (processed_file, 'rb') as fp:
            global processed_payments
            processed_payments = pickle.load(fp)

def save_processed():
    with open(processed_file, 'wb') as fp:
        pickle.dump(processed_payments, fp)


def get_category_by_mcc(mcc_code):
    for category, mcc_list in mcc.items():
        if mcc_code in mcc_list:
            return category
    return None


if __name__ == '__main__':

    operations: dict = {}

    init_processed()

    for file in os.listdir(f'./{month_name}'):
        if file.endswith('.json'):
            json_ops: list = json.load(open(f'./{month_name}/{file}'))['body']['operations']
            for operation in json_ops:
                operations[operation['uohId']] = operation

    check_operations_for_date(
        [
            datetime.strptime(dt['date'], "%d.%m.%YT%H:%M:%S").date() for dt in operations.values()
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
                    continue
            
            mcc_code = operation.get('classificationCode')
            recommended_category: int = get_category_by_mcc(mcc_code)
            cat_id = input(
                f'''
                Payment {len(operations.keys()) - len(processed_payments)}. What category for: 
                    CORRESPONDENT: {operation['correspondent']}
                    OPERATIONS: {operation['description']}
                    DATE: {operation['date']}
                    AMOUNT: {operation['operationAmount']['amount']}
                    MCC: {mcc_code}
                Recommended category: {categories[recommended_category] if recommended_category else ''}
                '''
            )

            if cat_id == '':
                cat_id = recommended_category
            elif cat_id == '0':
                save_processed()
                sys.exit(-1)
            else:
                cat_id = int(cat_id)
                
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
                        cat_id,
                        categories[cat_id],
                    )
                ],
            )
            processed_payments.append(uuid)
        except Exception as ex:
            print(f'\033[91mERROR: {ex} for operation {operation}\033[0m')
        finally:
            save_processed()

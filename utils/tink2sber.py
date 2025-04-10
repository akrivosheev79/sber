import datetime
import uuid


def tink2sber(operations: list) -> list:
    sber_ops: list = []
    for op in operations:
        operation = {}
        operation['date'] = datetime.datetime.fromtimestamp(op['debitingTime']['milliseconds']/1000).strftime('%d.%m.%YT%H:%M:%S')
        operation['uohId'] = str(uuid.UUID(int=int(op['id'])))
        operation['description'] = op['description']
        operation['correspondent'] = op['description']
        operation['operationAmount'] = {}
        operation['operationAmount']['amount'] = op['amount']['value'] if op['type'] == 'Credit' else op['amount']['value'] * -1
        operation['classificationCode'] = op['mcc']
        sber_ops.append(operation)

    return sber_ops
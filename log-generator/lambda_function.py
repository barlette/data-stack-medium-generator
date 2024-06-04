import boto3
import os 
import json
from typing import Any
import time 
from datetime import datetime
import random

firehose_client = boto3.client('firehose')

CREATE_CHARGE_TEMPLATE = {
                "client": None,
                "message": "Charge created",
                "timestamp": None,
                "id": None
            }

MISC_TEMPLATE = {
            "id": None,
            "timestamp": None,
            "message": "MISCELLANEOUS"
        }


CLIENT_RESPONSE_TEMPLATE = {
                "id": None,
                "message": None,
                "timestamp": None
            }

def send_log(log: dict, firehose_stream_name: str) -> None:
    firehose_client.put_record(
        DeliveryStreamName=firehose_stream_name,
        Record={
            'Data': json.dumps(log) + '\n'
        }
    )

def lambda_handler(event: Any, context: Any) -> None:
    firehose_stream_name = os.getenv("FIREHOSE_STREAM_NAME")

    client = event['client']
    transaction_id = event['id']
    fl_success = event['fl_success']

    charge = CREATE_CHARGE_TEMPLATE
    charge['client'] = client
    charge['id'] = transaction_id
    charge['timestamp'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
    send_log(charge, firehose_stream_name)

    time.sleep(random.randint(0, 15))
    misc = MISC_TEMPLATE
    misc['id'] = transaction_id
    misc['timestamp'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
    send_log(misc, firehose_stream_name)

    time.sleep(random.randint(0, 15))
    response = CLIENT_RESPONSE_TEMPLATE
    response['id'] = transaction_id
    response['timestamp'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")

    if fl_success:
        response['message'] = 'TRANSACTION SUCCESSFUL'
    else:
        response['message'] = 'TRANSACTION FAILED'
    send_log(response, firehose_stream_name)
    return None
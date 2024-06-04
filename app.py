import json
import random
import uuid
from datetime import datetime
import time
import multiprocessing
from typing import Dict

MAX_ITER = 1

def generate_transaction(client_config: Dict) -> None:
    for i in range(0, client_config['transactions-per-minute']):
        transaction_id = str(uuid.uuid4())
        client_name = client_config['name']
        fl_success = random.randint(0, 100) < client_config['performance']
        transactions_per_minute = client_config['transactions-per-minute']
        lambda_data = {
            "id": transaction_id,
            "client": client_name,
            "fl_success": fl_success,
            "transactions-per-minute": transactions_per_minute
        }
        print(lambda_data)

def main():
    with open('settings.json') as f:
        config = json.load(f)   

    expanded = []
    for client in config['clients']:
        expanded.append(client)
    pool = multiprocessing.Pool(processes=len(expanded))
    pool.map(generate_transaction, expanded)

if __name__ == "__main__":
    main()
import json
import time
import random
import uuid
from datetime import datetime
from kafka import KafkaProducer
from kafka.errors import KafkaError

KAFKA_BROKER = 'localhost:9092'
TOPIC = 'transactions'
NUM_USERS = 50
TRANSACTIONS_PER_BATCH = 10
BATCH_DELAY = 2

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    acks='all',
    retries=3
    )

def generate_transaction(user_id:str):
    return {
        'transaction_id': str(uuid.uuid4()),
        'user_id': user_id,
        'amount':round(random.uniform(10,500),2),
        'merchant_id':f'MERCHANT_{random.randint(1,100)}',
        'timestamp': datetime.utcnow().isoformat(),
        'card_last_four':f'{random.randint(1000,9999)}',
        'country': random.choice(['US', 'UK', 'CA', 'DE', 'FR', 'AU'])
    }

def send_transaction(transaction:dict):
    try:
        future = producer.send(TOPIC, value=transaction)
        record_metadata = future.get(timeout=10)
        print(f"Sent transaction {transaction['transaction_id']} to {record_metadata.topic}")
        return True

    except KafkaError as e:
        print(f"Failed to send transaction: {e}")
        return False


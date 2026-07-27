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

def main():
    print(f"Starting Kafka producer...")
    print(f"Bootstrap servers:{KAFKA_BROKER}")
    print(f"Topic: {TOPIC}")
    print(f"Generating transactions from {NUM_USERS} users... \n")

    user_ids = [f"USER_{i:04d}" for i in range(1, NUM_USERS + 1)]

    try:
        batch_num = 0
        while True:
            batch_num += 1
            print(f"\n--- Batch {batch_num} ---")

            for _ in range(TRANSACTIONS_PER_BATCH):
                user_id = random.choice(user_ids)
                transaction = generate_transaction(user_id)
                send_to_kafka(transaction)

            print(f"Sent {TRANSACTIONS_PER_BATCH} transactions. Waiting for {BATCH_DELAY}s...\n")
            time.sleep(BATCH_DELAY)

    except KeyboardInterrupt:
        print("Stopping Kafka producer...")

    finally:
        producer.flush()
        producer.close()
        print("Producer closed.")

if __name__ == "__main__":
    main()
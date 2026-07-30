import json
import psycopg2
from datetime import datetime, timedelta
from kafka import KafkaConsumer
from kafka.errors import KafkaError
from psycopg2.extras import execute_values

KAFKA_BROKER = 'localhost:9092'
TOPIC = 'transactions'
GROUP_ID = 'fraud_detection_group'

DB_CONFIG = {
    "host": "localhost",
    
    "port": 5432,
    
    "database": "fraud_db",
    
    "user": "fraud_user",
    
    "password": "fraud_password"
}

FRAUD_THRESHOLD_SIGMA = 3.0

class FraudDetector:
        def __init__(self, db_config):
            self.db_config = db_config
            self.conn = None
            self.connect()

        def connect(self):
              try:
                    self.conn = psycopg2.connect(**self.db_config)
                    print("Connected to the database.")
              except psycopg2.Error as e:
                    print(f"Error connecting to the database: {e}")
                    raise

        def store_raw_transaction(self, transaction):
              try:
                    with self.conn.cursor() as cur:
                          cur.execute("""
                            INSERT INTO raw_transactions 
                            (transaction_id, user_id, amount, merchant_id, timestamp, card_last_four, country)
                            VALUES (%s, %s, %s, %s, %s, %s, %s)
                            ON CONFLICT (transaction_id) DO NOTHING
                          """, (
                                transaction['transaction_id'],    # %s #1
                                transaction['user_id'],           # %s #2
                                transaction['amount'],            # %s #3
                                transaction['merchant_id'],       # %s #4
                                transaction['timestamp'],         # %s #5
                                transaction['card_last_four'],    # %s #6
                                transaction['country']            # %s #7
                         ))

              except psycopg2.Error as e:
                    print(f"Error storing raw transaction: {e}")
                    self.conn.rollback()
        
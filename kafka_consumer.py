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

        def get_user_stats(self, user_id, days=7):
                try:
                        with self.conn.cursor() as cur:
                            cutoff_time = datetime.utcnow() - timedelta(days=days)
                            cur.execute("""
                                SELECT 
                                    COUNT(*) as tx_count,           -- how many transactions?
                                    AVG(amount) as avg_amount,      -- what's the average?
                                    STDDEV(amount) as std_dev,      -- what's the spread?
                                    MAX(amount) as max_amount,      -- what's the max?
                                    MIN(amount) as min_amount       -- what's the min?
                                FROM raw_transactions
                                WHERE user_id = %s 
                                AND timestamp >= %s
                            """, (user_id, cutoff_time))
                            
                            result = cur.fetchone()
                            return {
                                'count': result[0] or 0,           # If NULL, use 0
                                'avg': float(result[1]) if result[1] else 0,
                                'stddev': float(result[2]) if result[2] else 0,
                                'max': float(result[3]) if result[3] else 0,
                                'min': float(result[4]) if result[4] else 0,
                            }
                except psycopg2.Error as e:
                        print(f"Error fetching user stats: {e}")
                        return {}
                
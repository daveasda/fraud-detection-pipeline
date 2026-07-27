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


from kafka import KafkaConsumer
from sqlalchemy import create_engine, text

import json
import logging


logger = logging.getLogger(__name__)

# Database
DB_ENGINE = 'postgres://naps:naps@postgresql.emergency-response-demo.svc:5432/emergency_response_demo'
DB_TABLE = 'missions'

# Kafka MQ
TOPIC = 'topic-mission-event'
BOOTSTRAP_SERVER = 'kafka-cluster-kafka-bootstrap.emergency-response-demo.svc:9092'
# BOOTSTRAP_SERVER = 'localhost:9092'

# Start DB engine and Kafka consumer -------------------------------

db = create_engine(DB_ENGINE)

consumer = KafkaConsumer(TOPIC,
                         bootstrap_servers=[BOOTSTRAP_SERVER])

for message in consumer:
    message_str = message.value.decode('utf-8')
    message_json = json.loads(message_str)

    # Keep from development, just for the hell of it
    print(f'{message.key}: {message_str}')

    query = None
    if message_json['messageType'] == 'MissionStartedEvent':
        query = f""" INSERT INTO missions (
                mission_id,
                last_status,
                ts_create,
                ts_update,
                responder_id,
                status)
            VALUES (
                '{message_json['body']['id']}',
                '{message_json['messageType']}',
                {message_json['timestamp']},
                {message_json['timestamp']},
                {message_json['body']['responderId']},
                '{message_json['body']['status']}'
            );
        """
    elif message_json['messageType'] == 'MissionPickedUpEvent':
        query = f""" UPDATE missions
            SET ts_update = {message_json['timestamp']},
                last_status = '{message_json['messageType']}',
                status = '{message_json['body']['status']}'
            WHERE mission_id = '{message_json['body']['id']}';
        """
    elif message_json['messageType'] == 'MissionCompletedEvent':
        query = f""" UPDATE missions
            SET ts_complete = {message_json['timestamp']},
                last_status = '{message_json['messageType']}',
                status = '{message_json['body']['status']}'
            WHERE mission_id = '{message_json['body']['id']}';
        """

    if query is not None: 
        connection = db.connect()
        connection.execute(text(query))


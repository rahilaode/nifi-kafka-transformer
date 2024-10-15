from confluent_kafka import Producer
import socket
import json
import os

KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS')
KAFKA_SECURITY_PROTOCOL = os.getenv('KAFKA_SECURITY_PROTOCOL')
KAFKA_PRODUCER_TOPIC = os.getenv('KAFKA_PRODUCER_TOPIC')

def producer(data):
    config= {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'security.protocol': KAFKA_SECURITY_PROTOCOL,
        'client.id': socket.gethostname(),
        'retries': 10
    }

    payload={
        "data": data
    }
    producer = Producer(config)
    producer.produce(topic=KAFKA_PRODUCER_TOPIC, key="test", value=json.dumps(payload).encode('utf-8'))
    producer.poll(1)
    producer.flush(1)
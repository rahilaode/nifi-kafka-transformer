from confluent_kafka import Consumer
import socket
import os
import logging

KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS')
KAFKA_CONSUMER_GROUP_ID = os.getenv('KAFKA_CONSUMER_GROUP_ID')

def create_consumer():
    try:
        config= {
            'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
            'group.id': KAFKA_CONSUMER_GROUP_ID,
            'enable.auto.commit': False,
            'auto.offset.reset': 'earliest',
            'group.instance.id': socket.gethostname(),
            'client.id': socket.gethostname()
        }
        return Consumer(config)  

    except Exception as e:
        raise e
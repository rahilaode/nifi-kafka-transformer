from confluent_kafka import KafkaException
from consumer import create_consumer
from producer import producer
from datetime import datetime
import os

KAFKA_CONSUMER_TOPIC = os.getenv('KAFKA_CONSUMER_TOPIC')
KAFKA_PRODUCER_TOPIC = os.getenv('KAFKA_PRODUCER_TOPIC')

consumer = create_consumer()
consumer.subscribe([KAFKA_CONSUMER_TOPIC])

try: 
    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            raise KafkaException(msg.error())
        
        else:
            timestamp = datetime.now().isoformat()

            consumer.commit(message=msg, asynchronous=False)
            received_data = msg.value().decode('utf-8')
            
            print(f"{timestamp} - Received data from kafka '{KAFKA_CONSUMER_TOPIC}': {received_data}")
            
            transformed_data = received_data.lower()
            print(f"{timestamp} - Transformed data: {transformed_data}")

            producer(transformed_data)
            print(f"{timestamp} - Produced data to kafka '{KAFKA_PRODUCER_TOPIC}': {transformed_data}")

except Exception as e:
    raise e

finally:
    consumer.close()
import dlt
from kafka import KafkaConsumer
import json


def kafka_consumer(topic: str, batch_size = 500):
    consumer = KafkaConsumer(
        topic, 
        bootstrap_servers=['localhost:9092'], 
        auto_offset_reset='earliest', # read from earliest offset
        max_poll_records = 500,
        value_deserializer = lambda m: m.decode('utf-8'),
    )
    batch = []
    for msg in consumer:
        batch.append(_process_message(msg))
        if len(batch) >= batch_size:
            # Pause consumer subscription to topic
            consumer.pause(*consumer.assignment())
            # Yield the entire batch for DLT processing
            print("Yielding batch")
            yield batch
            batch = []
            # Resume polling
            consumer.resume(*consumer.assignment())



def _process_message(msg):
    try:
        data = json.loads(msg.value) if msg.value else {}
    except (json.JSONDecodeError, TypeError):
        data = {}

    return {
        "_kafka": {
            "topic": msg.topic,  # required field
            "key": msg.key,
            "partition": msg.partition,
        },
        "data": data,
    }

        
        
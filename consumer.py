#!/usr/bin/env python3

from kafka import KafkaConsumer
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def consume_events():
    consumer = KafkaConsumer(
        'test-topic',
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='earliest'
    )
    
    try:
        for message in consumer:
            logger.info(f"Received: {message.value.decode('utf-8')}")
    except KeyboardInterrupt:
        logger.info("Consumer stopped")
    finally:
        consumer.close()

if __name__ == "__main__":
    consume_events()
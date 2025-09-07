#!/usr/bin/env python3
from kafka import KafkaProducer
from requests_sse import EventSource
import logging
import traceback
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def stream_wikimedia_to_kafka():
    print(os.environ.get('KAFKA_BROKER_ADDRESS'))
    producer = KafkaProducer(
        bootstrap_servers=[os.environ.get('KAFKA_BROKER_ADDRESS')],
        batch_size=64000,
        linger_ms=100
    )
    
    wikimedia_url = 'http://stream.wikimedia.org/v2/stream/recentchange'
    headers={'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:142.0) Gecko/20100101 Firefox/142.0'}

    with EventSource(wikimedia_url, headers=headers, timeout=30) as stream:
        for event in stream:
            try:
                producer.send('test-topic', value=event.data.encode('utf-8'))
            except Exception as e:
                logger.error(f"Failed to stream events: {type(e).__name__}: {str(e)}")
                traceback.print_exc()
                break
    producer.close()
                


if __name__ == "__main__":
    stream_wikimedia_to_kafka()
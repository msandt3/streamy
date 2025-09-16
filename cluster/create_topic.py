#!/usr/bin/env python3
from kafka.admin import KafkaAdminClient, NewTopic
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_kafka_topic():
    admin_client = KafkaAdminClient(bootstrap_servers=['kafka-service:29092']) # TODO - env var for prod
    
    topic = NewTopic(
        name='test-topic',
        num_partitions=1,
        replication_factor=1
    )
    
    try:
        admin_client.create_topics([topic])
        logger.info(f"Topic 'test-topic' created successfully")
    except Exception as e:
        logger.error(f"Failed to create topic: {e}")
    finally:
        admin_client.close()

if __name__ == "__main__":
    create_kafka_topic()
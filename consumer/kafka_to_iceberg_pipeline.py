import dlt
from lib.kafka_consumer import kafka_consumer


def kafka_to_filesystem_iceberg() -> None:
    pipeline = dlt.pipeline(
        pipeline_name="wikipedia_events",
        destination="filesystem",
        dataset_name="wiki_events"
    )

    for batch in kafka_consumer('test-topic'):
        pipeline.run(batch, table_name="kafka_messages", table_format="iceberg")
    
    

if __name__ == "__main__":
    kafka_to_filesystem_iceberg()
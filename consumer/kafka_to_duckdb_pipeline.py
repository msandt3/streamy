import dlt
from lib.kafka_consumer import kafka_consumer


def load_from_wikipedia_topic() -> None:
    pipeline = dlt.pipeline(
        pipeline_name="kafka_pipeline",
        destination='duckdb',
        dataset_name="kafka_messages",
    )

    for batch in kafka_consumer('test-topic'):
        pipeline.run(batch, table_name="kafka_messages")


if __name__ == "__main__":
    load_from_wikipedia_topic()
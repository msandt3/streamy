# Streamy Project Context

## Architecture Overview

This is a multi-component streaming data pipeline that mimics production architecture patterns:

### Components

- **Kafka Cluster** (`cluster/`): Apache Kafka 4.1.0 broker with controller
  - Ports: 9092 (external), 9093 (controller), 29092 (internal)
  - Auto-creates topics on startup via `init-kafka` service

- **Producer** (`producer/`): Python application that reads Wikipedia event streams
  - Consumes real-time events from Wikipedia's SSE endpoint
  - Publishes events to Kafka topics
  - Uses `kafka-python==2.2.15` and `requests-sse`
  - The data contract this is interacting with is stored here - https://schema.wikimedia.org/repositories/primary/jsonschema/mediawiki/recentchange/latest.yaml

- **Consumer** (`consumer/`): Client application for stream processing
  - Reads from Kafka topics in configurable batches
  - Supports multiple output formats: DuckDB, Iceberg tables
  - Uses `dlt` (data load tool) for data pipeline management
  - Contains testing framework with pytest

## Dependencies

### Root Dependencies
```
kafka-python==2.2.15
requests-sse
dlt
streamlit
confluent-kafka>=2.3.0
dlt[duckdb]>=0.5.1
```

### Consumer Dependencies
```
dlt
kafka-python==2.2.15
dlt[duckdb]>=0.5.1
dlt[filesystem, pyiceberg, parquet]
pyarrow
pyiceberg<=0.9.1  # compatibility constraint
sqlalchemy>=2.0.18
```

### Producer Dependencies
```
kafka-python==2.2.15
requests-sse
```

### Cluster Dependencies
```
kafka-python==2.2.15
```

## How to Run

### Start Infrastructure
```bash
docker compose build && docker compose up
```

### Run Consumer (Local)
```bash
cd consumer/
# Activate virtual environment first
source venv/bin/activate  # or create with: python -m venv venv
pip install -r requirements.txt
python kafka_to_duckdb_pipeline.py  # or kafka_to_iceberg_pipeline.py
```

### Inspect Pipeline Results
```bash
dlt pipeline kafka_pipeline show
```

## How to Run Tests

```bash
source venv/bin/activate
python -m pytest tests/*.py
```

Test files are located in `consumer/tests/` and cover:
- Message processing logic (`kafka_consumer_tests.py`)
- Batch handling and transformation
- Error handling for malformed messages
- Mock Kafka consumer behavior

## Lint/Type Check Commands

Based on the current setup, use standard Python tools:
```bash
# Must activate consumer venv first for tests
cd consumer/
source venv/bin/activate
python -m pytest tests/*.py
python -m flake8 .  # if configured
python -m mypy .    # if configured
```
 ## Documentation References
- dlt: https://dlthub.com/docs/
- Kafka Python: https://kafka-python.readthedocs.io/
- PyIceberg: https://py.iceberg.apache.org/

"""
Iceberg writer for Kafka message batches with persistent local catalog
"""
from pathlib import Path
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.table import Table
from pyiceberg.exceptions import NamespaceAlreadyExistsError, TableAlreadyExistsError
from pyiceberg.types import StringType, BooleanType, LongType, TimestampType, NestedField
from typing import List, Dict, Any
import pyarrow as pa


class IcebergWriter:
    """Writer class for persisting Kafka messages to Iceberg format"""

    def __init__(self, warehouse_path: str = "./iceberg_warehouse"):
        """
        Initialize IcebergWriter with SQLite-backed local catalog

        Args:
            warehouse_path: Path to warehouse directory for Iceberg data storage
        """
        self.warehouse_path = warehouse_path
        warehouse_path_obj = Path(warehouse_path)
        warehouse_path_obj.mkdir(parents=True, exist_ok=True)

        # Create SQLite catalog configuration
        catalog_config = {
            "type": "sql",
            "uri": f"sqlite:///{warehouse_path}/catalog.db",
            "warehouse": f"file://{warehouse_path_obj.absolute()}"
        }

        self.catalog = load_catalog("local", **catalog_config)

    def _create_wikipedia_schema(self) -> Schema:
        """
        Create schema for Wikipedia recentchange events with Kafka metadata

        Returns:
            Schema with flattened structure for Kafka metadata and Wikipedia fields
            Source: https://schema.wikimedia.org/repositories/primary/jsonschema/mediawiki/recentchange/latest.yaml
        """
        fields = [
            # Kafka metadata fields for provenance tracking
            NestedField(field_id=1, name="kafka_key", field_type=StringType(), required=False),
            NestedField(field_id=2, name="kafka_topic", field_type=StringType(), required=True),
            NestedField(field_id=3, name="kafka_partition", field_type=StringType(), required=True),

            # Core Wikipedia recentchange fields
            NestedField(field_id=4, name="id", field_type=LongType(), required=True),
            NestedField(field_id=5, name="type", field_type=StringType(), required=True),
            NestedField(field_id=6, name="title", field_type=StringType(), required=False),
            NestedField(field_id=7, name="user", field_type=StringType(), required=False),
            NestedField(field_id=8, name="bot", field_type=BooleanType(), required=False),
            NestedField(field_id=9, name="timestamp", field_type=TimestampType(), required=False),
            NestedField(field_id=10, name="wiki", field_type=StringType(), required=False),
            NestedField(field_id=11, name="server_name", field_type=StringType(), required=False),
            NestedField(field_id=12, name="server_url", field_type=StringType(), required=False),
            NestedField(field_id=13, name="namespace", field_type=LongType(), required=False),
            NestedField(field_id=14, name="comment", field_type=StringType(), required=False),
        ]
        ## TODO - store this on self - iseful later down the line
        return Schema(*fields)

    def _flatten_message(self, message: dict) -> dict:
        """
        Flatten a Kafka message to match our Iceberg schema

        Args:
            message: Message with structure {"_kafka": {...}, "data": {...}}

        Returns:
            Flattened record with kafka_ prefixed metadata and data fields
        """
        kafka_meta = message.get("_kafka", {})
        data = message.get("data", {})

        # Prefix kafka metadata fields and merge with data
        kafka_fields = {f"kafka_{k}": v for k, v in kafka_meta.items()}

        return {**kafka_fields, **data}

    def create_table(self, table_name: str) -> Table:
        """
        Create an Iceberg table with predefined schema for Kafka messages
        If table already exists, return the existing table

        Args:
            table_name: Name of the table to create

        Returns:
            Table reference
        """
        namespace = "kafka_streams"
        full_name = f"{namespace}.{table_name}"

        # Create namespace if it doesn't exist
        try:
            self.catalog.create_namespace(namespace)
        except NamespaceAlreadyExistsError:
            # Namespace already exists, continue
            pass

        # Try to create table, if it exists return existing table
        try:
            return self.catalog.create_table(full_name, schema=self._create_wikipedia_schema())
        except TableAlreadyExistsError:
            return self.catalog.load_table(full_name)

    def write_batch(self, batch: list, table_name: str) -> None:
        """
        Write a batch of processed Kafka messages to an Iceberg table

        Args:
            batch: List of processed Kafka messages with structure:
                   [{"_kafka": {"key": "...", "topic": "...", "partition": "..."},
                     "data": {...}}]
            table_name: Name of the table to write to
        """
        if not batch:
            # Handle empty batches - still create table for consistency
            self.create_table(table_name)
            return

        # Get or create the table
        table = self.create_table(table_name)

        # Flatten the message structure to match our schema
        flattened_records = [self._flatten_message(message) for message in batch]

        # Convert our Iceberg schema to PyArrow schema for consistency
        from pyiceberg.io.pyarrow import schema_to_pyarrow
        iceberg_schema = self._create_wikipedia_schema()
        arrow_schema = schema_to_pyarrow(iceberg_schema)

        # Convert to PyArrow table using our schema
        arrow_table = pa.Table.from_pylist(flattened_records, schema=arrow_schema)

        # Write to Iceberg table
        table.append(arrow_table)
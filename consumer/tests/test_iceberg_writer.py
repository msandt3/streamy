import pytest
import tempfile
import shutil
from pathlib import Path
from unittest.mock import patch, MagicMock

from lib.iceberg_writer import IcebergWriter

@pytest.fixture
def temp_warehouse():
    """Provides a temporary directory for testing warehouse operations"""
    temp_dir = tempfile.mkdtemp()
    yield temp_dir
    shutil.rmtree(temp_dir)


@pytest.fixture
def mock_catalog():
    """Mock catalog for testing"""
    return MagicMock()


@pytest.fixture
def integration_warehouse(tmp_path):
    """Provides a warehouse directory for integration tests with cleanup"""
    warehouse_dir = tmp_path / "iceberg_integration_test"
    warehouse_dir.mkdir()
    yield str(warehouse_dir)
    # Automatic cleanup when tmp_path is cleaned up by pytest


class TestIcebergWriterTableCreation:
    """Test cases for IcebergWriter table creation methods"""

    def test_create_table_accepts_table_name_parameter(self, integration_warehouse):
        """Should accept a table name parameter for table creation"""
        writer = IcebergWriter(warehouse_path=integration_warehouse)

        # Should not raise an exception when called with table_name
        result = writer.create_table("test_table")

        # Verify we get a table back
        assert result is not None
        from pyiceberg.table import Table
        assert isinstance(result, Table)

    def test_create_table_with_valid_table_name(self, integration_warehouse):
        """Should create table with valid table name"""
        writer = IcebergWriter(warehouse_path=integration_warehouse)

        result = writer.create_table("valid_table_name")

        assert result.name() == ("kafka_streams", "valid_table_name")

    def test_create_table_creates_namespace_if_not_exists(self, integration_warehouse):
        """Should create namespace before creating table"""
        writer = IcebergWriter(warehouse_path=integration_warehouse)

        # Verify namespace doesn't exist initially
        namespaces = writer.catalog.list_namespaces()
        assert ("kafka_streams",) not in namespaces

        writer.create_table("namespace_test_table")

        # Verify namespace was created
        namespaces = writer.catalog.list_namespaces()
        assert ("kafka_streams",) in namespaces

    def test_create_table_handles_existing_table(self, integration_warehouse):
        """Should return existing table when attempting to create a table that already exists"""
        writer = IcebergWriter(warehouse_path=integration_warehouse)

        # Create table first time
        table1 = writer.create_table("duplicate_test_table")

        # Attempt to create same table again - should return existing table, not raise error
        table2 = writer.create_table("duplicate_test_table")

        # Should return the same table
        assert table1.name() == table2.name()
        assert table1.name() == ("kafka_streams", "duplicate_test_table")

class TestIcebergWriterSchema:
    """Test cases for IcebergWriter schema handling"""

    def test_create_table_uses_proper_schema_for_wikipedia_events(self, integration_warehouse):
        """Should create table with schema that matches Wikipedia recentchange event structure"""
        writer = IcebergWriter(warehouse_path=integration_warehouse)

        table = writer.create_table("wikipedia_events")
        schema = table.schema()

        # Check that schema has required fields for Wikipedia events
        field_names = [field.name for field in schema.fields]

        # Kafka metadata fields for provenance tracking (flattened)
        assert "kafka_key" in field_names
        assert "kafka_topic" in field_names
        assert "kafka_partition" in field_names

        # Core Wikipedia recentchange fields
        assert "id" in field_names
        assert "type" in field_names
        assert "title" in field_names
        assert "user" in field_names
        assert "bot" in field_names
        assert "timestamp" in field_names
        assert "wiki" in field_names
        assert "server_name" in field_names
        assert "server_url" in field_names
        assert "namespace" in field_names
        assert "comment" in field_names

    def test_create_table_schema_has_correct_field_types(self, integration_warehouse):
        """Should create schema with appropriate data types for Wikipedia event fields"""
        writer = IcebergWriter(warehouse_path=integration_warehouse)

        table = writer.create_table("wikipedia_events_typed")
        schema = table.schema()

        # Get field types by name
        fields_by_name = {field.name: field.field_type for field in schema.fields}

        # Check core field types
        from pyiceberg.types import StringType, BooleanType, LongType, TimestampType

        # Kafka metadata field types
        assert isinstance(fields_by_name["kafka_key"], StringType)
        assert isinstance(fields_by_name["kafka_topic"], StringType)
        assert isinstance(fields_by_name["kafka_partition"], StringType)

        # Wikipedia event field types
        assert isinstance(fields_by_name["id"], LongType)
        assert isinstance(fields_by_name["type"], StringType)
        assert isinstance(fields_by_name["title"], StringType)
        assert isinstance(fields_by_name["user"], StringType)
        assert isinstance(fields_by_name["bot"], BooleanType)
        assert isinstance(fields_by_name["timestamp"], TimestampType)
        assert isinstance(fields_by_name["wiki"], StringType)
        assert isinstance(fields_by_name["server_name"], StringType)
        assert isinstance(fields_by_name["server_url"], StringType)
        assert isinstance(fields_by_name["namespace"], LongType)
        assert isinstance(fields_by_name["comment"], StringType)


class TestIcebergWriterBatch:
    """Test cases for IcebergWriter batch writing functionality"""

    def test_write_batch_stores_record_in_table(self, integration_warehouse):
        """Should write records to table and allow retrieval"""
        writer = IcebergWriter(warehouse_path=integration_warehouse)

        # Sample batch matching the consumer contract structure
        batch = [
            {
                "_kafka": {
                    "key": "key_1",
                    "topic": "wikipedia-events",
                    "partition": "0"
                },
                "data": {
                    "id": 123456,
                    "type": "edit",
                    "title": "Test Article",
                    "user": "TestUser",
                    "bot": False,
                    "timestamp": 1672531200,
                    "wiki": "enwiki",
                    "server_name": "en.wikipedia.org",
                    "server_url": "https://en.wikipedia.org",
                    "namespace": 0,
                    "comment": "Test edit"
                }
            }
        ]

        # Write the batch
        writer.write_batch(batch, "test_storage")

        # Retrieve the table and verify data was written
        table = writer.catalog.load_table("kafka_streams.test_storage")
        result = table.scan().to_arrow()

        # Verify record count
        assert len(result) == 1

        # Verify flattened structure and data
        record = result.to_pylist()[0]
        assert record["kafka_key"] == "key_1"
        assert record["kafka_topic"] == "wikipedia-events"
        assert record["kafka_partition"] == "0"
        assert record["id"] == 123456
        assert record["type"] == "edit"
        assert record["title"] == "Test Article"
        assert record["user"] == "TestUser"
        assert record["bot"] == False

    def test_write_batch_handles_multiple_records(self, integration_warehouse):
        """Should write multiple records and maintain data integrity"""
        writer = IcebergWriter(warehouse_path=integration_warehouse)

        batch = [
            {
                "_kafka": {"key": "key_1", "topic": "test-topic", "partition": "0"},
                "data": {"id": 1, "type": "edit", "title": "Article 1"}
            },
            {
                "_kafka": {"key": "key_2", "topic": "test-topic", "partition": "1"},
                "data": {"id": 2, "type": "new", "title": "Article 2"}
            }
        ]

        writer.write_batch(batch, "multi_test")

        # Verify both records were written
        table = writer.catalog.load_table("kafka_streams.multi_test")
        result = table.scan().to_arrow()
        assert len(result) == 2

        records = result.to_pylist()
        assert records[0]["id"] == 1
        assert records[1]["id"] == 2

    def test_write_batch_handles_empty_batch(self, integration_warehouse):
        """Should handle empty batches without error"""
        writer = IcebergWriter(warehouse_path=integration_warehouse)

        empty_batch = []
        writer.write_batch(empty_batch, "empty_test")

        # Table should exist but be empty
        table = writer.catalog.load_table("kafka_streams.empty_test")
        result = table.scan().to_arrow()
        assert len(result) == 0

    def test_write_batch_ignores_extra_fields_not_in_schema(self, integration_warehouse):
        """Should ignore fields in data that are not defined in our Iceberg schema"""
        writer = IcebergWriter(warehouse_path=integration_warehouse)

        batch = [
            {
                "_kafka": {
                    "key": "key_1",
                    "topic": "test-topic",
                    "partition": "0",
                    "extra_kafka_field": "should_be_ignored"  # Not in schema
                },
                "data": {
                    "id": 123,
                    "type": "edit",
                    "title": "Test Article",
                    "unknown_field": "should_be_ignored",  # Not in schema
                    "another_extra": {"nested": "data"},    # Not in schema
                    "random_number": 42                     # Not in schema
                }
            }
        ]

        # Should write successfully, ignoring extra fields
        writer.write_batch(batch, "extra_fields_test")

        # Verify only schema-defined fields are stored
        table = writer.catalog.load_table("kafka_streams.extra_fields_test")
        result = table.scan().to_arrow()
        assert len(result) == 1

        record = result.to_pylist()[0]

        # Verify expected fields are present
        assert record["kafka_key"] == "key_1"
        assert record["kafka_topic"] == "test-topic"
        assert record["kafka_partition"] == "0"
        assert record["id"] == 123
        assert record["type"] == "edit"
        assert record["title"] == "Test Article"

        # Verify extra fields are not present (PyArrow filtered them out)
        assert "extra_kafka_field" not in record
        assert "unknown_field" not in record
        assert "another_extra" not in record
        assert "random_number" not in record


class TestIcebergWriterInit:
    """Test cases for IcebergWriter.__init__ method"""

    def test_init_with_custom_warehouse_path(self, integration_warehouse):
        """Should initialize with custom warehouse path when provided"""
        custom_path = integration_warehouse + "_custom"
        writer = IcebergWriter(warehouse_path=custom_path)
        assert writer.warehouse_path == custom_path

    def test_init_creates_warehouse_directory_if_not_exists(self, integration_warehouse):
        """Should create warehouse directory if it doesn't exist"""
        nonexistent_path = Path(integration_warehouse) / "new_warehouse"
        assert not nonexistent_path.exists()

        writer = IcebergWriter(warehouse_path=str(nonexistent_path))

        assert nonexistent_path.exists()
        assert nonexistent_path.is_dir()

    def test_init_creates_parent_directories_if_not_exists(self, integration_warehouse):
        """Should create all parent directories in the path if they don't exist"""
        nested_path = Path(integration_warehouse) / "data" / "iceberg" / "warehouse"
        assert not nested_path.exists()
        assert not nested_path.parent.exists()
        assert not nested_path.parent.parent.exists()

        writer = IcebergWriter(warehouse_path=str(nested_path))

        assert nested_path.exists()
        assert nested_path.is_dir()
        assert nested_path.parent.exists()
        assert nested_path.parent.parent.exists()

    def test_init_does_not_fail_if_warehouse_directory_exists(self, integration_warehouse):
        """Should not fail if warehouse directory already exists"""
        existing_path = Path(integration_warehouse) / "existing_warehouse"
        existing_path.mkdir()  # Create directory first
        assert existing_path.exists()

        # Should not raise an exception
        writer = IcebergWriter(warehouse_path=str(existing_path))

        # Directory should still exist and be a directory
        assert existing_path.exists()
        assert existing_path.is_dir()
        assert writer.warehouse_path == str(existing_path)

    @patch('lib.iceberg_writer.load_catalog')
    def test_init_creates_sqlite_catalog_with_correct_config(self, mock_load_catalog, integration_warehouse):
        """Should create SQLite catalog with proper configuration"""
        warehouse_path = str(Path(integration_warehouse) / "test_warehouse")
        expected_config = {
            "type": "sql",
            "uri": f"sqlite:///{warehouse_path}/catalog.db",
            "warehouse": f"file://{Path(warehouse_path).absolute()}"
        }

        writer = IcebergWriter(warehouse_path=warehouse_path)

        mock_load_catalog.assert_called_once_with("local", **expected_config)

    @patch('lib.iceberg_writer.load_catalog')
    def test_init_sets_catalog_attribute(self, mock_load_catalog, mock_catalog, integration_warehouse):
        """Should set self.catalog attribute after initialization"""
        mock_catalog = MagicMock()
        mock_load_catalog.return_value = mock_catalog

        warehouse_path = str(Path(integration_warehouse) / "test_warehouse")
        writer = IcebergWriter(warehouse_path=warehouse_path)

        assert writer.catalog is mock_catalog



import pytest
from collections import namedtuple
from unittest.mock import patch
from lib.kafka_consumer import _process_message, kafka_consumer

SampleMessage = namedtuple('SampleMessage', ['key', 'topic', 'partition', 'value'])

@pytest.fixture
def test_batch():
    return [
        SampleMessage(f"key_{i}", f"topic_{i % 10}", f"partition_{i % 3}", f'{{"id": {i}, "name": "example_{i}", "active": true}}')
        for i in range(500)
    ]

@pytest.fixture
def bad_batch():
    return [
        SampleMessage(f"key_{i}", f"topic_{i % 10}", f"partition_{i % 3}", None)
        for i in range(500)
    ]

@pytest.fixture
def sample_message():
    return SampleMessage("hello", "world", "new", '{"id": 1, "name": "example", "active": true}')

@pytest.fixture
def bad_message():
    return SampleMessage("hello", "world", "new", '')

@pytest.fixture
def empty_batch():
    return []


def test_processes_message(sample_message):
    want = { 
        "_kafka": { 
            "key": "hello", 
            "topic": "world", 
            "partition": "new" 
        }, 
        "data": {
            "id": 1, 
            "name": "example", 
            "active": True
        }
    }
    got = _process_message(sample_message)
    assert want == got


def test_process_bad_message(bad_message):
    want = { 
        "_kafka": { 
            "key": "hello", 
            "topic": "world", 
            "partition": "new" 
        }, 
        "data": {}
    }
    got = _process_message(bad_message)
    assert want == got

@patch('lib.kafka_consumer.KafkaConsumer')
def test_returns_a_batch(mock_kafka_consumer, test_batch):
    # Mock the KafkaConsumer to return our test batch
    mock_consumer_instance = mock_kafka_consumer.return_value
    mock_consumer_instance.__iter__.return_value = iter(test_batch)
    mock_consumer_instance.assignment.return_value = []
    
    # Get the batch from the consumer
    for batch in kafka_consumer("test_topic"):
        assert len(batch) == 500
        print("Iteration")

@patch('lib.kafka_consumer.KafkaConsumer')
def test_splits_batches_appropriately(mock_kafka_consumer, test_batch):
    # Mock the KafkaConsumer to return our test batch
    mock_consumer_instance = mock_kafka_consumer.return_value
    mock_consumer_instance.__iter__.return_value = iter(test_batch)
    mock_consumer_instance.assignment.return_value = []
    
    # Force Splitting of the batch
    for batch in kafka_consumer("test_topic", batch_size=100):
        print("Iterating")
        assert len(batch) == 100


@patch('lib.kafka_consumer.KafkaConsumer')
def test_returns_transformed_records(mock_kafka_consumer, test_batch):
    mock_consumer_instance = mock_kafka_consumer.return_value
    mock_consumer_instance.__iter__.return_value = iter(test_batch)
    mock_consumer_instance.assignment.return_value = []

    for batch in kafka_consumer("test_topic", batch_size=500):
        for i in range(len(batch)):
            assert batch[i]["data"]["id"] == i
            assert batch[i]["_kafka"]["key"] == f"key_{i}"
            assert batch[i]["_kafka"]["topic"] == f"topic_{i % 10}"
            assert batch[i]["_kafka"]["partition"] == f"partition_{i % 3}"

@patch('lib.kafka_consumer.KafkaConsumer')
def test_handles_bad_batch(mock_kafka_consumer, bad_batch):
    mock_consumer_instance = mock_kafka_consumer.return_value
    mock_consumer_instance.__iter__.return_value = iter(bad_batch)
    mock_consumer_instance.assignment.return_value = []

    for batch in kafka_consumer("test_topic", batch_size=500):
        for i in range(len(batch)):
            assert batch[i]["data"] == {}
            assert batch[i]["_kafka"]["key"] == f"key_{i}"
            assert batch[i]["_kafka"]["topic"] == f"topic_{i % 10}"
            assert batch[i]["_kafka"]["partition"] == f"partition_{i % 3}"

@patch('lib.kafka_consumer.KafkaConsumer')
def test_handles_empty_batch(mock_kafka_consumer, empty_batch):
    mock_consumer_instance = mock_kafka_consumer.return_value
    mock_consumer_instance.__iter__.return_value = iter(empty_batch)
    mock_consumer_instance.assignment.return_value = []
    
    batches = list(kafka_consumer("test_topic", batch_size=100))
    
    assert len(batches) == 0



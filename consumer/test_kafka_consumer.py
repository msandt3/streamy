import pytest
from lib.kafka_consumer import KafkaSource
from kafka import KafkaConsumer
from unittest.mock import patch, MagicMock, create_autospec


@pytest.fixture
def no_records_in_poll():
    return None

@pytest.fixture
def sample_kafka_messages():
    return {
        'topic': 'test-topic',
        'value': b'{"$schema":"/mediawiki/recentchange/1.0.0","meta":{"uri":"https://www.wikidata.org/wiki/Q131784833","request_id":"8a5cc3f0-bd92-4d5c-80c2-756310e5937e","id":"280ba41b-35dc-4af9-a14a-933bab6df8b5","domain":"www.wikidata.org","stream":"mediawiki.recentchange","dt":"2025-09-08T15:25:40.109Z","topic":"eqiad.mediawiki.recentchange","partition":0,"offset":5920127015},"id":2475994397,"type":"edit","namespace":0,"title":"Q131784833","title_url":"https://www.wikidata.org/wiki/Q131784833","comment":"/* wbsetqualifier-add:1| */ [[Property:P1352]]: 20, #quickstatements; #temporary_batch_1757345009399","timestamp":1757345139,"user":"Guillaumrs","bot":false,"notify_url":"https://www.wikidata.org/w/index.php?diff=2402354425&oldid=2402354409&rcid=2475994397","minor":false,"patrolled":true,"length":{"old":130762,"new":131027},"revision":{"old":2402354409,"new":2402354425},"server_url":"https://www.wikidata.org","server_name":"www.wikidata.org","server_script_path":"/w","wiki":"wikidatawiki","parsedcomment":"\xe2\x80\x8e<span dir=\\"auto\\"><span class=\\"autocomment\\">Qualificatif ajout\xc3\xa9\xe2\x80\xaf: </span></span> <a href=\\"/wiki/Property:P1352\\" title=\\"\xe2\x80\x8erang/classement\xe2\x80\x8e | \xe2\x80\x8eposition ordinale du sujet par rapport aux autres membres d&#039;un classement de qualit\xc3\xa9\xe2\x80\x8e\\"><span class=\\"wb-itemlink\\"><span class=\\"wb-itemlink-label\\" lang=\\"fr\\" dir=\\"ltr\\">rang/classement</span> <span class=\\"wb-itemlink-id\\">(P1352)</span></span></a>: 20, #quickstatements; #temporary_batch_1757345009399"}'
    }
    return None


def tests_creates_kafka_consumer():
    mocked_consumer = create_autospec(KafkaConsumer, instance=True)
    mocked_consumer.poll.return_value = ["hello", "world"]
    source = KafkaSource(consumer = mocked_consumer)
    records = list(source.poll_for_records())
    assert records == ["hello", "world"]
    print(records)
    
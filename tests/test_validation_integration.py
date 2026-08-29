"""Optional validator integration tests. Excluded from default pytest."""

from __future__ import annotations

import json
import uuid

import pytest
from confluent_kafka import Consumer, KafkaException
from confluent_kafka.admin import AdminClient

from producer.app import publish_events
from producer.config import KafkaConfig
from producer.inspect_topic import decode_key, parse_payload
from producer.kafka_producer import TransactionProducer
from producer.publish_invalid import publish_invalid
from validation.service import ValidationService

pytestmark = pytest.mark.integration


def _broker_available(bootstrap_servers: str) -> bool:
    try:
        metadata = AdminClient({"bootstrap.servers": bootstrap_servers}).list_topics(timeout=3)
    except KafkaException:
        return False
    return metadata is not None


@pytest.fixture
def kafka_config():
    config = KafkaConfig.from_env()
    if not _broker_available(config.bootstrap_servers):
        pytest.skip("Kafka broker is not running")
    return KafkaConfig(
        bootstrap_servers=config.bootstrap_servers,
        raw_topic=config.raw_topic,
        validated_topic=config.validated_topic,
        dlq_topic=config.dlq_topic,
        validator_group=f"sentinelpay-validator-it-{uuid.uuid4().hex[:8]}",
    )


def _read_topic(config, topic, limit):
    consumer = Consumer(
        {
            "bootstrap.servers": config.bootstrap_servers,
            "group.id": f"sentinelpay-it-{uuid.uuid4().hex[:8]}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([topic])
    found = []
    try:
        polls = 40
        while len(found) < limit and polls:
            message = consumer.poll(1.0)
            polls -= 1
            if message is None or message.error():
                continue
            found.append((decode_key(message.key()), parse_payload(message.value())))
    finally:
        consumer.close()
    return found


def test_valid_events_appear_on_validated_topic(kafka_config):
    producer = TransactionProducer(config=kafka_config)
    try:
        publish_events(count=5, seed=42, rate=0, producer=producer)
    finally:
        producer.close()

    service = ValidationService(config=kafka_config)
    try:
        processed = service.run(max_messages=5, timeout=20)
    finally:
        service.close()

    assert processed == 5
    records = _read_topic(kafka_config, kafka_config.validated_topic, 5)
    assert len(records) >= 5
    for key, payload in records[-5:]:
        assert key == payload["account_id"]
        assert payload["schema_version"] == 1


def test_invalid_event_appears_on_dlq(kafka_config):
    publish_invalid("amount", config=kafka_config)

    service = ValidationService(config=kafka_config)
    try:
        processed = service.run(max_messages=1, timeout=20)
    finally:
        service.close()

    assert processed == 1
    records = _read_topic(kafka_config, kafka_config.dlq_topic, 1)
    assert records
    key, payload = records[-1]
    assert key == "acct_9999"
    assert payload["error_type"] == "INVALID_AMOUNT"
    assert payload["original_topic"] == kafka_config.raw_topic
    json.dumps(payload)

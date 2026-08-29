"""Optional Kafka integration tests. Excluded from default pytest."""

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
    return config


def test_publish_and_read_raw_events(kafka_config):
    producer = TransactionProducer(config=kafka_config)
    try:
        publish_events(count=3, seed=99, rate=0, producer=producer)
    finally:
        producer.close()

    consumer = Consumer(
        {
            "bootstrap.servers": kafka_config.bootstrap_servers,
            "group.id": f"sentinelpay-it-{uuid.uuid4().hex[:8]}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([kafka_config.raw_topic])
    seen = []
    try:
        deadline_polls = 40
        while len(seen) < 3 and deadline_polls:
            message = consumer.poll(1.0)
            deadline_polls -= 1
            if message is None or message.error():
                continue
            payload = parse_payload(message.value())
            if payload.get("transaction_id") in {"txn_000001", "txn_000002", "txn_000003"}:
                seen.append((decode_key(message.key()), payload))
    finally:
        consumer.close()

    assert len(seen) == 3
    for key, payload in seen:
        assert key == payload["account_id"]
        assert payload["schema_version"] == 1
        json.dumps(payload)

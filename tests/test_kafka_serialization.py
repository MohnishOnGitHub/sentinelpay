"""Unit tests for Kafka JSON serialization and producer wiring.

These tests do not require a running Kafka broker.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from decimal import Decimal
from types import SimpleNamespace

from producer.config import DEFAULT_KAFKA_RAW_TOPIC, KafkaConfig
from producer.kafka_producer import (
    KafkaPublishError,
    TransactionProducer,
    event_json_payload,
    message_key,
    serialize_event,
)
from producer.schemas import (
    Channel,
    Country,
    Currency,
    MerchantCategory,
    TransactionEvent,
    TransactionType,
)


def _sample_event() -> TransactionEvent:
    return TransactionEvent(
        schema_version=1,
        transaction_id="txn_001",
        account_id="acct_1001",
        event_timestamp=datetime(2026, 8, 29, 10, 15, 12, 183000, tzinfo=timezone.utc),
        amount=Decimal("1299.00"),
        currency=Currency.INR,
        merchant_id="m_340",
        merchant_category=MerchantCategory.ELECTRONICS,
        device_id="dev_005",
        latitude=12.9716,
        longitude=77.5946,
        country=Country.IN,
        channel=Channel.ECOMMERCE,
        transaction_type=TransactionType.PURCHASE,
    )


class _FakeProducer:
    def __init__(self, delivery_error=None):
        self.calls = []
        self.delivery_error = delivery_error
        self.flush_timeouts = []

    def produce(self, topic, key=None, value=None, on_delivery=None):
        self.calls.append({"topic": topic, "key": key, "value": value})
        if on_delivery is not None:
            msg = SimpleNamespace(key=lambda: key)
            on_delivery(self.delivery_error, msg)

    def poll(self, _timeout=0):
        return 0

    def flush(self, timeout=None):
        self.flush_timeouts.append(timeout)
        return 0


def test_transaction_event_serializes_to_valid_json():
    raw = serialize_event(_sample_event())
    payload = json.loads(raw)

    assert isinstance(payload, dict)
    assert payload["transaction_id"] == "txn_001"
    assert payload["currency"] == "INR"


def test_message_key_is_account_id():
    event = _sample_event()

    assert message_key(event) == b"acct_1001"
    assert event_json_payload(event)["account_id"] == "acct_1001"


def test_schema_version_is_present():
    payload = event_json_payload(_sample_event())

    assert payload["schema_version"] == 1


def test_timestamp_serializes_as_iso8601_utc():
    payload = event_json_payload(_sample_event())

    assert payload["event_timestamp"] == "2026-08-29T10:15:12.183Z"
    parsed = datetime.fromisoformat(payload["event_timestamp"].replace("Z", "+00:00"))
    assert parsed == datetime(2026, 8, 29, 10, 15, 12, 183000, tzinfo=timezone.utc)


def test_amount_serializes_as_two_decimal_string():
    payload = event_json_payload(_sample_event())

    assert payload["amount"] == "1299.00"
    assert Decimal(payload["amount"]) == Decimal("1299.00")


def test_publish_uses_account_id_key_and_raw_topic():
    fake = _FakeProducer()
    producer = TransactionProducer(
        config=KafkaConfig(raw_topic=DEFAULT_KAFKA_RAW_TOPIC),
        producer=fake,
    )
    event = _sample_event()

    producer.publish(event)

    assert len(fake.calls) == 1
    call = fake.calls[0]
    assert call["topic"] == "transactions.raw"
    assert call["key"] == b"acct_1001"
    assert json.loads(call["value"])["schema_version"] == 1


def test_flush_raises_on_delivery_error():
    fake = _FakeProducer(delivery_error="Local: Broker transport failure")
    producer = TransactionProducer(producer=fake)

    producer.publish(_sample_event())

    try:
        producer.flush()
    except KafkaPublishError as exc:
        assert "acct_1001" in str(exc)
        assert "Broker transport failure" in str(exc)
    else:
        raise AssertionError("expected KafkaPublishError")

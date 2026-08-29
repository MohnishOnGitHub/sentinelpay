"""Broker-free unit tests for validation routing and DLQ metadata."""

from __future__ import annotations

import json
from types import SimpleNamespace

import pytest

from producer.config import KafkaConfig
from producer.kafka_producer import KafkaPublishError
from producer.publish_invalid import build_invalid_event
from validation.service import ValidationService, validate_raw_payload

VALID_PAYLOAD = {
    "schema_version": 1,
    "transaction_id": "txn_001",
    "account_id": "acct_1001",
    "event_timestamp": "2026-08-29T10:15:12.183Z",
    "amount": "1299.00",
    "currency": "INR",
    "merchant_id": "m_340",
    "merchant_category": "ELECTRONICS",
    "device_id": "dev_005",
    "latitude": 12.9716,
    "longitude": 77.5946,
    "country": "IN",
    "channel": "ECOMMERCE",
    "transaction_type": "PURCHASE",
}


class _FakeProducer:
    def __init__(self, fail=False):
        self.calls = []
        self.fail = fail

    def produce(self, topic, key=None, value=None, on_delivery=None):
        if self.fail:
            raise BufferError("broker unavailable")
        self.calls.append({"topic": topic, "key": key, "value": value})
        if on_delivery is not None:
            on_delivery(None, SimpleNamespace(key=lambda: key))

    def poll(self, _timeout=0):
        return 0

    def flush(self, timeout=None):
        return 0


class _FakeConsumer:
    def __init__(self):
        self.commits = []

    def commit(self, message=None, asynchronous=True):
        self.commits.append(message)

    def close(self):
        return None


def _message(value, key=b"acct_1001", topic="transactions.raw", partition=3, offset=17):
    return SimpleNamespace(
        error=lambda: None,
        key=lambda: key,
        value=lambda: value,
        topic=lambda: topic,
        partition=lambda: partition,
        offset=lambda: offset,
    )


def _service(producer=None, consumer=None):
    return ValidationService(
        config=KafkaConfig(),
        consumer=consumer or _FakeConsumer(),
        producer=producer or _FakeProducer(),
    )


def test_valid_payload_routes_to_validated():
    producer = _FakeProducer()
    consumer = _FakeConsumer()
    service = _service(producer, consumer)
    raw = json.dumps(VALID_PAYLOAD).encode("utf-8")

    destination = service.process_message(_message(raw))

    assert destination == "transactions.validated"
    assert producer.calls[0]["topic"] == "transactions.validated"
    assert json.loads(producer.calls[0]["value"])["transaction_id"] == "txn_001"
    assert consumer.commits


def test_malformed_json_routes_to_dlq():
    producer = _FakeProducer()
    service = _service(producer)
    destination = service.process_message(_message(b"{not-json"))

    assert destination == "transactions.dlq"
    payload = json.loads(producer.calls[0]["value"])
    assert payload["error_type"] == "MALFORMED_JSON"


def test_invalid_amount_routes_to_dlq():
    producer = _FakeProducer()
    service = _service(producer)
    raw = json.dumps({**VALID_PAYLOAD, "amount": "0.00"}).encode("utf-8")

    destination = service.process_message(_message(raw))

    assert destination == "transactions.dlq"
    assert json.loads(producer.calls[0]["value"])["error_type"] == "INVALID_AMOUNT"


def test_invalid_enum_routes_to_dlq():
    producer = _FakeProducer()
    service = _service(producer)
    raw = json.dumps({**VALID_PAYLOAD, "currency": "ZZZ"}).encode("utf-8")

    destination = service.process_message(_message(raw))

    assert destination == "transactions.dlq"
    assert json.loads(producer.calls[0]["value"])["error_type"] == "INVALID_ENUM"


def test_extra_field_routes_to_dlq():
    producer = _FakeProducer()
    service = _service(producer)
    raw = json.dumps({**VALID_PAYLOAD, "unexpected_field": "nope"}).encode("utf-8")

    destination = service.process_message(_message(raw))

    assert destination == "transactions.dlq"
    assert json.loads(producer.calls[0]["value"])["error_type"] == "UNEXPECTED_FIELD"


def test_dlq_metadata_contains_original_topic_partition_offset():
    producer = _FakeProducer()
    service = _service(producer)
    raw = json.dumps({**VALID_PAYLOAD, "amount": "-5.00"}).encode("utf-8")

    service.process_message(
        _message(raw, topic="transactions.raw", partition=3, offset=17)
    )
    payload = json.loads(producer.calls[0]["value"])

    assert payload["original_topic"] == "transactions.raw"
    assert payload["original_partition"] == 3
    assert payload["original_offset"] == 17
    assert payload["schema_version"] == 1
    assert payload["validator_version"] == "1"
    assert "failed_at" in payload


def test_kafka_key_is_preserved_for_valid_and_invalid():
    producer = _FakeProducer()
    service = _service(producer)
    valid = json.dumps(VALID_PAYLOAD).encode("utf-8")
    invalid = json.dumps({**VALID_PAYLOAD, "currency": "ZZZ"}).encode("utf-8")

    service.process_message(_message(valid, key=b"acct_1001"))
    service.process_message(_message(invalid, key=b"acct_1001"))

    assert producer.calls[0]["key"] == b"acct_1001"
    assert producer.calls[1]["key"] == b"acct_1001"
    assert json.loads(producer.calls[1]["value"])["original_key"] == "acct_1001"


def test_publish_failure_does_not_commit():
    producer = _FakeProducer(fail=True)
    consumer = _FakeConsumer()
    service = _service(producer, consumer)

    with pytest.raises(KafkaPublishError):
        service.process_message(_message(json.dumps(VALID_PAYLOAD).encode("utf-8")))

    assert consumer.commits == []


def test_validate_raw_payload_classifies_coords_and_missing():
    missing = dict(VALID_PAYLOAD)
    del missing["merchant_id"]
    coords = {**VALID_PAYLOAD, "latitude": 200}

    assert validate_raw_payload(json.dumps(missing).encode("utf-8")).error_type == "MISSING_FIELD"
    assert validate_raw_payload(json.dumps(coords).encode("utf-8")).error_type == "INVALID_COORDINATES"


def test_build_invalid_event_cases_are_rejected():
    for case in ("json", "amount", "enum", "extra", "coords", "missing"):
        _key, value = build_invalid_event(case)
        outcome = validate_raw_payload(value)
        assert not outcome.is_valid

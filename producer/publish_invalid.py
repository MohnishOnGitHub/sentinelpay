"""Publish one intentionally invalid event to ``transactions.raw``.

Used to verify dead-letter routing. Valid events still come from
``python -m producer.app``.

Example:

    python -m producer.publish_invalid --case amount
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from typing import Optional, Tuple

from confluent_kafka import KafkaException, Producer

from producer.config import KafkaConfig
from producer.kafka_producer import KafkaPublishError

_VALID_BASE = {
    "schema_version": 1,
    "transaction_id": "txn_invalid_001",
    "account_id": "acct_9999",
    "event_timestamp": datetime(2026, 8, 29, 10, 15, 12, tzinfo=timezone.utc)
    .isoformat()
    .replace("+00:00", "Z"),
    "amount": "25.00",
    "currency": "INR",
    "merchant_id": "m_001",
    "merchant_category": "ELECTRONICS",
    "device_id": "dev_001",
    "latitude": 12.9716,
    "longitude": 77.5946,
    "country": "IN",
    "channel": "ECOMMERCE",
    "transaction_type": "PURCHASE",
}


def build_invalid_event(case: str) -> Tuple[bytes, bytes]:
    """Return ``(key, value)`` for a known invalid fixture."""
    payload = dict(_VALID_BASE)
    key = payload["account_id"].encode("utf-8")

    if case == "json":
        return key, b"{not-json"
    if case == "amount":
        payload["amount"] = "-10.00"
        return key, json.dumps(payload).encode("utf-8")
    if case == "enum":
        payload["currency"] = "ZZZ"
        return key, json.dumps(payload).encode("utf-8")
    if case == "extra":
        payload["unexpected_field"] = "drop-me"
        return key, json.dumps(payload).encode("utf-8")
    if case == "coords":
        payload["latitude"] = 200.0
        return key, json.dumps(payload).encode("utf-8")
    if case == "missing":
        del payload["amount"]
        return key, json.dumps(payload).encode("utf-8")
    raise ValueError(f"unknown invalid case: {case}")


def publish_invalid(case: str, config: Optional[KafkaConfig] = None, producer: Optional[object] = None) -> None:
    config = config or KafkaConfig.from_env()
    key, value = build_invalid_event(case)
    client = producer or Producer(
        {
            "bootstrap.servers": config.bootstrap_servers,
            "client.id": "sentinelpay-invalid-publisher",
            "acks": "all",
        }
    )
    errors = []

    def on_delivery(err, _msg):
        if err is not None:
            errors.append(str(err))

    try:
        client.produce(topic=config.raw_topic, key=key, value=value, on_delivery=on_delivery)
    except (BufferError, KafkaException) as exc:
        raise KafkaPublishError(f"Failed to enqueue invalid event: {exc}") from exc
    remaining = client.flush(10.0)
    if remaining > 0:
        raise KafkaPublishError("Invalid event was not delivered before flush timeout")
    if errors:
        raise KafkaPublishError("; ".join(errors))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Publish one invalid transaction to Kafka.")
    parser.add_argument(
        "--case",
        choices=("json", "amount", "enum", "extra", "coords", "missing"),
        default="amount",
        help="Which validation failure to emit (default: amount).",
    )
    return parser


def main(argv: Optional[list[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    config = KafkaConfig.from_env()
    try:
        publish_invalid(args.case, config=config)
    except KafkaPublishError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(f"Published invalid '{args.case}' event to {config.raw_topic}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

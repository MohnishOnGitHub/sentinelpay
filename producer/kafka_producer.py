"""Kafka producer wrapper for SentinelPay transaction events.

Publishes JSON-serialized ``TransactionEvent`` records to ``transactions.raw``
with ``account_id`` as the message key so downstream consumers can preserve
per-account ordering (DESIGN.md section 5–6).
"""

from __future__ import annotations

import json
from typing import Any, Optional

from confluent_kafka import KafkaError, KafkaException, Producer

from producer.config import KafkaConfig
from producer.schemas import TransactionEvent


class KafkaPublishError(Exception):
    """Raised when one or more events fail to reach Kafka."""


def serialize_event(event: TransactionEvent) -> bytes:
    """Serialize a transaction event to UTF-8 JSON using the Pydantic contract."""
    return event.model_dump_json().encode("utf-8")


def message_key(event: TransactionEvent) -> bytes:
    """Return the Kafka key that keeps events for one account on one partition."""
    return event.account_id.encode("utf-8")


class TransactionProducer:
    """Thin wrapper around ``confluent_kafka.Producer``.

    Delivery failures are collected from the client callback and raised on
    ``flush`` / ``close``. Callers must not assume a publish succeeded until
    those methods return.
    """

    def __init__(
        self,
        config: Optional[KafkaConfig] = None,
        producer: Optional[Any] = None,
    ) -> None:
        self.config = config or KafkaConfig.from_env()
        self._producer = producer or Producer(
            {
                "bootstrap.servers": self.config.bootstrap_servers,
                "client.id": "sentinelpay-transaction-producer",
                "acks": "all",
            }
        )
        self._delivery_errors: list[str] = []

    def publish(self, event: TransactionEvent, topic: Optional[str] = None) -> None:
        """Enqueue one event. Delivery is confirmed later by ``flush``."""
        destination = topic or self.config.raw_topic
        try:
            self._producer.produce(
                topic=destination,
                key=message_key(event),
                value=serialize_event(event),
                on_delivery=self._on_delivery,
            )
        except (BufferError, KafkaException) as exc:
            raise KafkaPublishError(f"Failed to enqueue {event.transaction_id}: {exc}") from exc
        self._producer.poll(0)

    def flush(self, timeout: float = 10.0) -> None:
        """Block until queued messages are delivered or raise on failure."""
        remaining = self._producer.flush(timeout)
        if remaining > 0:
            raise KafkaPublishError(
                f"{remaining} message(s) still in flight after {timeout:.1f}s flush timeout"
            )
        self._raise_if_delivery_errors()

    def close(self) -> None:
        """Flush outstanding messages. Safe to call more than once."""
        self.flush()

    def _on_delivery(self, err: Optional[KafkaError], msg: Any) -> None:
        if err is None:
            return
        key = _safe_key(msg)
        self._delivery_errors.append(f"Delivery failed for key={key}: {err}")

    def _raise_if_delivery_errors(self) -> None:
        if not self._delivery_errors:
            return
        summary = "; ".join(self._delivery_errors)
        self._delivery_errors = []
        raise KafkaPublishError(summary)


def event_json_payload(event: TransactionEvent) -> dict:
    """Return the JSON-compatible dict that is published as the message value."""
    return json.loads(serialize_event(event))


def _safe_key(msg: Any) -> str:
    if msg is None:
        return "<unknown>"
    key = msg.key() if callable(getattr(msg, "key", None)) else None
    if key is None:
        return "<unknown>"
    if isinstance(key, bytes):
        return key.decode("utf-8", errors="replace")
    return str(key)

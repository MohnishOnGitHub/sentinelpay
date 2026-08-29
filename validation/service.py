"""Validate raw transaction events and route them to validated or DLQ topics.

Flow:

    transactions.raw
          ↓
       validator
      ↙        ↘
    valid      invalid
      ↓          ↓
    transactions.validated
    transactions.dlq

Offsets are committed only after the output publish succeeds.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Optional

from confluent_kafka import Consumer, KafkaError, KafkaException, Producer
from pydantic import ValidationError

from producer.config import KafkaConfig
from producer.kafka_producer import KafkaPublishError, serialize_event
from producer.schemas import TransactionEvent
from validation.models import DeadLetterEvent


@dataclass(frozen=True)
class ValidationOutcome:
    """Result of checking one raw payload against ``TransactionEvent``."""

    event: Optional[TransactionEvent]
    error_type: Optional[str]
    error_message: Optional[str]
    original_payload: Any
    schema_version: Optional[int]

    @property
    def is_valid(self) -> bool:
        return self.event is not None


def _recover_schema_version(payload: Any) -> Optional[int]:
    if isinstance(payload, dict) and isinstance(payload.get("schema_version"), int):
        return payload["schema_version"]
    return None


def _classify_validation_error(exc: ValidationError) -> str:
    for error in exc.errors():
        error_type = error.get("type", "")
        location = error.get("loc", ())
        if error_type == "extra_forbidden":
            return "UNEXPECTED_FIELD"
        if error_type == "enum":
            return "INVALID_ENUM"
        if error_type == "missing":
            return "MISSING_FIELD"
        if "amount" in location:
            return "INVALID_AMOUNT"
        if "latitude" in location or "longitude" in location:
            return "INVALID_COORDINATES"
    return "SCHEMA_VALIDATION"


def validate_raw_payload(raw_value: Optional[bytes]) -> ValidationOutcome:
    """Decode JSON and validate it as a ``TransactionEvent``."""
    if raw_value is None:
        return ValidationOutcome(
            event=None,
            error_type="EMPTY_PAYLOAD",
            error_message="message value is empty",
            original_payload=None,
            schema_version=None,
        )

    try:
        text = raw_value.decode("utf-8")
    except UnicodeDecodeError as exc:
        return ValidationOutcome(
            event=None,
            error_type="MALFORMED_JSON",
            error_message=str(exc),
            original_payload=repr(raw_value),
            schema_version=None,
        )

    try:
        parsed = json.loads(text)
    except json.JSONDecodeError as exc:
        return ValidationOutcome(
            event=None,
            error_type="MALFORMED_JSON",
            error_message=str(exc),
            original_payload=text,
            schema_version=None,
        )

    schema_version = _recover_schema_version(parsed)
    try:
        event = TransactionEvent.model_validate(parsed)
    except ValidationError as exc:
        return ValidationOutcome(
            event=None,
            error_type=_classify_validation_error(exc),
            error_message=str(exc),
            original_payload=parsed,
            schema_version=schema_version,
        )

    return ValidationOutcome(
        event=event,
        error_type=None,
        error_message=None,
        original_payload=parsed,
        schema_version=event.schema_version,
    )


def _decode_key(key: Optional[bytes]) -> Optional[str]:
    if key is None:
        return None
    return key.decode("utf-8", errors="replace")


def build_dead_letter(message: Any, outcome: ValidationOutcome) -> DeadLetterEvent:
    return DeadLetterEvent(
        original_topic=message.topic(),
        original_partition=message.partition(),
        original_offset=message.offset(),
        original_key=_decode_key(message.key()),
        original_payload=outcome.original_payload,
        error_type=outcome.error_type or "UNKNOWN",
        error_message=outcome.error_message or "unknown validation error",
        failed_at=datetime.now(timezone.utc),
        schema_version=outcome.schema_version,
    )


class ValidationService:
    """Consume ``transactions.raw`` and publish to validated or DLQ."""

    def __init__(
        self,
        config: Optional[KafkaConfig] = None,
        consumer: Optional[Any] = None,
        producer: Optional[Any] = None,
    ) -> None:
        self.config = config or KafkaConfig.from_env()
        self._consumer = consumer or Consumer(
            {
                "bootstrap.servers": self.config.bootstrap_servers,
                "group.id": self.config.validator_group,
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,
            }
        )
        self._owns_consumer = consumer is None
        self._producer = producer or Producer(
            {
                "bootstrap.servers": self.config.bootstrap_servers,
                "client.id": "sentinelpay-validator",
                "acks": "all",
            }
        )
        self._owns_producer = producer is None
        self._delivery_errors: list[str] = []
        if self._owns_consumer:
            self._consumer.subscribe([self.config.raw_topic])

    def process_message(self, message: Any) -> str:
        """Validate one record, publish the result, then commit the input offset."""
        if message.error():
            raise KafkaException(message.error())

        outcome = validate_raw_payload(message.value())
        key = message.key()
        if outcome.is_valid:
            topic = self.config.validated_topic
            value = serialize_event(outcome.event)
        else:
            topic = self.config.dlq_topic
            value = build_dead_letter(message, outcome).model_dump_json().encode("utf-8")

        self._publish(topic, key, value)
        self._consumer.commit(message=message, asynchronous=False)
        return topic

    def run(self, max_messages: int = 0, timeout: float = 0.0) -> int:
        """Process messages until interrupted, ``max_messages``, or ``timeout``."""
        processed = 0
        deadline = None if timeout <= 0 else time.monotonic() + timeout
        try:
            while max_messages <= 0 or processed < max_messages:
                if deadline is not None and time.monotonic() >= deadline:
                    break
                message = self._consumer.poll(1.0)
                if message is None:
                    continue
                destination = self.process_message(message)
                processed += 1
                print(
                    f"routed offset={message.offset()} key={_decode_key(message.key())} "
                    f"-> {destination}",
                    flush=True,
                )
        except KeyboardInterrupt:
            print("validator stopped", file=sys.stderr)
        return processed

    def close(self) -> None:
        remaining = self._producer.flush(10)
        if remaining > 0:
            raise KafkaPublishError(f"{remaining} validator message(s) still in flight")
        self._raise_if_delivery_errors()
        if self._owns_consumer:
            self._consumer.close()

    def _publish(self, topic: str, key: Optional[bytes], value: bytes) -> None:
        try:
            self._producer.produce(
                topic=topic,
                key=key,
                value=value,
                on_delivery=self._on_delivery,
            )
        except (BufferError, KafkaException) as exc:
            raise KafkaPublishError(f"Failed to enqueue validation output: {exc}") from exc
        self._producer.poll(0)
        remaining = self._producer.flush(10.0)
        if remaining > 0:
            raise KafkaPublishError(
                f"{remaining} message(s) still in flight after validation publish"
            )
        self._raise_if_delivery_errors()

    def _on_delivery(self, err: Optional[KafkaError], msg: Any) -> None:
        if err is None:
            return
        self._delivery_errors.append(str(err))

    def _raise_if_delivery_errors(self) -> None:
        if not self._delivery_errors:
            return
        summary = "; ".join(self._delivery_errors)
        self._delivery_errors = []
        raise KafkaPublishError(summary)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Validate raw transactions and route them to validated or DLQ topics."
    )
    parser.add_argument(
        "--max-messages",
        type=int,
        default=0,
        help="Stop after this many messages. 0 means run until interrupted.",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=0.0,
        help="Optional seconds to wait for messages. 0 means no timeout.",
    )
    return parser


def main(argv: Optional[list[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    if args.max_messages < 0:
        print("error: --max-messages must be >= 0", file=sys.stderr)
        return 2
    if args.timeout < 0:
        print("error: --timeout must be >= 0", file=sys.stderr)
        return 2

    config = KafkaConfig.from_env()
    service = ValidationService(config=config)
    print(
        f"Validating {config.raw_topic} -> {config.validated_topic} | {config.dlq_topic} "
        f"(group={config.validator_group}, bootstrap={config.bootstrap_servers})",
        flush=True,
    )
    try:
        processed = service.run(max_messages=args.max_messages, timeout=args.timeout)
    except KafkaPublishError as exc:
        print(f"error: validation publish failed: {exc}", file=sys.stderr)
        return 1
    finally:
        try:
            service.close()
        except KafkaPublishError as exc:
            print(f"error: validator flush failed: {exc}", file=sys.stderr)
            return 1

    print(f"Processed {processed} message(s)")
    return 0


if __name__ == "__main__":
    sys.exit(main())

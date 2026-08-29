"""Debug consumer for inspecting raw transaction events on Kafka.

This is a verification tool, not a processing consumer. Each run uses a
fresh consumer group and reads from the earliest offset so a just-published
batch can be printed immediately.

Example:

    python -m producer.inspect_topic --max-messages 10
"""

from __future__ import annotations

import argparse
import json
import sys
import time
import uuid
from typing import Any, Optional, TextIO

from confluent_kafka import Consumer, KafkaException

from producer.config import KafkaConfig


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Print keys and JSON payloads from the raw transaction topic."
    )
    parser.add_argument(
        "--max-messages",
        type=int,
        default=10,
        help="Stop after this many messages (default: 10).",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=30.0,
        help="Seconds to wait for the requested messages (default: 30).",
    )
    parser.add_argument(
        "--topic",
        default=None,
        help="Topic to read. Defaults to KAFKA_RAW_TOPIC / transactions.raw.",
    )
    return parser


def decode_key(key: Optional[bytes]) -> str:
    if key is None:
        return ""
    return key.decode("utf-8")


def parse_payload(value: Optional[bytes]) -> dict:
    if value is None:
        raise ValueError("message value is empty")
    payload = json.loads(value.decode("utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("message value is not a JSON object")
    return payload


def format_record(key: str, payload: dict) -> str:
    pretty = json.dumps(payload, indent=2, sort_keys=False)
    return f"key={key}\n{pretty}"


def consume_and_print(
    consumer: Any,
    max_messages: int,
    timeout: float,
    out: Optional[TextIO] = None,
    err: Optional[TextIO] = None,
) -> int:
    """Poll until ``max_messages`` arrive or ``timeout`` elapses. Returns the count."""
    if max_messages < 1:
        raise ValueError("--max-messages must be >= 1")
    if timeout <= 0:
        raise ValueError("--timeout must be > 0")

    out = out if out is not None else sys.stdout
    err = err if err is not None else sys.stderr
    received = 0
    deadline = time.monotonic() + timeout
    while received < max_messages:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            break
        message = consumer.poll(min(1.0, remaining))
        if message is None:
            continue
        if message.error():
            raise KafkaException(message.error())
        key = decode_key(message.key())
        payload = parse_payload(message.value())
        print(format_record(key, payload), file=out)
        print(file=out)
        received += 1

    if received < max_messages:
        print(
            f"error: received {received} of {max_messages} message(s) before timeout",
            file=err,
        )
    return received


def main(argv: Optional[list[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        if args.max_messages < 1:
            raise ValueError("--max-messages must be >= 1")
        if args.timeout <= 0:
            raise ValueError("--timeout must be > 0")
    except ValueError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2

    config = KafkaConfig.from_env()
    topic = args.topic or config.raw_topic
    consumer = Consumer(
        {
            "bootstrap.servers": config.bootstrap_servers,
            "group.id": f"sentinelpay-inspect-{uuid.uuid4().hex[:8]}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([topic])
    try:
        received = consume_and_print(consumer, args.max_messages, args.timeout)
    except (KafkaException, ValueError, json.JSONDecodeError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    finally:
        consumer.close()

    print(
        f"Read {received} message(s) from {topic} "
        f"via {config.bootstrap_servers}"
    )
    return 0 if received == args.max_messages else 1


if __name__ == "__main__":
    sys.exit(main())

"""CLI entry point for publishing synthetic transactions to Kafka.

Example:

    python -m producer.app --count 100 --seed 42 --rate 10
"""

from __future__ import annotations

import argparse
import sys
import time

from producer.config import (
    DEFAULT_PRODUCE_COUNT,
    DEFAULT_PRODUCE_RATE,
    DEFAULT_SEED,
    KafkaConfig,
)
from producer.generator import TransactionGenerator
from producer.kafka_producer import KafkaPublishError, TransactionProducer


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Generate synthetic SentinelPay transactions and publish them to Kafka."
    )
    parser.add_argument(
        "--count",
        type=int,
        default=DEFAULT_PRODUCE_COUNT,
        help=f"Number of events to publish (default: {DEFAULT_PRODUCE_COUNT}).",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=DEFAULT_SEED,
        help=f"Generator seed for reproducible output (default: {DEFAULT_SEED}).",
    )
    parser.add_argument(
        "--rate",
        type=float,
        default=DEFAULT_PRODUCE_RATE,
        help=(
            "Events per second. Use 0 to publish as fast as the client allows "
            f"(default: {DEFAULT_PRODUCE_RATE:g})."
        ),
    )
    return parser


def _validate_args(count: int, rate: float) -> None:
    if count < 0:
        raise ValueError("--count must be >= 0")
    if rate < 0:
        raise ValueError("--rate must be >= 0")


def publish_events(
    count: int,
    seed: int,
    rate: float,
    producer: TransactionProducer,
) -> int:
    """Generate and publish ``count`` events, honoring the events-per-second rate."""
    _validate_args(count, rate)
    generator = TransactionGenerator(seed=seed)
    interval = (1.0 / rate) if rate > 0 else 0.0
    started_at = time.monotonic()

    for index in range(count):
        producer.publish(generator.generate_one())
        if interval and index + 1 < count:
            target = started_at + (index + 1) * interval
            remaining = target - time.monotonic()
            if remaining > 0:
                time.sleep(remaining)

    producer.flush()
    return count


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        _validate_args(args.count, args.rate)
    except ValueError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2

    kafka_config = KafkaConfig.from_env()
    producer = TransactionProducer(config=kafka_config)
    started_at = time.monotonic()
    try:
        published = publish_events(args.count, args.seed, args.rate, producer)
    except KafkaPublishError as exc:
        print(f"error: Kafka publish failed: {exc}", file=sys.stderr)
        return 1
    finally:
        try:
            producer.close()
        except KafkaPublishError as exc:
            print(f"error: Kafka flush failed: {exc}", file=sys.stderr)
            return 1

    elapsed = max(time.monotonic() - started_at, 0.0)
    observed_rate = (published / elapsed) if elapsed > 0 and published else 0.0
    print(
        f"Published {published} event(s) to {kafka_config.raw_topic} "
        f"via {kafka_config.bootstrap_servers} "
        f"(seed={args.seed}, requested_rate={args.rate:g}/s, "
        f"elapsed={elapsed:.2f}s, observed_rate={observed_rate:.1f}/s)"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())

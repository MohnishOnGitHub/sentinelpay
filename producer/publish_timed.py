"""Publish a small deterministic event-time wave for lake smoke tests.

Two waves let Spark advance the watermark so append-mode Gold windows can
finalize:

    python -m producer.publish_timed --wave early
    python -m producer.publish_timed --wave late

``early`` lands in the 10:00–10:05 UTC window. ``late`` is after 10:50 UTC
so a 10-minute watermark can close that earlier window.
"""

from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime, timezone
from decimal import Decimal

from producer.config import KafkaConfig
from producer.kafka_producer import KafkaPublishError, TransactionProducer
from producer.schemas import (
    Channel,
    Country,
    Currency,
    MerchantCategory,
    TransactionEvent,
    TransactionType,
)

_WAVES: dict[str, tuple[tuple[str, int, int], ...]] = {
    "early": (("a", 10, 1), ("b", 10, 3)),
    "late": (("c", 10, 50), ("d", 10, 52)),
}


def _event(run_id: str, suffix: str, hour: int, minute: int) -> TransactionEvent:
    return TransactionEvent(
        schema_version=1,
        transaction_id=f"txn_lake_{run_id}_{suffix}",
        account_id="acct_lake_3a",
        event_timestamp=datetime(2026, 1, 1, hour, minute, tzinfo=timezone.utc),
        amount=Decimal("25.50"),
        currency=Currency.INR,
        merchant_id="m_lake",
        merchant_category=MerchantCategory.GROCERY,
        device_id="dev_lake",
        latitude=12.9716,
        longitude=77.5946,
        country=Country.IN,
        channel=Channel.ECOMMERCE,
        transaction_type=TransactionType.PURCHASE,
    )


def events_for_wave(wave: str, run_id: str) -> list[TransactionEvent]:
    try:
        specs = _WAVES[wave]
    except KeyError as exc:
        raise ValueError(f"unknown wave {wave!r}; expected early or late") from exc
    return [_event(run_id, suffix, hour, minute) for suffix, hour, minute in specs]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Publish timed events that can close Gold windows.")
    parser.add_argument("--wave", choices=sorted(_WAVES), required=True)
    parser.add_argument(
        "--run-id",
        default="",
        help="Unique prefix so reruns are not dropped by transaction_id dedup.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    run_id = args.run_id or os.environ.get("SMOKE_RUN_ID") or "default"
    producer = TransactionProducer(config=KafkaConfig.from_env())
    published = events_for_wave(args.wave, run_id)
    try:
        for event in published:
            producer.publish(event)
        producer.close()
    except KafkaPublishError as exc:
        print(f"error: Kafka publish failed: {exc}", file=sys.stderr)
        return 1
    print(
        f"Published {len(published)} {args.wave} event(s) "
        f"(run_id={run_id}, account=acct_lake_3a)",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())

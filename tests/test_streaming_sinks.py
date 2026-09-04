"""Broker-free Spark tests for lake partitions, schema, and Parquet round-trips."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from producer.schemas import (
    Channel,
    Country,
    Currency,
    MerchantCategory,
    TransactionEvent,
    TransactionType,
)
from streaming.features import account_window_features, prepare_events
from streaming.schema import parse_validated_json
from streaming.session import UTC_TIMEZONE
from streaming.sinks import (
    GOLD_OUTPUT_COLUMNS,
    SILVER_OUTPUT_COLUMNS,
    prepare_gold_features,
    prepare_silver_transactions,
)

pytestmark = pytest.mark.spark

BANGALORE = (12.9716, 77.5946)


def _event(
    transaction_id: str,
    account_id: str,
    timestamp: datetime,
    amount: str,
) -> str:
    return TransactionEvent(
        schema_version=1,
        transaction_id=transaction_id,
        account_id=account_id,
        event_timestamp=timestamp,
        amount=Decimal(amount),
        currency=Currency.INR,
        merchant_id="m_340",
        merchant_category=MerchantCategory.ELECTRONICS,
        device_id="dev_005",
        latitude=BANGALORE[0],
        longitude=BANGALORE[1],
        country=Country.IN,
        channel=Channel.ECOMMERCE,
        transaction_type=TransactionType.PURCHASE,
    ).model_dump_json()


def _ts(hour: int, minute: int) -> datetime:
    return datetime(2026, 1, 1, hour, minute, tzinfo=timezone.utc)


def _events_frame(spark, payloads):
    return parse_validated_json(spark.createDataFrame([(payload,) for payload in payloads], ["value"]))


def test_silver_partitions_come_from_event_timestamp(spark):
    payloads = [_event("txn_001", "acct_1001", _ts(10, 15), "1299.00")]
    row = prepare_silver_transactions(_events_frame(spark, payloads)).collect()[0]

    assert row.event_date == datetime(2026, 1, 1).date()
    assert row.event_hour == 10
    assert row.event_timestamp == datetime(2026, 1, 1, 10, 15)


def test_silver_partitions_use_utc_not_offset_local_time(spark):
    ist = datetime(2026, 1, 1, 15, 31, tzinfo=timezone(timedelta(hours=5, minutes=30)))
    payloads = [_event("txn_ist", "acct_1001", ist, "10.00")]
    row = prepare_silver_transactions(_events_frame(spark, payloads)).collect()[0]

    assert spark.conf.get("spark.sql.session.timeZone") == UTC_TIMEZONE
    assert row.event_date == datetime(2026, 1, 1).date()
    assert row.event_hour == 10
    assert row.event_date != datetime.now(timezone.utc).date()


def test_silver_schema_preserves_transaction_fields_and_decimal(spark):
    payloads = [_event("txn_001", "acct_1001", _ts(10, 15), "1299.00")]
    frame = prepare_silver_transactions(_events_frame(spark, payloads))
    types = dict(frame.dtypes)
    row = frame.collect()[0]

    assert frame.columns == list(SILVER_OUTPUT_COLUMNS)
    assert types["amount"] == "decimal(12,2)"
    assert row.amount == Decimal("1299.00")
    assert row.transaction_id == "txn_001"
    assert row.account_id == "acct_1001"
    assert row.currency == "INR"
    assert row.merchant_id == "m_340"
    assert row.device_id == "dev_005"
    assert row.country == "IN"


def test_gold_partition_comes_from_window_start(spark):
    payloads = [
        _event("txn_1", "acct_1001", _ts(10, 1), "10.00"),
        _event("txn_2", "acct_1001", _ts(10, 3), "15.50"),
    ]
    features = account_window_features(prepare_events(_events_frame(spark, payloads)))
    gold = prepare_gold_features(features)
    five = next(row for row in gold.collect() if row.window_size == "5m")

    assert gold.columns == list(GOLD_OUTPUT_COLUMNS)
    assert five.window_start == datetime(2026, 1, 1, 10, 0)
    assert five.window_date == datetime(2026, 1, 1).date()
    assert five.txn_count == 2
    assert five.amount_sum == Decimal("25.50")


def test_silver_parquet_round_trip_preserves_decimal_and_timestamp(spark, tmp_path):
    payloads = [_event("txn_001", "acct_1001", _ts(10, 15), "1299.00")]
    silver = prepare_silver_transactions(_events_frame(spark, payloads))
    out = str(tmp_path / "silver")
    silver.write.mode("overwrite").partitionBy("event_date", "event_hour").parquet(out)

    loaded = spark.read.parquet(out)
    types = dict(loaded.dtypes)
    row = loaded.collect()[0]

    assert types["amount"] == "decimal(12,2)"
    assert row.amount == Decimal("1299.00")
    assert row.event_timestamp == datetime(2026, 1, 1, 10, 15)
    assert row.event_date == datetime(2026, 1, 1).date()
    assert row.event_hour == 10
    assert (tmp_path / "silver" / "event_date=2026-01-01" / "event_hour=10").is_dir()


def test_gold_parquet_round_trip_preserves_window_metrics(spark, tmp_path):
    payloads = [
        _event("txn_1", "acct_1001", _ts(10, 1), "10.00"),
        _event("txn_2", "acct_1001", _ts(10, 3), "15.50"),
    ]
    gold = prepare_gold_features(
        account_window_features(prepare_events(_events_frame(spark, payloads)))
    )
    out = str(tmp_path / "gold")
    gold.write.mode("overwrite").partitionBy("window_date").parquet(out)

    loaded = spark.read.parquet(out)
    types = dict(loaded.dtypes)
    five = next(row for row in loaded.collect() if row.window_size == "5m")

    assert types["amount_sum"] == "decimal(12,2)"
    assert types["amount_avg"] == "decimal(12,2)"
    assert types["amount_max"] == "decimal(12,2)"
    assert five.amount_sum == Decimal("25.50")
    assert five.amount_avg == Decimal("12.75")
    assert five.window_start == datetime(2026, 1, 1, 10, 0)
    assert five.window_date == datetime(2026, 1, 1).date()
    assert (tmp_path / "gold" / "window_date=2026-01-01").is_dir()

"""Local SparkSession tests for parse, dedup, and event-time windows."""

from __future__ import annotations

import shutil
import subprocess
from datetime import datetime, timezone
from decimal import Decimal

import pytest


def _java_runtime_available() -> bool:
    java = shutil.which("java")
    if java is None:
        return False
    try:
        output = subprocess.check_output([java, "-version"], stderr=subprocess.STDOUT)
        text = output.decode("utf-8", "replace")
    except (OSError, subprocess.CalledProcessError) as exc:
        text = (getattr(exc, "output", b"") or b"").decode("utf-8", "replace")
    return "version" in text.lower() and "Unable to locate a Java Runtime" not in text

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
from streaming.session import UTC_TIMEZONE, create_spark_session

pytestmark = pytest.mark.spark


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
        latitude=12.9716,
        longitude=77.5946,
        country=Country.IN,
        channel=Channel.ECOMMERCE,
        transaction_type=TransactionType.PURCHASE,
    ).model_dump_json()


def _ts(hour: int, minute: int) -> datetime:
    return datetime(2026, 1, 1, hour, minute, tzinfo=timezone.utc)


@pytest.fixture(scope="module")
def spark():
    if not _java_runtime_available():
        pytest.skip("JDK 11/17 is required for local Spark tests")
    pytest.importorskip("pyspark")

    try:
        session = create_spark_session(
            "sentinelpay-streaming-tests",
            master="local[1]",
            extra_configs={
                "spark.ui.enabled": "false",
                "spark.sql.shuffle.partitions": "1",
            },
        )
        session.sparkContext.setLogLevel("ERROR")
    except Exception as exc:
        pytest.skip(f"local SparkSession is unavailable: {exc}")
    yield session
    session.stop()


def test_spark_session_timezone_is_utc(spark):
    assert spark.conf.get("spark.sql.session.timeZone") == UTC_TIMEZONE


def _events_frame(spark, payloads):
    return parse_validated_json(spark.createDataFrame([(payload,) for payload in payloads], ["value"]))


def test_schema_parsing_types_and_values(spark):
    payload = _event("txn_001", "acct_1001", _ts(10, 15), "1299.00")
    row = _events_frame(spark, [payload]).collect()[0]

    assert row.transaction_id == "txn_001"
    assert row.account_id == "acct_1001"
    assert row.schema_version == 1
    assert row.amount == Decimal("1299.00")
    assert row.event_timestamp == datetime(2026, 1, 1, 10, 15)


def test_duplicate_transaction_id_is_removed(spark):
    payloads = [
        _event("txn_dup", "acct_1001", _ts(10, 1), "10.00"),
        _event("txn_dup", "acct_1001", _ts(10, 1), "10.00"),
        _event("txn_other", "acct_1001", _ts(10, 2), "5.00"),
    ]
    result = prepare_events(_events_frame(spark, payloads)).collect()

    assert {row.transaction_id for row in result} == {"txn_dup", "txn_other"}
    assert len(result) == 2


def test_five_minute_window_count_and_amount_sum(spark):
    payloads = [
        _event("txn_1", "acct_1001", _ts(10, 1), "10.00"),
        _event("txn_1", "acct_1001", _ts(10, 1), "10.00"),
        _event("txn_2", "acct_1001", _ts(10, 3), "15.50"),
        _event("txn_3", "acct_1001", _ts(10, 20), "5.00"),
    ]
    features = account_window_features(prepare_events(_events_frame(spark, payloads)))
    five = [row for row in features.collect() if row.window_size == "5m"]

    first = next(
        row
        for row in five
        if row.window_start == datetime(2026, 1, 1, 10, 0) and row.account_id == "acct_1001"
    )
    assert first.txn_count == 2
    assert first.amount_sum == Decimal("25.50")
    assert first.window_end == datetime(2026, 1, 1, 10, 5)


def test_thirty_minute_window_count(spark):
    payloads = [
        _event("txn_1", "acct_1001", _ts(10, 1), "10.00"),
        _event("txn_2", "acct_1001", _ts(10, 20), "5.00"),
        _event("txn_3", "acct_2002", _ts(10, 10), "7.00"),
    ]
    features = account_window_features(prepare_events(_events_frame(spark, payloads)))
    thirty = [
        row
        for row in features.collect()
        if row.window_size == "30m" and row.account_id == "acct_1001"
    ]

    assert len(thirty) == 1
    assert thirty[0].window_start == datetime(2026, 1, 1, 10, 0)
    assert thirty[0].window_end == datetime(2026, 1, 1, 10, 30)
    assert thirty[0].txn_count == 2
    assert thirty[0].amount_sum == Decimal("15.00")


def test_windows_use_event_time_not_processing_time(spark):
    payloads = [
        _event("txn_old", "acct_1001", _ts(10, 1), "10.00"),
        _event("txn_old_2", "acct_1001", _ts(10, 4), "6.00"),
    ]
    features = account_window_features(prepare_events(_events_frame(spark, payloads)))
    rows = [row for row in features.collect() if row.window_size == "5m"]

    assert len(rows) == 1
    assert rows[0].window_start == datetime(2026, 1, 1, 10, 0)
    assert rows[0].window_end == datetime(2026, 1, 1, 10, 5)
    assert rows[0].window_start.year == 2026
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    assert abs((now - rows[0].window_start).total_seconds()) > 3600

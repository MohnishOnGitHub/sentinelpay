"""Local SparkSession tests for parse, dedup, event-time windows, and signals."""

from __future__ import annotations

import shutil
import subprocess
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
from streaming.config import FeatureConfig
from streaming.features import account_window_features, prepare_events
from streaming.risk_signals import apply_transaction_signals
from streaming.schema import parse_validated_json
from streaming.session import UTC_TIMEZONE, create_spark_session


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


pytestmark = pytest.mark.spark

BANGALORE = (12.9716, 77.5946)
DELHI = (28.6139, 77.2090)


def _event(
    transaction_id: str,
    account_id: str,
    timestamp: datetime,
    amount: str,
    *,
    merchant_id: str = "m_340",
    device_id: str = "dev_005",
    latitude: float = BANGALORE[0],
    longitude: float = BANGALORE[1],
) -> str:
    return TransactionEvent(
        schema_version=1,
        transaction_id=transaction_id,
        account_id=account_id,
        event_timestamp=timestamp,
        amount=Decimal(amount),
        currency=Currency.INR,
        merchant_id=merchant_id,
        merchant_category=MerchantCategory.ELECTRONICS,
        device_id=device_id,
        latitude=latitude,
        longitude=longitude,
        country=Country.IN,
        channel=Channel.ECOMMERCE,
        transaction_type=TransactionType.PURCHASE,
    ).model_dump_json()


def _ts(hour: int, minute: int) -> datetime:
    return datetime(2026, 1, 1, hour, minute, tzinfo=timezone.utc)


def _ist(hour: int, minute: int) -> datetime:
    return datetime(2026, 1, 1, hour, minute, tzinfo=timezone(timedelta(hours=5, minutes=30)))


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


def _features(spark, payloads, config: FeatureConfig | None = None):
    return account_window_features(prepare_events(_events_frame(spark, payloads)), config)


def _window_row(rows, window_size: str, account_id: str, window_start: datetime):
    return next(
        row
        for row in rows
        if row.window_size == window_size
        and row.account_id == account_id
        and row.window_start == window_start
    )


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


def test_five_minute_window_count_sum_avg_max(spark):
    payloads = [
        _event("txn_1", "acct_1001", _ts(10, 1), "10.00"),
        _event("txn_1", "acct_1001", _ts(10, 1), "10.00"),
        _event("txn_2", "acct_1001", _ts(10, 3), "15.50"),
        _event("txn_3", "acct_1001", _ts(10, 20), "5.00"),
    ]
    features = _features(spark, payloads).collect()
    first = _window_row(features, "5m", "acct_1001", datetime(2026, 1, 1, 10, 0))

    assert first.txn_count == 2
    assert first.amount_sum == Decimal("25.50")
    assert first.amount_avg == Decimal("12.75")
    assert first.amount_max == Decimal("15.50")
    assert first.window_end == datetime(2026, 1, 1, 10, 5)


def test_thirty_minute_window_count_sum_avg_max(spark):
    payloads = [
        _event("txn_1", "acct_1001", _ts(10, 1), "10.00"),
        _event("txn_2", "acct_1001", _ts(10, 20), "5.00"),
        _event("txn_3", "acct_2002", _ts(10, 10), "7.00"),
    ]
    features = _features(spark, payloads).collect()
    thirty = _window_row(features, "30m", "acct_1001", datetime(2026, 1, 1, 10, 0))

    assert thirty.window_end == datetime(2026, 1, 1, 10, 30)
    assert thirty.txn_count == 2
    assert thirty.amount_sum == Decimal("15.00")
    assert thirty.amount_avg == Decimal("7.50")
    assert thirty.amount_max == Decimal("10.00")


def test_unique_merchants_and_devices_by_window(spark):
    payloads = [
        _event("txn_1", "acct_1001", _ts(10, 1), "10.00", merchant_id="m_1", device_id="dev_a"),
        _event("txn_2", "acct_1001", _ts(10, 3), "12.00", merchant_id="m_2", device_id="dev_a"),
        _event("txn_3", "acct_1001", _ts(10, 20), "8.00", merchant_id="m_3", device_id="dev_b"),
    ]
    features = _features(spark, payloads).collect()

    five_early = _window_row(features, "5m", "acct_1001", datetime(2026, 1, 1, 10, 0))
    five_late = _window_row(features, "5m", "acct_1001", datetime(2026, 1, 1, 10, 20))
    thirty = _window_row(features, "30m", "acct_1001", datetime(2026, 1, 1, 10, 0))

    assert five_early.unique_merchants == 2
    assert five_early.unique_devices == 1
    assert five_late.unique_merchants == 1
    assert five_late.unique_devices == 1
    assert thirty.unique_merchants == 3
    assert thirty.unique_devices == 2


def test_duplicate_transaction_does_not_inflate_metrics(spark):
    payloads = [
        _event("txn_1", "acct_1001", _ts(10, 1), "10.00", merchant_id="m_1", device_id="dev_a"),
        _event("txn_1", "acct_1001", _ts(10, 1), "10.00", merchant_id="m_1", device_id="dev_a"),
        _event("txn_2", "acct_1001", _ts(10, 2), "20.00", merchant_id="m_2", device_id="dev_b"),
    ]
    features = _features(spark, payloads).collect()
    five = _window_row(features, "5m", "acct_1001", datetime(2026, 1, 1, 10, 0))

    assert five.txn_count == 2
    assert five.amount_sum == Decimal("30.00")
    assert five.unique_merchants == 2
    assert five.unique_devices == 2


def test_windows_use_event_time_not_processing_time(spark):
    payloads = [
        _event("txn_old", "acct_1001", _ts(10, 1), "10.00"),
        _event("txn_old_2", "acct_1001", _ts(10, 4), "6.00"),
    ]
    features = _features(spark, payloads).collect()
    rows = [row for row in features if row.window_size == "5m"]

    assert len(rows) == 1
    assert rows[0].window_start == datetime(2026, 1, 1, 10, 0)
    assert rows[0].window_end == datetime(2026, 1, 1, 10, 5)
    assert rows[0].window_start.year == 2026
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    assert abs((now - rows[0].window_start).total_seconds()) > 3600


def test_offset_timestamp_windows_in_utc(spark):
    """15:31 IST is 10:01 UTC and must land in the 10:00–10:05 UTC window."""
    payloads = [_event("txn_ist", "acct_1001", _ist(15, 31), "10.00")]
    features = _features(spark, payloads).collect()
    five = _window_row(features, "5m", "acct_1001", datetime(2026, 1, 1, 10, 0))

    assert five.window_end == datetime(2026, 1, 1, 10, 5)
    assert five.txn_count == 1


def test_is_high_amount_uses_configured_threshold(spark):
    config = FeatureConfig(high_amount_threshold=Decimal("100.00"))
    payloads = [
        _event("txn_low", "acct_1001", _ts(10, 1), "99.99"),
        _event("txn_high", "acct_1001", _ts(10, 2), "100.00"),
    ]
    signaled = apply_transaction_signals(_events_frame(spark, payloads), config).collect()
    by_id = {row.transaction_id: row.is_high_amount for row in signaled}

    assert by_id["txn_low"] is False
    assert by_id["txn_high"] is True

    five = _window_row(_features(spark, payloads, config).collect(), "5m", "acct_1001", datetime(2026, 1, 1, 10, 0))
    assert five.high_amount_count == 1


def test_multi_device_signal_is_window_scoped(spark):
    config = FeatureConfig(multi_device_threshold=2)
    payloads = [
        _event("txn_1", "acct_1001", _ts(10, 1), "10.00", device_id="dev_a"),
        _event("txn_2", "acct_1001", _ts(10, 2), "11.00", device_id="dev_b"),
        _event("txn_3", "acct_1001", _ts(10, 21), "12.00", device_id="dev_a"),
    ]
    features = _features(spark, payloads, config).collect()

    five_multi = _window_row(features, "5m", "acct_1001", datetime(2026, 1, 1, 10, 0))
    five_single = _window_row(features, "5m", "acct_1001", datetime(2026, 1, 1, 10, 20))
    thirty = _window_row(features, "30m", "acct_1001", datetime(2026, 1, 1, 10, 0))

    assert five_multi.unique_devices == 2
    assert five_multi.multi_device_signal is True
    assert five_single.unique_devices == 1
    assert five_single.multi_device_signal is False
    assert thirty.unique_devices == 2
    assert thirty.multi_device_signal is True


def test_rapid_transaction_signal_uses_count_threshold(spark):
    config = FeatureConfig(rapid_txn_count_threshold=3)
    payloads = [
        _event("txn_1", "acct_1001", _ts(10, 1), "10.00"),
        _event("txn_2", "acct_1001", _ts(10, 2), "11.00"),
        _event("txn_3", "acct_1001", _ts(10, 3), "12.00"),
        _event("txn_4", "acct_1001", _ts(10, 21), "13.00"),
    ]
    features = _features(spark, payloads, config).collect()

    five_rapid = _window_row(features, "5m", "acct_1001", datetime(2026, 1, 1, 10, 0))
    five_slow = _window_row(features, "5m", "acct_1001", datetime(2026, 1, 1, 10, 20))
    thirty = _window_row(features, "30m", "acct_1001", datetime(2026, 1, 1, 10, 0))

    assert five_rapid.txn_count == 3
    assert five_rapid.rapid_transaction_signal is True
    assert five_slow.txn_count == 1
    assert five_slow.rapid_transaction_signal is False
    assert thirty.txn_count == 4
    assert thirty.rapid_transaction_signal is True


def test_unique_locations_and_spread_and_location_signal(spark):
    config = FeatureConfig(location_spread_km_threshold=25.0)
    same_city = [
        _event("txn_1", "acct_1001", _ts(10, 1), "10.00", latitude=BANGALORE[0], longitude=BANGALORE[1]),
        _event("txn_2", "acct_1001", _ts(10, 2), "11.00", latitude=BANGALORE[0], longitude=BANGALORE[1]),
    ]
    distant = [
        _event("txn_3", "acct_2002", _ts(10, 1), "10.00", latitude=BANGALORE[0], longitude=BANGALORE[1]),
        _event("txn_4", "acct_2002", _ts(10, 2), "11.00", latitude=DELHI[0], longitude=DELHI[1]),
    ]
    features = _features(spark, same_city + distant, config).collect()

    local = _window_row(features, "5m", "acct_1001", datetime(2026, 1, 1, 10, 0))
    travel = _window_row(features, "5m", "acct_2002", datetime(2026, 1, 1, 10, 0))

    assert local.unique_locations == 1
    assert local.location_spread_km == 0.0
    assert local.rapid_location_change_signal is False

    assert travel.unique_locations == 2
    assert 1700.0 < travel.location_spread_km < 1800.0
    assert travel.rapid_location_change_signal is True

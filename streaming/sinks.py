"""Parquet data-lake sinks and event-time partition helpers.

Silver is append-only deduplicated transactions. Gold is append-only
finalized tumbling-window aggregates. File sinks require append mode;
window rows therefore appear only after the watermark passes ``window_end``.
"""

from __future__ import annotations

from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from streaming.schema import TRANSACTION_EVENT_FIELD_NAMES

SILVER_PARTITION_COLUMNS = ("event_date", "event_hour")
GOLD_PARTITION_COLUMNS = ("window_date",)

SILVER_OUTPUT_COLUMNS = TRANSACTION_EVENT_FIELD_NAMES + SILVER_PARTITION_COLUMNS
GOLD_FEATURE_COLUMNS = (
    "account_id",
    "window_start",
    "window_end",
    "window_size",
    "txn_count",
    "amount_sum",
    "amount_avg",
    "amount_max",
    "unique_merchants",
    "unique_devices",
    "unique_locations",
    "location_spread_km",
    "high_amount_count",
    "multi_device_signal",
    "rapid_transaction_signal",
    "rapid_location_change_signal",
)
GOLD_OUTPUT_COLUMNS = GOLD_FEATURE_COLUMNS + GOLD_PARTITION_COLUMNS


def add_silver_partitions(events: DataFrame) -> DataFrame:
    """Derive ``event_date`` / ``event_hour`` from event time, not processing time."""
    return events.withColumn("event_date", F.to_date(F.col("event_timestamp"))).withColumn(
        "event_hour", F.hour(F.col("event_timestamp"))
    )


def add_gold_partitions(features: DataFrame) -> DataFrame:
    """Derive ``window_date`` from the event-time window start."""
    return features.withColumn("window_date", F.to_date(F.col("window_start")))


def prepare_silver_transactions(events: DataFrame) -> DataFrame:
    """Select the Silver grain: one row per deduplicated transaction plus partitions."""
    return add_silver_partitions(events).select(*SILVER_OUTPUT_COLUMNS)


def prepare_gold_features(features: DataFrame) -> DataFrame:
    """Select the Gold grain: one row per (account, window, window_size)."""
    return add_gold_partitions(features).select(*GOLD_OUTPUT_COLUMNS)


def _ensure_parent(path: str) -> None:
    Path(path).mkdir(parents=True, exist_ok=True)


def start_silver_parquet_query(
    events: DataFrame,
    path: str,
    checkpoint_dir: str,
    trigger_seconds: int,
):
    """Append watermark-deduplicated transactions as partitioned Parquet."""
    _ensure_parent(path)
    _ensure_parent(checkpoint_dir)
    return (
        prepare_silver_transactions(events)
        .writeStream.outputMode("append")
        .format("parquet")
        .option("path", path)
        .option("checkpointLocation", checkpoint_dir)
        .partitionBy(*SILVER_PARTITION_COLUMNS)
        .queryName("silver-transactions")
        .trigger(processingTime=f"{trigger_seconds} seconds")
        .start()
    )


def start_gold_parquet_query(
    features: DataFrame,
    path: str,
    checkpoint_dir: str,
    trigger_seconds: int,
):
    """Append finalized account-window features as partitioned Parquet.

    Append mode emits a window only after the watermark has passed
    ``window_end``. Update-mode console output can show in-progress windows;
    Gold files will not.
    """
    _ensure_parent(path)
    _ensure_parent(checkpoint_dir)
    return (
        prepare_gold_features(features)
        .writeStream.outputMode("append")
        .format("parquet")
        .option("path", path)
        .option("checkpointLocation", checkpoint_dir)
        .partitionBy(*GOLD_PARTITION_COLUMNS)
        .queryName("gold-account-features")
        .trigger(processingTime=f"{trigger_seconds} seconds")
        .start()
    )


def start_console_query(features: DataFrame, checkpoint_dir: str, trigger_seconds: int):
    """Optional update-mode console sink for local debugging."""
    _ensure_parent(checkpoint_dir)
    return (
        features.writeStream.outputMode("update")
        .format("console")
        .option("truncate", "false")
        .option("numRows", "40")
        .option("checkpointLocation", checkpoint_dir)
        .queryName("console-features")
        .trigger(processingTime=f"{trigger_seconds} seconds")
        .start()
    )

"""Event-time watermark, deduplication, and first account velocity features."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from streaming.config import DEFAULT_WATERMARK


def apply_watermark(events: DataFrame, watermark: str = DEFAULT_WATERMARK) -> DataFrame:
    """Bound late-event state using ``event_timestamp``, not processing time."""
    return events.withWatermark("event_timestamp", watermark)


def deduplicate_transactions(events: DataFrame) -> DataFrame:
    """Keep one row per ``transaction_id`` within the watermark horizon."""
    return events.dropDuplicates(["transaction_id"])


def prepare_events(events: DataFrame, watermark: str = DEFAULT_WATERMARK) -> DataFrame:
    """Apply event-time watermark then deduplicate by ``transaction_id``."""
    return deduplicate_transactions(apply_watermark(events, watermark))


def _window_aggregates(events: DataFrame, duration: str, window_size: str) -> DataFrame:
    return (
        events.groupBy(F.col("account_id"), F.window(F.col("event_timestamp"), duration))
        .agg(
            F.count(F.lit(1)).alias("txn_count"),
            F.sum("amount").alias("amount_sum"),
        )
        .select(
            F.col("account_id"),
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            F.lit(window_size).alias("window_size"),
            F.col("txn_count"),
            F.col("amount_sum"),
        )
    )


def account_window_features(events: DataFrame) -> DataFrame:
    """Compute 5-minute and 30-minute account velocity features.

    Windows are tumbling event-time windows on ``event_timestamp``:

    - 5m → ``txn_count_5m`` / ``amount_sum_5m`` (via ``window_size='5m'``)
    - 30m → ``txn_count_30m`` / ``amount_sum_30m`` (via ``window_size='30m'``)
    """
    five = _window_aggregates(events, "5 minutes", "5m")
    thirty = _window_aggregates(events, "30 minutes", "30m")
    return five.unionByName(thirty)

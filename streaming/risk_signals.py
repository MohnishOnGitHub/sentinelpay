"""Deterministic behavioral signals for later rules or models.

These flags are not fraud decisions. They mark simple, thresholded patterns
visible in a single transaction or a single event-time window.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

from streaming.config import FeatureConfig


def apply_transaction_signals(
    events: DataFrame, feature_config: FeatureConfig | None = None
) -> DataFrame:
    """Add stateless per-transaction flags that need no account history.

    Currently:

    - ``is_high_amount`` — ``amount >= HIGH_AMOUNT_THRESHOLD``

    Device and location flags that need other transactions in the same
    event-time window are applied after aggregation in
    ``apply_window_signals``. Lifetime "new device" is not computed here.
    """
    config = feature_config or FeatureConfig()
    threshold = F.lit(config.high_amount_threshold).cast(DecimalType(12, 2))
    return events.withColumn("is_high_amount", F.col("amount") >= threshold)


def apply_window_signals(
    windows: DataFrame, feature_config: FeatureConfig | None = None
) -> DataFrame:
    """Add window-level behavioral flags from already-computed aggregates.

    - ``multi_device_signal`` — ``unique_devices >= MULTI_DEVICE_THRESHOLD``
    - ``rapid_transaction_signal`` — ``txn_count >= RAPID_TXN_COUNT_THRESHOLD``
    - ``rapid_location_change_signal`` — at least two grid cells and a
      bounding-box spread at or above ``LOCATION_SPREAD_KM_THRESHOLD``

    The same count threshold is used for 5m and 30m windows; a 30m window
    hitting the cutoff is a weaker "rapid" signal than a 5m window.
    """
    config = feature_config or FeatureConfig()
    return (
        windows.withColumn(
            "multi_device_signal",
            F.col("unique_devices") >= F.lit(config.multi_device_threshold),
        )
        .withColumn(
            "rapid_transaction_signal",
            F.col("txn_count") >= F.lit(config.rapid_txn_count_threshold),
        )
        .withColumn(
            "rapid_location_change_signal",
            (F.col("unique_locations") >= F.lit(2))
            & (F.col("location_spread_km") >= F.lit(config.location_spread_km_threshold)),
        )
    )

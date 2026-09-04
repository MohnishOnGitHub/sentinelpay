"""Event-time watermark, deduplication, and account window features.

Windows are tumbling event-time windows on ``event_timestamp``. Sliding
windows are not used: they would multiply per-key state by the slide ratio
without producing true per-transaction lookbacks. Point-in-time rolling
features would need keyed state (``applyInPandasWithState`` / mapGroupsWithState)
and are left for a later phase.
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

from streaming.config import DEFAULT_WATERMARK, EARTH_RADIUS_KM, FeatureConfig
from streaming.risk_signals import apply_transaction_signals, apply_window_signals


def apply_watermark(events: DataFrame, watermark: str = DEFAULT_WATERMARK) -> DataFrame:
    """Bound late-event state using ``event_timestamp``, not processing time."""
    return events.withWatermark("event_timestamp", watermark)


def deduplicate_transactions(events: DataFrame) -> DataFrame:
    """Keep one row per ``transaction_id`` within the watermark horizon."""
    return events.dropDuplicates(["transaction_id"])


def prepare_events(events: DataFrame, watermark: str = DEFAULT_WATERMARK) -> DataFrame:
    """Apply event-time watermark then deduplicate by ``transaction_id``."""
    return deduplicate_transactions(apply_watermark(events, watermark))


def haversine_km(lat1: Column, lon1: Column, lat2: Column, lon2: Column) -> Column:
    """Great-circle distance in kilometers between two WGS84 points.

    Implemented as Spark SQL expressions (no Python UDF) so it can run
    inside streaming aggregations. Clamps the haversine ``a`` term to
    ``[0, 1]`` to avoid floating-point ``asin`` domain errors.
    """
    radius = F.lit(EARTH_RADIUS_KM)
    phi1 = F.radians(lat1)
    phi2 = F.radians(lat2)
    d_phi = F.radians(lat2 - lat1)
    d_lambda = F.radians(lon2 - lon1)
    a = F.pow(F.sin(d_phi / F.lit(2.0)), 2) + F.cos(phi1) * F.cos(phi2) * F.pow(
        F.sin(d_lambda / F.lit(2.0)), 2
    )
    a = F.greatest(F.lit(0.0), F.least(a, F.lit(1.0)))
    return F.lit(2.0) * radius * F.asin(F.sqrt(a))


def location_grid_key(decimals: int) -> Column:
    """Round lat/lon to a grid cell used as a distinct-location identity.

    Three decimal places is roughly 100 m at the equator. This is a
    diversity key, not a geohash system.
    """
    return F.concat(
        F.round(F.col("latitude"), decimals).cast("string"),
        F.lit(","),
        F.round(F.col("longitude"), decimals).cast("string"),
    )


def _window_aggregates(
    events: DataFrame, duration: str, window_size: str, config: FeatureConfig
) -> DataFrame:
    loc_key = location_grid_key(config.location_grid_decimals)
    aggregated = events.groupBy(F.col("account_id"), F.window(F.col("event_timestamp"), duration)).agg(
        F.count(F.lit(1)).alias("txn_count"),
        F.sum("amount").cast(DecimalType(12, 2)).alias("amount_sum"),
        F.avg("amount").cast(DecimalType(12, 2)).alias("amount_avg"),
        F.max("amount").cast(DecimalType(12, 2)).alias("amount_max"),
        F.size(F.collect_set("merchant_id")).alias("unique_merchants"),
        F.size(F.collect_set("device_id")).alias("unique_devices"),
        F.size(F.collect_set(loc_key)).alias("unique_locations"),
        F.min("latitude").alias("_min_lat"),
        F.max("latitude").alias("_max_lat"),
        F.min("longitude").alias("_min_lon"),
        F.max("longitude").alias("_max_lon"),
        F.sum(F.col("is_high_amount").cast("int")).alias("high_amount_count"),
    )
    selected = aggregated.select(
        F.col("account_id"),
        F.col("window.start").alias("window_start"),
        F.col("window.end").alias("window_end"),
        F.lit(window_size).alias("window_size"),
        F.col("txn_count"),
        F.col("amount_sum"),
        F.col("amount_avg"),
        F.col("amount_max"),
        F.col("unique_merchants"),
        F.col("unique_devices"),
        F.col("unique_locations"),
        F.round(
            haversine_km(
                F.col("_min_lat"),
                F.col("_min_lon"),
                F.col("_max_lat"),
                F.col("_max_lon"),
            ),
            3,
        ).alias("location_spread_km"),
        F.col("high_amount_count"),
    )
    return apply_window_signals(selected, config)


def account_window_features(
    events: DataFrame, feature_config: FeatureConfig | None = None
) -> DataFrame:
    """Compute 5-minute and 30-minute account behavioral features.

    Windows are tumbling event-time windows on ``event_timestamp``:

    - 5m → ``window_size='5m'`` (maps to ``*_5m`` feature names)
    - 30m → ``window_size='30m'`` (maps to ``*_30m`` feature names)

    ``location_spread_km`` is the haversine length of the window's lat/lon
    bounding-box diagonal, not max pairwise distance and not impossible-travel.
    """
    config = feature_config or FeatureConfig()
    signaled = apply_transaction_signals(events, config)
    five = _window_aggregates(signaled, "5 minutes", "5m", config)
    thirty = _window_aggregates(signaled, "30 minutes", "30m", config)
    return five.unionByName(thirty)

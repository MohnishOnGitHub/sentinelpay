"""Environment-driven settings for the local Spark streaming job."""

from __future__ import annotations

import os
from dataclasses import dataclass
from decimal import Decimal

from producer.config import KafkaConfig

DEFAULT_CHECKPOINT_DIR = ".checkpoints/streaming"
DEFAULT_WATERMARK = "10 minutes"
DEFAULT_TRIGGER_SECONDS = 5

DEFAULT_HIGH_AMOUNT_THRESHOLD = Decimal("5000.00")
DEFAULT_RAPID_TXN_COUNT_THRESHOLD = 5
DEFAULT_MULTI_DEVICE_THRESHOLD = 2
DEFAULT_LOCATION_SPREAD_KM_THRESHOLD = 25.0
DEFAULT_LOCATION_GRID_DECIMALS = 3
EARTH_RADIUS_KM = 6371.0


def _env(name: str, default: str) -> str:
    value = os.environ.get(name)
    return default if value is None or value.strip() == "" else value


@dataclass(frozen=True)
class FeatureConfig:
    """Thresholds for window aggregates and behavioral risk signals.

    Business cutoffs live here (or in env vars) rather than in DataFrame
    expressions so later rules/models can tune them in one place.
    """

    high_amount_threshold: Decimal = DEFAULT_HIGH_AMOUNT_THRESHOLD
    rapid_txn_count_threshold: int = DEFAULT_RAPID_TXN_COUNT_THRESHOLD
    multi_device_threshold: int = DEFAULT_MULTI_DEVICE_THRESHOLD
    location_spread_km_threshold: float = DEFAULT_LOCATION_SPREAD_KM_THRESHOLD
    location_grid_decimals: int = DEFAULT_LOCATION_GRID_DECIMALS

    @classmethod
    def from_env(cls) -> FeatureConfig:
        return cls(
            high_amount_threshold=Decimal(
                _env("HIGH_AMOUNT_THRESHOLD", str(DEFAULT_HIGH_AMOUNT_THRESHOLD))
            ),
            rapid_txn_count_threshold=int(
                _env("RAPID_TXN_COUNT_THRESHOLD", str(DEFAULT_RAPID_TXN_COUNT_THRESHOLD))
            ),
            multi_device_threshold=int(
                _env("MULTI_DEVICE_THRESHOLD", str(DEFAULT_MULTI_DEVICE_THRESHOLD))
            ),
            location_spread_km_threshold=float(
                _env("LOCATION_SPREAD_KM_THRESHOLD", str(DEFAULT_LOCATION_SPREAD_KM_THRESHOLD))
            ),
            location_grid_decimals=int(
                _env("LOCATION_GRID_DECIMALS", str(DEFAULT_LOCATION_GRID_DECIMALS))
            ),
        )


class StreamingConfig:
    """Kafka source, checkpoint, watermark, and feature thresholds."""

    def __init__(
        self,
        bootstrap_servers: str | None = None,
        validated_topic: str | None = None,
        checkpoint_dir: str | None = None,
        watermark: str | None = None,
        trigger_seconds: int | None = None,
        features: FeatureConfig | None = None,
    ) -> None:
        kafka = KafkaConfig.from_env()
        self.bootstrap_servers = bootstrap_servers or kafka.bootstrap_servers
        self.validated_topic = validated_topic or kafka.validated_topic
        self.checkpoint_dir = checkpoint_dir or _env("SPARK_CHECKPOINT_DIR", DEFAULT_CHECKPOINT_DIR)
        self.watermark = watermark or _env("SPARK_WATERMARK", DEFAULT_WATERMARK)
        if trigger_seconds is None:
            trigger_seconds = int(_env("SPARK_TRIGGER_SECONDS", str(DEFAULT_TRIGGER_SECONDS)))
        self.trigger_seconds = trigger_seconds
        self.features = features if features is not None else FeatureConfig.from_env()

    @classmethod
    def from_env(cls) -> StreamingConfig:
        return cls()

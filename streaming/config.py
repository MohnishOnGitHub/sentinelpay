"""Environment-driven settings for the local Spark streaming job."""

from __future__ import annotations

import os

from producer.config import KafkaConfig

DEFAULT_CHECKPOINT_DIR = ".checkpoints/streaming"
DEFAULT_WATERMARK = "10 minutes"
DEFAULT_TRIGGER_SECONDS = 5


def _env(name: str, default: str) -> str:
    value = os.environ.get(name)
    return default if value is None or value.strip() == "" else value


class StreamingConfig:
    """Kafka source, checkpoint, and event-time watermark for Phase 2A."""

    def __init__(
        self,
        bootstrap_servers: str | None = None,
        validated_topic: str | None = None,
        checkpoint_dir: str | None = None,
        watermark: str | None = None,
        trigger_seconds: int | None = None,
    ) -> None:
        kafka = KafkaConfig.from_env()
        self.bootstrap_servers = bootstrap_servers or kafka.bootstrap_servers
        self.validated_topic = validated_topic or kafka.validated_topic
        self.checkpoint_dir = checkpoint_dir or _env("SPARK_CHECKPOINT_DIR", DEFAULT_CHECKPOINT_DIR)
        self.watermark = watermark or _env("SPARK_WATERMARK", DEFAULT_WATERMARK)
        if trigger_seconds is None:
            trigger_seconds = int(_env("SPARK_TRIGGER_SECONDS", str(DEFAULT_TRIGGER_SECONDS)))
        self.trigger_seconds = trigger_seconds

    @classmethod
    def from_env(cls) -> StreamingConfig:
        return cls()

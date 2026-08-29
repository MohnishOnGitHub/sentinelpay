"""Dead-letter event contract for failed transaction validation."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Optional

from pydantic import BaseModel, ConfigDict, Field, field_serializer

VALIDATOR_VERSION = "1"


class DeadLetterEvent(BaseModel):
    """Envelope written to ``transactions.dlq`` when a raw event cannot be used.

    The original payload is preserved so nothing is dropped. DESIGN.md section 6
    requires the payload, error reason, processing timestamp, and validator
    version; Phase 1D also records the source topic, partition, and offset.
    """

    model_config = ConfigDict(extra="forbid")

    original_topic: str
    original_partition: int
    original_offset: int
    original_key: Optional[str] = None
    original_payload: Any
    error_type: str
    error_message: str
    failed_at: datetime = Field(description="UTC time the validator rejected the event.")
    schema_version: Optional[int] = None
    validator_version: str = VALIDATOR_VERSION

    @field_serializer("failed_at", when_used="json")
    def serialize_failed_at(self, value: datetime) -> str:
        utc = value.astimezone(timezone.utc)
        return utc.isoformat(timespec="milliseconds").replace("+00:00", "Z")

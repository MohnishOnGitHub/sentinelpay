"""Typed transaction event contract for SentinelPay.

This module is the Phase 1A source of truth for the v1 event shape in DESIGN.md.
Downstream validation, Kafka payloads, and feature pipelines should consume
``TransactionEvent`` rather than ad-hoc dictionaries.
"""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from enum import Enum

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, field_validator


class Currency(str, Enum):
    """ISO-4217 currencies supported in the v1 contract."""

    INR = "INR"
    USD = "USD"
    EUR = "EUR"
    GBP = "GBP"
    SGD = "SGD"
    AUD = "AUD"


class Channel(str, Enum):
    """Payment channel through which the transaction was initiated."""

    ECOMMERCE = "ECOMMERCE"
    POS = "POS"
    MOBILE = "MOBILE"
    ATM = "ATM"


class TransactionType(str, Enum):
    """High-level transaction intent."""

    PURCHASE = "PURCHASE"
    REFUND = "REFUND"
    WITHDRAWAL = "WITHDRAWAL"
    TRANSFER = "TRANSFER"


class MerchantCategory(str, Enum):
    """Merchant category codes used for later category-risk features."""

    ELECTRONICS = "ELECTRONICS"
    GROCERY = "GROCERY"
    RESTAURANT = "RESTAURANT"
    FUEL = "FUEL"
    TRAVEL = "TRAVEL"
    CLOTHING = "CLOTHING"
    ENTERTAINMENT = "ENTERTAINMENT"
    HEALTH = "HEALTH"
    JEWELRY = "JEWELRY"
    UTILITIES = "UTILITIES"


class Country(str, Enum):
    """ISO-3166-1 alpha-2 countries supported in the v1 contract."""

    IN = "IN"
    US = "US"
    GB = "GB"
    DE = "DE"
    SG = "SG"
    AU = "AU"


class TransactionEvent(BaseModel):
    """Normalized payment transaction event (schema version 1).

    Field names and example values follow DESIGN.md section 5. Amount uses
    ``Decimal`` so later scoring and aggregates do not accumulate binary
    floating-point error. ``event_timestamp`` must be timezone-aware because
    streaming features are computed on event time, not processing time.
    """

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    schema_version: int = Field(default=1, ge=1, description="Event contract version.")
    transaction_id: str = Field(min_length=1, description="Unique transaction identifier.")
    account_id: str = Field(min_length=1, description="Account that initiated the payment.")
    event_timestamp: AwareDatetime = Field(description="Timezone-aware event time.")
    amount: Decimal = Field(gt=0, max_digits=12, decimal_places=2, description="Positive transaction amount.")
    currency: Currency
    merchant_id: str = Field(min_length=1, description="Merchant receiving the payment.")
    merchant_category: MerchantCategory
    device_id: str = Field(min_length=1, description="Device used to initiate the payment.")
    latitude: float = Field(ge=-90, le=90)
    longitude: float = Field(ge=-180, le=180)
    country: Country
    channel: Channel
    transaction_type: TransactionType

    @field_validator("transaction_id", "account_id", "merchant_id", "device_id")
    @classmethod
    def ids_must_not_be_empty(cls, value: str) -> str:
        if not value:
            raise ValueError("ID fields must not be empty")
        return value

    @field_validator("event_timestamp")
    @classmethod
    def timestamp_must_be_aware(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("event_timestamp must be timezone-aware")
        return value

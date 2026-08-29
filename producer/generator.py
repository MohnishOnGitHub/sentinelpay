"""Reusable synthetic transaction generator.

Produces valid ``TransactionEvent`` objects from a seeded population of
accounts, merchants, and devices. Events are realistic enough to support
later fraud-pattern injection, but this module does not label or inject fraud.
"""

from __future__ import annotations

import random
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Sequence, TypeVar

T = TypeVar("T")

from producer.config import GeneratorConfig
from producer.schemas import (
    Channel,
    Country,
    Currency,
    MerchantCategory,
    TransactionEvent,
    TransactionType,
)

# Representative city coordinates keep generated locations geographically
# coherent for later impossible-travel and country-change features.
_COUNTRY_COORDINATES = {
    Country.IN: (12.9716, 77.5946),  # Bengaluru — matches DESIGN.md example
    Country.US: (40.7128, -74.0060),
    Country.GB: (51.5074, -0.1278),
    Country.DE: (52.5200, 13.4050),
    Country.SG: (1.3521, 103.8198),
    Country.AU: (-33.8688, 151.2093),
}

_COUNTRY_CURRENCY = {
    Country.IN: Currency.INR,
    Country.US: Currency.USD,
    Country.GB: Currency.GBP,
    Country.DE: Currency.EUR,
    Country.SG: Currency.SGD,
    Country.AU: Currency.AUD,
}

# Typical ticket sizes by merchant category (min, max) in local currency.
_CATEGORY_AMOUNT_RANGE = {
    MerchantCategory.GROCERY: (8.0, 150.0),
    MerchantCategory.RESTAURANT: (10.0, 120.0),
    MerchantCategory.FUEL: (20.0, 100.0),
    MerchantCategory.ELECTRONICS: (50.0, 2500.0),
    MerchantCategory.TRAVEL: (80.0, 3000.0),
    MerchantCategory.CLOTHING: (15.0, 400.0),
    MerchantCategory.ENTERTAINMENT: (8.0, 200.0),
    MerchantCategory.HEALTH: (15.0, 500.0),
    MerchantCategory.JEWELRY: (100.0, 8000.0),
    MerchantCategory.UTILITIES: (20.0, 300.0),
}

_CHANNEL_WEIGHTS = (
    (Channel.ECOMMERCE, 0.40),
    (Channel.POS, 0.30),
    (Channel.MOBILE, 0.25),
    (Channel.ATM, 0.05),
)

_TYPE_WEIGHTS = (
    (TransactionType.PURCHASE, 0.88),
    (TransactionType.REFUND, 0.05),
    (TransactionType.WITHDRAWAL, 0.04),
    (TransactionType.TRANSFER, 0.03),
)

_BASE_EVENT_TIME = datetime(2026, 1, 1, tzinfo=timezone.utc)


@dataclass(frozen=True)
class _AccountProfile:
    account_id: str
    country: Country
    currency: Currency
    device_ids: tuple[str, ...]
    latitude: float
    longitude: float


@dataclass(frozen=True)
class _MerchantProfile:
    merchant_id: str
    category: MerchantCategory
    country: Country


class TransactionGenerator:
    """Generate valid synthetic transaction events.

    The same ``seed`` always rebuilds the same population and event sequence,
    which keeps tests and later replay experiments reproducible.
    """

    def __init__(self, config: GeneratorConfig | None = None, seed: int | None = None) -> None:
        self.config = config or GeneratorConfig()
        resolved_seed = self.config.seed if seed is None else seed
        self._rng = random.Random(resolved_seed)
        self._txn_seq = 0
        self._clock = _BASE_EVENT_TIME
        self._accounts = self._build_accounts()
        self._merchants = self._build_merchants()

    def generate_one(self) -> TransactionEvent:
        """Return a single valid transaction event and advance generator state."""
        account = self._rng.choice(self._accounts)
        merchant = self._rng.choice(self._merchants)
        channel = _weighted_choice(self._rng, _CHANNEL_WEIGHTS)
        transaction_type = _transaction_type_for_channel(self._rng, channel)
        amount = _amount_for_category(self._rng, merchant.category)
        latitude, longitude = _jittered_coordinates(
            self._rng, account.latitude, account.longitude
        )
        self._clock = self._clock + timedelta(seconds=self._rng.randint(5, 3600))
        self._txn_seq += 1

        return TransactionEvent(
            schema_version=1,
            transaction_id=f"txn_{self._txn_seq:06d}",
            account_id=account.account_id,
            event_timestamp=self._clock,
            amount=amount,
            currency=account.currency,
            merchant_id=merchant.merchant_id,
            merchant_category=merchant.category,
            device_id=self._rng.choice(account.device_ids),
            latitude=latitude,
            longitude=longitude,
            country=account.country,
            channel=channel,
            transaction_type=transaction_type,
        )

    def generate(self, n: int) -> list[TransactionEvent]:
        """Return ``n`` valid transaction events."""
        if n < 0:
            raise ValueError("n must be >= 0")
        return [self.generate_one() for _ in range(n)]

    def _build_accounts(self) -> list[_AccountProfile]:
        device_ids = [f"dev_{i + 1:03d}" for i in range(self.config.num_devices)]
        if not device_ids:
            raise ValueError("num_devices must be >= 1")

        accounts = []
        countries = self._require_sequence(self.config.countries, "countries")
        allowed_currencies = set(self.config.currencies)
        for i in range(self.config.num_accounts):
            country = countries[i % len(countries)]
            currency = _COUNTRY_CURRENCY[country]
            if currency not in allowed_currencies:
                currency = self.config.currencies[0]
            lat, lon = _COUNTRY_COORDINATES[country]
            accounts.append(
                _AccountProfile(
                    account_id=f"acct_{1001 + i:04d}",
                    country=country,
                    currency=currency,
                    device_ids=tuple(_assign_devices(self._rng, device_ids, i)),
                    latitude=lat,
                    longitude=lon,
                )
            )
        return accounts

    def _build_merchants(self) -> list[_MerchantProfile]:
        countries = self._require_sequence(self.config.countries, "countries")
        categories = list(MerchantCategory)
        return [
            _MerchantProfile(
                merchant_id=f"m_{i + 1:03d}",
                category=categories[i % len(categories)],
                country=countries[i % len(countries)],
            )
            for i in range(self.config.num_merchants)
        ]

    @staticmethod
    def _require_sequence(values: Sequence[object], name: str) -> Sequence[object]:
        if not values:
            raise ValueError(f"{name} must not be empty")
        return values


def _assign_devices(rng: random.Random, device_ids: list[str], account_index: int) -> list[str]:
    """Give each account a small, stable device set drawn from the pool."""
    primary = device_ids[account_index % len(device_ids)]
    extra_count = min(rng.randint(0, 2), max(len(device_ids) - 1, 0))
    extras = rng.sample(device_ids, extra_count) if extra_count else []
    unique = []
    for device_id in [primary] + extras:
        if device_id not in unique:
            unique.append(device_id)
    return unique


def _weighted_choice(rng: random.Random, weights: Sequence[tuple[T, float]]) -> T:
    population, probs = zip(*weights)
    return rng.choices(population, weights=probs, k=1)[0]


def _transaction_type_for_channel(rng: random.Random, channel: Channel) -> TransactionType:
    if channel == Channel.ATM:
        return TransactionType.WITHDRAWAL
    return _weighted_choice(rng, _TYPE_WEIGHTS)


def _amount_for_category(rng: random.Random, category: MerchantCategory) -> Decimal:
    low, high = _CATEGORY_AMOUNT_RANGE[category]
    quantized = Decimal(str(round(rng.uniform(low, high), 2)))
    if quantized <= 0:
        return Decimal("0.01")
    return quantized


def _jittered_coordinates(rng: random.Random, latitude: float, longitude: float) -> tuple[float, float]:
    """Add city-scale noise and clamp to valid geographic bounds."""
    jittered_lat = max(-90.0, min(90.0, latitude + rng.uniform(-0.15, 0.15)))
    jittered_lon = max(-180.0, min(180.0, longitude + rng.uniform(-0.15, 0.15)))
    return jittered_lat, jittered_lon

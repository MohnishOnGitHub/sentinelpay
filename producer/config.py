"""Defaults for the synthetic transaction generator."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Tuple

from producer.schemas import Country, Currency

DEFAULT_SEED = 42
DEFAULT_NUM_ACCOUNTS = 100
DEFAULT_NUM_MERCHANTS = 50
DEFAULT_NUM_DEVICES = 150

SUPPORTED_CURRENCIES: Tuple[Currency, ...] = (
    Currency.INR,
    Currency.USD,
    Currency.EUR,
    Currency.GBP,
    Currency.SGD,
    Currency.AUD,
)

SUPPORTED_COUNTRIES: Tuple[Country, ...] = (
    Country.IN,
    Country.US,
    Country.GB,
    Country.DE,
    Country.SG,
    Country.AU,
)


@dataclass(frozen=True)
class GeneratorConfig:
    """Tunable generator settings.

    Pools of accounts, merchants, and devices are built once per generator
    instance so repeated ``generate`` calls draw from a stable population.
    """

    seed: int = DEFAULT_SEED
    num_accounts: int = DEFAULT_NUM_ACCOUNTS
    num_merchants: int = DEFAULT_NUM_MERCHANTS
    num_devices: int = DEFAULT_NUM_DEVICES
    currencies: Tuple[Currency, ...] = field(default=SUPPORTED_CURRENCIES)
    countries: Tuple[Country, ...] = field(default=SUPPORTED_COUNTRIES)

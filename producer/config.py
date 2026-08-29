"""Defaults for the synthetic transaction generator and Kafka producer."""

from __future__ import annotations

import os
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

DEFAULT_KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
DEFAULT_KAFKA_RAW_TOPIC = "transactions.raw"
DEFAULT_KAFKA_VALIDATED_TOPIC = "transactions.validated"
DEFAULT_KAFKA_DLQ_TOPIC = "transactions.dlq"
DEFAULT_KAFKA_VALIDATOR_GROUP = "sentinelpay-validator"
DEFAULT_PRODUCE_RATE = 10.0
DEFAULT_PRODUCE_COUNT = 100


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


@dataclass(frozen=True)
class KafkaConfig:
    """Kafka connection settings loaded from the environment.

    Local defaults match a single-broker development cluster. Override with
    environment variables rather than editing code.
    """

    bootstrap_servers: str = DEFAULT_KAFKA_BOOTSTRAP_SERVERS
    raw_topic: str = DEFAULT_KAFKA_RAW_TOPIC
    validated_topic: str = DEFAULT_KAFKA_VALIDATED_TOPIC
    dlq_topic: str = DEFAULT_KAFKA_DLQ_TOPIC
    validator_group: str = DEFAULT_KAFKA_VALIDATOR_GROUP

    @classmethod
    def from_env(cls) -> KafkaConfig:
        return cls(
            bootstrap_servers=os.environ.get(
                "KAFKA_BOOTSTRAP_SERVERS", DEFAULT_KAFKA_BOOTSTRAP_SERVERS
            ),
            raw_topic=os.environ.get("KAFKA_RAW_TOPIC", DEFAULT_KAFKA_RAW_TOPIC),
            validated_topic=os.environ.get(
                "KAFKA_VALIDATED_TOPIC", DEFAULT_KAFKA_VALIDATED_TOPIC
            ),
            dlq_topic=os.environ.get("KAFKA_DLQ_TOPIC", DEFAULT_KAFKA_DLQ_TOPIC),
            validator_group=os.environ.get(
                "KAFKA_VALIDATOR_GROUP", DEFAULT_KAFKA_VALIDATOR_GROUP
            ),
        )

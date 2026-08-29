"""Unit tests for the Phase 1A synthetic transaction generator."""

from datetime import timezone

from producer import TransactionEvent, TransactionGenerator
from producer.config import DEFAULT_SEED


def test_generated_event_validates_successfully():
    event = TransactionGenerator(seed=DEFAULT_SEED).generate_one()

    revalidated = TransactionEvent.model_validate(event.model_dump())

    assert revalidated == event


def test_deterministic_generation_with_same_seed():
    first = TransactionGenerator(seed=7).generate(20)
    second = TransactionGenerator(seed=7).generate(20)

    assert first == second
    assert TransactionGenerator(seed=8).generate(20) != first


def test_generate_returns_requested_event_count():
    events = TransactionGenerator(seed=DEFAULT_SEED).generate(25)

    assert len(events) == 25


def test_generated_ids_are_non_empty():
    events = TransactionGenerator(seed=DEFAULT_SEED).generate(30)

    for event in events:
        assert event.transaction_id.strip()
        assert event.account_id.strip()
        assert event.merchant_id.strip()
        assert event.device_id.strip()


def test_amount_is_always_positive():
    events = TransactionGenerator(seed=DEFAULT_SEED).generate(50)

    assert all(event.amount > 0 for event in events)


def test_coordinates_remain_within_valid_bounds():
    events = TransactionGenerator(seed=DEFAULT_SEED).generate(50)

    for event in events:
        assert -90 <= event.latitude <= 90
        assert -180 <= event.longitude <= 180


def test_timestamp_is_timezone_aware():
    events = TransactionGenerator(seed=DEFAULT_SEED).generate(20)

    for event in events:
        assert event.event_timestamp.tzinfo is not None
        assert event.event_timestamp.utcoffset() is not None
        assert event.event_timestamp.tzinfo == timezone.utc

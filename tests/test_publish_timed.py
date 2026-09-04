"""Broker-free tests for deterministic lake-smoke event waves."""

from __future__ import annotations

from datetime import datetime, timezone

from producer.publish_timed import events_for_wave


def test_early_wave_is_in_the_1000_utc_window():
    events = events_for_wave("early", "run1")
    assert [event.transaction_id for event in events] == [
        "txn_lake_run1_a",
        "txn_lake_run1_b",
    ]
    assert {event.account_id for event in events} == {"acct_lake_3a"}
    assert events[0].event_timestamp == datetime(2026, 1, 1, 10, 1, tzinfo=timezone.utc)
    assert events[1].event_timestamp == datetime(2026, 1, 1, 10, 3, tzinfo=timezone.utc)


def test_late_wave_advances_past_ten_minute_watermark():
    events = events_for_wave("late", "run1")
    assert events[0].event_timestamp == datetime(2026, 1, 1, 10, 50, tzinfo=timezone.utc)
    assert events[1].event_timestamp == datetime(2026, 1, 1, 10, 52, tzinfo=timezone.utc)

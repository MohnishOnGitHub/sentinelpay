"""Broker-free tests for streaming feature thresholds."""

from __future__ import annotations

from decimal import Decimal

from streaming.config import FeatureConfig, StreamingConfig


def test_feature_config_defaults():
    config = FeatureConfig()
    assert config.high_amount_threshold == Decimal("5000.00")
    assert config.rapid_txn_count_threshold == 5
    assert config.multi_device_threshold == 2
    assert config.location_spread_km_threshold == 25.0
    assert config.location_grid_decimals == 3


def test_feature_config_from_env(monkeypatch):
    monkeypatch.setenv("HIGH_AMOUNT_THRESHOLD", "250.50")
    monkeypatch.setenv("RAPID_TXN_COUNT_THRESHOLD", "3")
    monkeypatch.setenv("MULTI_DEVICE_THRESHOLD", "4")
    monkeypatch.setenv("LOCATION_SPREAD_KM_THRESHOLD", "10.5")
    monkeypatch.setenv("LOCATION_GRID_DECIMALS", "2")

    config = FeatureConfig.from_env()
    assert config.high_amount_threshold == Decimal("250.50")
    assert config.rapid_txn_count_threshold == 3
    assert config.multi_device_threshold == 4
    assert config.location_spread_km_threshold == 10.5
    assert config.location_grid_decimals == 2


def test_streaming_config_loads_feature_thresholds(monkeypatch):
    monkeypatch.setenv("HIGH_AMOUNT_THRESHOLD", "99.00")
    config = StreamingConfig.from_env()
    assert config.features.high_amount_threshold == Decimal("99.00")

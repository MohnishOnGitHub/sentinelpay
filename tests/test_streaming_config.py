"""Broker-free tests for streaming feature thresholds."""

from __future__ import annotations

from decimal import Decimal

from streaming.config import DataLakeConfig, FeatureConfig, StreamingConfig
from streaming.sinks import GOLD_PARTITION_COLUMNS, SILVER_PARTITION_COLUMNS


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


def test_data_lake_config_defaults():
    lake = DataLakeConfig()
    assert lake.data_lake_dir == "data"
    assert lake.silver_transactions_path == "data/silver/transactions"
    assert lake.gold_features_path == "data/gold/account_features"
    assert lake.silver_checkpoint_dir == ".checkpoints/silver-transactions"
    assert lake.gold_checkpoint_dir == ".checkpoints/gold-account-features"
    assert lake.console_checkpoint_dir == ".checkpoints/console-features"
    assert lake.console_sink is False
    assert lake.silver_checkpoint_dir != lake.gold_checkpoint_dir


def test_data_lake_config_derives_paths_from_lake_dir(monkeypatch):
    monkeypatch.setenv("DATA_LAKE_DIR", "/tmp/sentinelpay-lake")
    monkeypatch.delenv("SILVER_TRANSACTIONS_PATH", raising=False)
    monkeypatch.delenv("GOLD_FEATURES_PATH", raising=False)
    lake = DataLakeConfig.from_env()
    assert lake.silver_transactions_path == "/tmp/sentinelpay-lake/silver/transactions"
    assert lake.gold_features_path == "/tmp/sentinelpay-lake/gold/account_features"


def test_data_lake_config_env_overrides(monkeypatch):
    monkeypatch.setenv("DATA_LAKE_DIR", "/tmp/ignored-lake")
    monkeypatch.setenv("SILVER_TRANSACTIONS_PATH", "/custom/silver")
    monkeypatch.setenv("GOLD_FEATURES_PATH", "/custom/gold")
    monkeypatch.setenv("SILVER_CHECKPOINT_DIR", "/ck/silver")
    monkeypatch.setenv("GOLD_CHECKPOINT_DIR", "/ck/gold")
    monkeypatch.setenv("SPARK_CHECKPOINT_DIR", "/ck/console")
    monkeypatch.setenv("SPARK_CONSOLE_SINK", "true")

    lake = DataLakeConfig.from_env()
    assert lake.silver_transactions_path == "/custom/silver"
    assert lake.gold_features_path == "/custom/gold"
    assert lake.silver_checkpoint_dir == "/ck/silver"
    assert lake.gold_checkpoint_dir == "/ck/gold"
    assert lake.console_checkpoint_dir == "/ck/console"
    assert lake.console_sink is True


def test_lake_partitions_are_event_time_not_entity_ids():
    assert SILVER_PARTITION_COLUMNS == ("event_date", "event_hour")
    assert GOLD_PARTITION_COLUMNS == ("window_date",)
    for column in ("account_id", "transaction_id", "merchant_id", "device_id"):
        assert column not in SILVER_PARTITION_COLUMNS
        assert column not in GOLD_PARTITION_COLUMNS


def test_streaming_config_exposes_lake(monkeypatch):
    monkeypatch.setenv("SILVER_CHECKPOINT_DIR", ".checkpoints/override-silver")
    config = StreamingConfig.from_env()
    assert config.lake.silver_checkpoint_dir == ".checkpoints/override-silver"
    assert config.checkpoint_dir == config.lake.console_checkpoint_dir

"""Local Spark Structured Streaming job for validated transactions.

Example:

    python -m streaming.main

Requires a JDK (11 or 17) on PATH / JAVA_HOME. Runs in local[*] mode.
Writes Silver transactions and Gold account features as local Parquet.
Set SPARK_CONSOLE_SINK=true to also print update-mode window rows.
"""

from __future__ import annotations

import argparse
import shutil
import subprocess
import sys

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from streaming.config import FeatureConfig, StreamingConfig
from streaming.features import account_window_features, prepare_events
from streaming.schema import parse_validated_json
from streaming.session import create_spark_session
from streaming.sinks import (
    start_console_query,
    start_gold_parquet_query,
    start_silver_parquet_query,
)


def require_java() -> None:
    if shutil.which("java") is None:
        raise RuntimeError(
            "Java is required for PySpark. Install a JDK 11 or 17 and set JAVA_HOME, "
            "then retry `python -m streaming.main`."
        )
    try:
        output = subprocess.check_output(["java", "-version"], stderr=subprocess.STDOUT)
        text = output.decode("utf-8", "replace")
    except subprocess.CalledProcessError as exc:
        text = (exc.output or b"").decode("utf-8", "replace")
        if "Unable to locate a Java Runtime" in text or "No Java runtime" in text:
            raise RuntimeError(
                "macOS java stub found, but no JDK is installed. "
                "Install Temurin 17 or OpenJDK 17 and set JAVA_HOME."
            ) from exc
        raise RuntimeError("Java is installed but could not be executed.") from exc
    except OSError as exc:
        raise RuntimeError("Java is installed but could not be executed.") from exc
    if "Unable to locate a Java Runtime" in text or "No Java runtime" in text:
        raise RuntimeError(
            "macOS java stub found, but no JDK is installed. "
            "Install Temurin 17 or OpenJDK 17 and set JAVA_HOME."
        )


def build_spark_session(app_name: str = "sentinelpay-streaming") -> SparkSession:
    spark_version = pyspark.__version__
    kafka_package = f"org.apache.spark:spark-sql-kafka-0-10_2.12:{spark_version}"
    return create_spark_session(
        app_name,
        master="local[*]",
        extra_configs={
            "spark.jars.packages": kafka_package,
            "spark.sql.shuffle.partitions": "4",
            "spark.ui.enabled": "false",
        },
    )


def read_validated_stream(spark: SparkSession, config: StreamingConfig):
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", config.bootstrap_servers)
        .option("subscribe", config.validated_topic)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
        .select(F.col("value"))
    )


def build_prepared_events(raw_value_df, watermark: str):
    """Parse Kafka JSON, apply UTC event-time watermark, and dedup."""
    return prepare_events(parse_validated_json(raw_value_df), watermark=watermark)


def build_feature_stream(
    raw_value_df, watermark: str, feature_config: FeatureConfig | None = None
):
    return account_window_features(build_prepared_events(raw_value_df, watermark), feature_config)


def _stop_queries(queries) -> None:
    for query in queries:
        if query.isActive:
            query.stop()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Stream validated transactions to a local Parquet data lake."
    )
    parser.add_argument(
        "--timeout-seconds",
        type=int,
        default=0,
        help="Stop after N seconds. 0 means run until interrupted.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        require_java()
    except RuntimeError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2

    config = StreamingConfig.from_env()
    lake = config.lake
    spark = build_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    print(
        f"Streaming {config.validated_topic} @ {config.bootstrap_servers} "
        f"(watermark={config.watermark}, "
        f"silver={lake.silver_transactions_path}, gold={lake.gold_features_path}, "
        f"console={str(lake.console_sink).lower()})",
        flush=True,
    )

    raw = read_validated_stream(spark, config)
    prepared = build_prepared_events(raw, config.watermark)
    features = account_window_features(prepared, config.features)

    queries = [
        start_silver_parquet_query(
            prepared,
            lake.silver_transactions_path,
            lake.silver_checkpoint_dir,
            config.trigger_seconds,
        ),
        start_gold_parquet_query(
            features,
            lake.gold_features_path,
            lake.gold_checkpoint_dir,
            config.trigger_seconds,
        ),
    ]
    if lake.console_sink:
        queries.append(
            start_console_query(features, config.checkpoint_dir, config.trigger_seconds)
        )

    try:
        if args.timeout_seconds > 0:
            spark.streams.awaitAnyTermination(args.timeout_seconds)
            _stop_queries(queries)
        else:
            spark.streams.awaitAnyTermination()
    except KeyboardInterrupt:
        print("streaming stopped", file=sys.stderr)
        _stop_queries(queries)
    finally:
        _stop_queries(queries)
        spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())

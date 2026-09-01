"""Local Spark Structured Streaming job for validated transactions.

Example:

    python -m streaming.main

Requires a JDK (11 or 17) on PATH / JAVA_HOME. Runs in local[*] mode.
"""

from __future__ import annotations

import argparse
import shutil
import subprocess
import sys
from pathlib import Path

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from streaming.config import StreamingConfig
from streaming.features import account_window_features, prepare_events
from streaming.schema import parse_validated_json
from streaming.session import create_spark_session


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


def build_feature_stream(raw_value_df, watermark: str):
    events = parse_validated_json(raw_value_df)
    prepared = prepare_events(events, watermark=watermark)
    return account_window_features(prepared)


def start_console_query(features, checkpoint_dir: str, trigger_seconds: int):
    Path(checkpoint_dir).mkdir(parents=True, exist_ok=True)
    return (
        features.writeStream.outputMode("update")
        .format("console")
        .option("truncate", "false")
        .option("checkpointLocation", checkpoint_dir)
        .trigger(processingTime=f"{trigger_seconds} seconds")
        .start()
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Stream validated transactions and print account window features."
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
    spark = build_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    print(
        f"Streaming {config.validated_topic} @ {config.bootstrap_servers} "
        f"(watermark={config.watermark}, checkpoint={config.checkpoint_dir})",
        flush=True,
    )

    raw = read_validated_stream(spark, config)
    features = build_feature_stream(raw, config.watermark)
    query = start_console_query(features, config.checkpoint_dir, config.trigger_seconds)
    try:
        if args.timeout_seconds > 0:
            query.awaitTermination(args.timeout_seconds)
            query.stop()
        else:
            query.awaitTermination()
    except KeyboardInterrupt:
        print("streaming stopped", file=sys.stderr)
        query.stop()
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())

"""Read local Silver/Gold Parquet and print counts plus a few rows.

Used by the streaming smoke test to prove files contain real data, not
just empty directories.

    python -m streaming.inspect_lake
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from streaming.config import DataLakeConfig
from streaming.session import create_spark_session


def _parquet_ready(path: str) -> bool:
    root = Path(path)
    if not root.exists():
        return False
    return any(root.rglob("*.parquet"))


def _print_frame(label: str, frame) -> int:
    count = frame.count()
    print(f"{label}: {count} row(s)", flush=True)
    if count == 0:
        return 0
    frame.show(5, truncate=False)
    return count


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Inspect local SentinelPay Parquet lake files.")
    parser.add_argument(
        "--require-gold",
        action="store_true",
        help="Exit non-zero when Gold has no parquet rows (Silver is always required).",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    lake = DataLakeConfig.from_env()
    if not _parquet_ready(lake.silver_transactions_path):
        print(
            f"error: no Silver parquet under {lake.silver_transactions_path}",
            file=sys.stderr,
        )
        return 2

    spark = create_spark_session(
        "sentinelpay-inspect-lake",
        master="local[1]",
        extra_configs={"spark.ui.enabled": "false", "spark.sql.shuffle.partitions": "1"},
    )
    spark.sparkContext.setLogLevel("ERROR")
    try:
        silver = spark.read.parquet(lake.silver_transactions_path)
        print(f"Silver path={lake.silver_transactions_path}", flush=True)
        print("Silver schema:", flush=True)
        silver.printSchema()
        silver_count = _print_frame("Silver", silver)

        gold_count = 0
        if _parquet_ready(lake.gold_features_path):
            gold = spark.read.parquet(lake.gold_features_path)
            print(f"Gold path={lake.gold_features_path}", flush=True)
            print("Gold schema:", flush=True)
            gold.printSchema()
            gold_count = _print_frame("Gold", gold)
        else:
            print(
                f"Gold path={lake.gold_features_path}: no parquet yet "
                "(append-mode windows emit only after the watermark passes window_end)",
                flush=True,
            )
    finally:
        spark.stop()

    if silver_count == 0:
        print("error: Silver parquet exists but has 0 rows", file=sys.stderr)
        return 2
    if args.require_gold and gold_count == 0:
        print("error: Gold parquet has 0 rows", file=sys.stderr)
        return 3
    return 0


if __name__ == "__main__":
    sys.exit(main())

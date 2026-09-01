"""Spark StructType for the existing TransactionEvent JSON contract.

This is not a second event model. It is the same Phase 1A / DESIGN.md v1
fields expressed as a Spark schema so Kafka JSON can be parsed in-cluster.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DecimalType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)


def transaction_event_schema() -> StructType:
    """Return the Spark schema that matches ``TransactionEvent`` JSON.

    ``amount`` and ``event_timestamp`` are strings on the wire (Pydantic JSON
    serializers). Parsing to decimal/timestamp happens after ``from_json``.
    """
    return StructType(
        [
            StructField("schema_version", IntegerType(), False),
            StructField("transaction_id", StringType(), False),
            StructField("account_id", StringType(), False),
            StructField("event_timestamp", StringType(), False),
            StructField("amount", StringType(), False),
            StructField("currency", StringType(), False),
            StructField("merchant_id", StringType(), False),
            StructField("merchant_category", StringType(), False),
            StructField("device_id", StringType(), False),
            StructField("latitude", DoubleType(), False),
            StructField("longitude", DoubleType(), False),
            StructField("country", StringType(), False),
            StructField("channel", StringType(), False),
            StructField("transaction_type", StringType(), False),
        ]
    )


TRANSACTION_EVENT_FIELD_NAMES = tuple(field.name for field in transaction_event_schema().fields)


def parse_validated_json(frame: DataFrame) -> DataFrame:
    """Parse Kafka ``value`` JSON into typed TransactionEvent columns.

    Accepts a DataFrame with a binary or string ``value`` column, matching
    the Kafka source layout. ``event_timestamp`` becomes a timestamp used as
    Spark event time; ``amount`` becomes ``DECIMAL(12,2)``.
    """
    json_col = F.col("value")
    if dict(frame.dtypes).get("value") == "binary":
        json_col = json_col.cast("string")

    parsed = F.from_json(json_col, transaction_event_schema())
    return (
        frame.select(parsed.alias("event"))
        .select("event.*")
        .withColumn(
            "event_timestamp",
            F.to_timestamp(
                F.regexp_replace(F.col("event_timestamp"), "Z$", "+00:00"),
                "yyyy-MM-dd'T'HH:mm:ss.SSSXXX",
            ),
        )
        .withColumn("amount", F.col("amount").cast(DecimalType(12, 2)))
    )

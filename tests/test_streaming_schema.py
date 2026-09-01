"""Spark schema tests that do not start a SparkSession or Kafka."""

from __future__ import annotations

from producer.schemas import TransactionEvent
from streaming.schema import TRANSACTION_EVENT_FIELD_NAMES, transaction_event_schema


def test_spark_schema_fields_match_transaction_event_contract():
    spark_fields = set(TRANSACTION_EVENT_FIELD_NAMES)
    pydantic_fields = set(TransactionEvent.model_fields)

    assert spark_fields == pydantic_fields
    assert transaction_event_schema().fieldNames() == list(TRANSACTION_EVENT_FIELD_NAMES)

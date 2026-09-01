"""Shared SparkSession factory with UTC event-time semantics.

Spark timestamps are timezone-less instants. If the session or JVM default
timezone is Asia/Kolkata, ISO-8601 ``Z`` values are displayed and windowed
as local time (UTC+05:30). Production and tests must share this factory so
both parse, window, and ``collect()`` in UTC.
"""

from __future__ import annotations

import os
import time
from typing import Mapping, Optional

from pyspark.sql import SparkSession

UTC_TIMEZONE = "UTC"


def configure_process_utc() -> None:
    """Make the Python process treat naive conversions as UTC."""
    os.environ["TZ"] = UTC_TIMEZONE
    if hasattr(time, "tzset"):
        time.tzset()


def apply_spark_utc(session: SparkSession) -> SparkSession:
    """Force an existing session and its JVM onto UTC."""
    session.conf.set("spark.sql.session.timeZone", UTC_TIMEZONE)
    jvm = session._jvm
    jvm.java.util.TimeZone.setDefault(jvm.java.util.TimeZone.getTimeZone(UTC_TIMEZONE))
    return session


def create_spark_session(
    app_name: str,
    master: str = "local[*]",
    extra_configs: Optional[Mapping[str, str]] = None,
) -> SparkSession:
    """Create or reuse a SparkSession configured for UTC event-time processing."""
    configure_process_utc()

    existing = SparkSession.getActiveSession()
    if existing is not None and existing.conf.get("spark.sql.session.timeZone") != UTC_TIMEZONE:
        existing.stop()

    builder = (
        SparkSession.builder.appName(app_name)
        .master(master)
        .config("spark.sql.session.timeZone", UTC_TIMEZONE)
        .config("spark.driver.extraJavaOptions", "-Duser.timezone=UTC")
        .config("spark.executor.extraJavaOptions", "-Duser.timezone=UTC")
    )
    for key, value in (extra_configs or {}).items():
        builder = builder.config(key, value)

    session = builder.getOrCreate()
    return apply_spark_utc(session)

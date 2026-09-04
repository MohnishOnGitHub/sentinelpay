"""Shared pytest fixtures. The Spark session is session-scoped and skipped without a JDK."""

from __future__ import annotations

import shutil
import subprocess

import pytest

from streaming.session import create_spark_session


def java_runtime_available() -> bool:
    java = shutil.which("java")
    if java is None:
        return False
    try:
        output = subprocess.check_output([java, "-version"], stderr=subprocess.STDOUT)
        text = output.decode("utf-8", "replace")
    except (OSError, subprocess.CalledProcessError) as exc:
        text = (getattr(exc, "output", b"") or b"").decode("utf-8", "replace")
    return "version" in text.lower() and "Unable to locate a Java Runtime" not in text


@pytest.fixture(scope="session")
def spark():
    if not java_runtime_available():
        pytest.skip("JDK 11/17 is required for local Spark tests")
    pytest.importorskip("pyspark")

    try:
        session = create_spark_session(
            "sentinelpay-streaming-tests",
            master="local[1]",
            extra_configs={
                "spark.ui.enabled": "false",
                "spark.sql.shuffle.partitions": "1",
            },
        )
        session.sparkContext.setLogLevel("ERROR")
    except Exception as exc:
        pytest.skip(f"local SparkSession is unavailable: {exc}")
    yield session
    session.stop()

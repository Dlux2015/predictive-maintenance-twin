"""
Shared pytest fixtures for the Predictive Maintenance Digital Twin test suite.

Spark tests use a local[1] session without Delta Lake extensions so they run
without a Hadoop/Databricks environment. Tests that exercise pure-Python code
(ingestion, config parsing) have no Spark dependency and run anywhere.

Skip all Spark-dependent tests gracefully when PySpark is not installed:
    pytest -m "not spark"
"""

from __future__ import annotations

import os

import pytest

# ---------------------------------------------------------------------------
# Silence noisy PySpark/log4j output during tests
# ---------------------------------------------------------------------------
os.environ.setdefault("PYSPARK_SUBMIT_ARGS", "--master local[1] pyspark-shell")
os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")

# Suppress py4j / log4j noise
import logging
logging.getLogger("py4j").setLevel(logging.ERROR)
logging.getLogger("pyspark").setLevel(logging.ERROR)


# ---------------------------------------------------------------------------
# SparkSession fixture
# ---------------------------------------------------------------------------

pyspark = pytest.importorskip("pyspark", reason="PySpark not installed — skipping Spark tests")


@pytest.fixture(scope="session")
def spark():
    """
    Session-scoped local SparkSession for unit tests.

    Uses a single shuffle partition and disables the Spark UI to keep tests
    fast. Does NOT load Delta Lake extensions — transformation tests work on
    plain DataFrames only.
    """
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.master("local[1]")
        .appName("pmt-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.driver.host", "127.0.0.1")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()

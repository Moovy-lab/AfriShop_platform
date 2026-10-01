"""Shared Spark fixture for curated job tests."""

from __future__ import annotations

import os

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    """Create one local Spark session for the test run."""
    from spark_session import get_spark

    os.environ.setdefault("SPARK_SHUFFLE_PARTITIONS", "2")
    session = get_spark("afrishop-curated-tests", cluster=False)
    yield session
    session.stop()

from collections.abc import Generator

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark_session() -> Generator[SparkSession, None, None]:
    spark = (
        SparkSession.builder.master("local[1]")
        .appName("spark-feature-engine-pytest")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.ui.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.default.parallelism", "1")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    try:
        yield spark
    finally:
        spark.stop()
        if hasattr(SparkSession, "clearActiveSession"):
            SparkSession.clearActiveSession()
        if hasattr(SparkSession, "clearDefaultSession"):
            SparkSession.clearDefaultSession()

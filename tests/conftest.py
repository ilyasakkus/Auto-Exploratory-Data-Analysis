"""Pytest configuration and PySpark test fixtures."""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    LongType,
    DoubleType,
    StringType,
    BooleanType,
)


@pytest.fixture(scope="session")
def spark():
    """Create a lightweight local SparkSession for unit tests."""
    session = (
        SparkSession.builder.master("local[2]")
        .appName("SparkAutoEDA-Tests")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture
def sample_df(spark):
    """Create a well-defined test DataFrame with known metrics."""
    data = [
        (1, 10.0, "CategoryA", True),
        (2, 20.0, "CategoryB", False),
        (3, 30.0, "CategoryA", True),
        (4, 40.0, "CategoryC", True),
        (5, 50.0, "CategoryB", False),
        (6, None, "CategoryA", None),  # nulls
        (7, 70.0, None, True),          # null category
        (8, 80.0, "CategoryA", False),
        (9, 90.0, "CategoryA", True),
        (10, 1000.0, "CategoryB", False), # intentional outlier
    ]
    schema = StructType([
        StructField("id", LongType(), False),
        StructField("score", DoubleType(), True),
        StructField("category", StringType(), True),
        StructField("is_active", BooleanType(), True),
    ])
    return spark.createDataFrame(data, schema)

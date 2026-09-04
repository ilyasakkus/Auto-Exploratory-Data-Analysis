"""Tests for numerical distributions and histograms."""

from spark_auto_eda.config import EDAConfig
from spark_auto_eda.core.numerical import (
    compute_distributed_histogram,
    compute_numerical_outliers,
    profile_all_numerical_columns,
)


def test_compute_distributed_histogram(sample_df):
    hist = compute_distributed_histogram(
        df=sample_df,
        col_name="score",
        min_val=10.0,
        max_val=1000.0,
        num_bins=5,
    )
    assert len(hist["counts"]) == 5
    assert len(hist["bin_edges"]) == 6
    # Total counted non-null values must equal 9
    assert sum(hist["counts"]) == 9


def test_compute_numerical_outliers(sample_df):
    # For scores [10, 20, 30, 40, 50, 70, 80, 90, 1000], 1000 is an outlier
    outliers = compute_numerical_outliers(
        df=sample_df,
        col_name="score",
        q1=30.0,
        q3=80.0,
        iqr_multiplier=1.5,
    )
    assert outliers["outlier_count"] >= 1
    assert outliers["upper_bound"] == 80.0 + (1.5 * 50.0) # 155.0

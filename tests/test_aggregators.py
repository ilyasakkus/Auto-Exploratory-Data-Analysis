"""Tests for single-pass aggregations and overview metrics."""

from spark_auto_eda.config import EDAConfig
from spark_auto_eda.core.aggregators import (
    classify_columns,
    compute_dataset_overview,
    build_single_pass_aggregations,
)


def test_classify_columns(sample_df):
    classes = classify_columns(sample_df)
    assert "id" in classes["numerical"]
    assert "score" in classes["numerical"]
    assert "category" in classes["categorical"]
    assert "is_active" in classes["boolean"]


def test_compute_dataset_overview(sample_df):
    config = EDAConfig()
    classes = classify_columns(sample_df)
    overview = compute_dataset_overview(sample_df, config, classes)

    assert overview["total_rows"] == 10
    assert overview["total_columns"] == 4
    assert overview["duplicate_rows"] == 0
    assert overview["numerical_columns"] == 2
    assert overview["categorical_columns"] == 1
    assert overview["boolean_columns"] == 1


def test_build_single_pass_aggregations(sample_df):
    config = EDAConfig()
    classes = classify_columns(sample_df)
    stats = build_single_pass_aggregations(sample_df, config, classes)

    assert "score" in stats
    score_stat = stats["score"]
    # 1 null out of 10 rows = 10%
    assert score_stat["null_count"] == 1
    assert score_stat["missing_percentage"] == 10.0
    assert score_stat["min"] == 10.0
    assert score_stat["max"] == 1000.0

    category_stat = stats["category"]
    assert category_stat["null_count"] == 1
    assert category_stat["missing_percentage"] == 10.0

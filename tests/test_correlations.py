"""Tests for correlation matrix computation."""

from spark_auto_eda.config import EDAConfig
from spark_auto_eda.core.correlations import compute_correlation_matrix


def test_compute_correlation_matrix(sample_df):
    config = EDAConfig(compute_correlations=True, collinearity_threshold=0.8)
    corr_res = compute_correlation_matrix(
        df=sample_df,
        numerical_columns=["id", "score"],
        config=config,
    )
    assert len(corr_res["matrix"]) == 2
    # Self-correlation must be 1.0
    assert corr_res["matrix"][0][0] == 1.0
    assert corr_res["matrix"][1][1] == 1.0

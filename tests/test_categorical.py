"""Tests for categorical profiling."""

from spark_auto_eda.core.categorical import profile_categorical_column


def test_profile_categorical_column(sample_df):
    cat_profile = profile_categorical_column(
        df=sample_df,
        col_name="category",
        total_rows=10,
        top_k=5,
    )
    # CategoryA appears 5 times, CategoryB appears 3 times, CategoryC appears 1 time, 1 is None
    top_vals = {v["value"]: v["count"] for v in cat_profile["top_values"]}
    assert top_vals["CategoryA"] == 5
    assert top_vals["CategoryB"] == 3
    assert top_vals["CategoryC"] == 1
    assert cat_profile["min_length"] == 9  # len('CategoryA')
    assert cat_profile["max_length"] == 9

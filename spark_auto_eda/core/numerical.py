"""Numerical column profiling and distributed histogram generation."""

from typing import Dict, Any, List
import math
from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from spark_auto_eda.config import EDAConfig


def compute_distributed_histogram(
    df: DataFrame,
    col_name: str,
    min_val: float,
    max_val: float,
    num_bins: int = 20,
) -> Dict[str, Any]:
    """Calculate histogram bin edges and frequencies distributedly using Spark SQL expressions.

    Returns:
        Dict with 'bin_edges', 'counts', and 'labels' without collecting raw rows.
    """
    if min_val is None or max_val is None or math.isnan(min_val) or math.isnan(max_val):
        return {"bin_edges": [], "counts": [], "labels": []}

    if min_val == max_val:
        # Constant column or single value
        non_null_count = df.filter(F.col(f"`{col_name}`").isNotNull()).count()
        return {
            "bin_edges": [min_val, max_val],
            "counts": [non_null_count],
            "labels": [f"{min_val:.2f}"],
        }

    bin_width = (max_val - min_val) / num_bins
    safe_col = f"`{col_name}`"

    # Compute bin index: floor((col - min) / bin_width)
    # Clip at num_bins - 1 for the maximum value edge
    bin_idx_col = F.least(
        F.greatest(
            F.floor((F.col(safe_col) - F.lit(min_val)) / F.lit(bin_width)).cast("int"),
            F.lit(0),
        ),
        F.lit(num_bins - 1),
    )

    bin_counts = (
        df.filter(F.col(safe_col).isNotNull() & (~F.isnan(F.col(safe_col))))
        .groupBy(bin_idx_col.alias("bin"))
        .count()
        .collect()
    )

    count_map = {row["bin"]: row["count"] for row in bin_counts if row["bin"] is not None}

    bin_edges = [min_val + i * bin_width for i in range(num_bins + 1)]
    counts = [count_map.get(i, 0) for i in range(num_bins)]
    labels = [
        f"{bin_edges[i]:.2f} - {bin_edges[i+1]:.2f}"
        for i in range(num_bins)
    ]

    return {
        "bin_edges": [round(b, 4) for b in bin_edges],
        "counts": counts,
        "labels": labels,
    }


def compute_numerical_outliers(
    df: DataFrame,
    col_name: str,
    q1: float,
    q3: float,
    iqr_multiplier: float = 1.5,
) -> Dict[str, Any]:
    """Calculate outlier bounds using Tukey's IQR method and count outliers distributedly."""
    if q1 is None or q3 is None or math.isnan(q1) or math.isnan(q3):
        return {
            "outlier_count": 0,
            "outlier_percentage": 0.0,
            "lower_bound": None,
            "upper_bound": None,
        }

    iqr = q3 - q1
    lower_bound = q1 - (iqr_multiplier * iqr)
    upper_bound = q3 + (iqr_multiplier * iqr)
    safe_col = f"`{col_name}`"

    outlier_count = df.filter(
        (F.col(safe_col) < lower_bound) | (F.col(safe_col) > upper_bound)
    ).count()

    total_valid = df.filter(F.col(safe_col).isNotNull()).count()
    outlier_pct = (outlier_count / total_valid * 100.0) if total_valid > 0 else 0.0

    return {
        "outlier_count": outlier_count,
        "outlier_percentage": round(outlier_pct, 2),
        "lower_bound": round(lower_bound, 4),
        "upper_bound": round(upper_bound, 4),
    }


def profile_all_numerical_columns(
    df: DataFrame,
    column_stats: Dict[str, Dict[str, Any]],
    numerical_columns: List[str],
    config: EDAConfig,
) -> Dict[str, Dict[str, Any]]:
    """Enrich single-pass numerical stats with histograms and outlier analysis."""
    for col_name in numerical_columns:
        stats = column_stats.get(col_name, {})
        min_v = stats.get("min")
        max_v = stats.get("max")
        q1 = stats.get("q1")
        q3 = stats.get("q3")

        # Distributed Histogram
        histogram_data = compute_distributed_histogram(
            df=df,
            col_name=col_name,
            min_val=min_v,
            max_val=max_v,
            num_bins=config.histogram_bins,
        )
        stats["histogram"] = histogram_data

        # Outlier counts
        outlier_data = compute_numerical_outliers(
            df=df,
            col_name=col_name,
            q1=q1,
            q3=q3,
            iqr_multiplier=config.outlier_iqr_multiplier,
        )
        stats.update(outlier_data)

    return column_stats

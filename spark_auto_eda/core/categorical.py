"""Categorical column profiling (frequencies, cardinality, text length statistics)."""

from typing import Dict, Any, List
from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from spark_auto_eda.config import EDAConfig


def profile_categorical_column(
    df: DataFrame,
    col_name: str,
    total_rows: int,
    top_k: int = 10,
) -> Dict[str, Any]:
    """Compute top-K frequent categories and string length metrics."""
    safe_col = f"`{col_name}`"

    # Top-K frequent values
    top_rows = (
        df.filter(F.col(safe_col).isNotNull())
        .groupBy(safe_col)
        .count()
        .orderBy(F.col("count").desc())
        .limit(top_k)
        .collect()
    )

    top_values = []
    top_counts_sum = 0
    for row in top_rows:
        val = row[0]
        cnt = row["count"]
        top_counts_sum += cnt
        pct = (cnt / total_rows * 100.0) if total_rows > 0 else 0.0
        top_values.append({
            "value": str(val) if val is not None else "[NULL]",
            "count": cnt,
            "percentage": round(pct, 2),
        })

    other_count = max(0, total_rows - top_counts_sum)
    other_pct = (other_count / total_rows * 100.0) if total_rows > 0 else 0.0

    # String length statistics (min, max, avg)
    length_col = F.length(F.col(safe_col))
    length_stats = (
        df.filter(F.col(safe_col).isNotNull())
        .agg(
            F.min(length_col).alias("min_len"),
            F.max(length_col).alias("max_len"),
            F.mean(length_col).alias("avg_len"),
        )
        .collect()
    )

    min_len = 0
    max_len = 0
    avg_len = 0.0
    if length_stats and length_stats[0]:
        min_len = length_stats[0]["min_len"] or 0
        max_len = length_stats[0]["max_len"] or 0
        avg_len = round(length_stats[0]["avg_len"] or 0.0, 2)

    return {
        "top_values": top_values,
        "other_count": other_count,
        "other_percentage": round(other_pct, 2),
        "min_length": min_len,
        "max_length": max_len,
        "avg_length": avg_len,
    }


def profile_all_categorical_columns(
    df: DataFrame,
    column_stats: Dict[str, Dict[str, Any]],
    categorical_columns: List[str],
    boolean_columns: List[str],
    total_rows: int,
    config: EDAConfig,
) -> Dict[str, Dict[str, Any]]:
    """Enrich single-pass categorical and boolean stats with frequency distributions."""
    # Process string categoricals
    for col_name in categorical_columns:
        cat_info = profile_categorical_column(
            df=df,
            col_name=col_name,
            total_rows=total_rows,
            top_k=config.top_k_categories,
        )
        column_stats.setdefault(col_name, {}).update(cat_info)

    # Process boolean columns
    for col_name in boolean_columns:
        safe_col = f"`{col_name}`"
        bool_counts = (
            df.filter(F.col(safe_col).isNotNull())
            .groupBy(safe_col)
            .count()
            .collect()
        )
        top_values = []
        for row in bool_counts:
            val = str(row[0])
            cnt = row["count"]
            pct = (cnt / total_rows * 100.0) if total_rows > 0 else 0.0
            top_values.append({
                "value": val,
                "count": cnt,
                "percentage": round(pct, 2),
            })
        column_stats.setdefault(col_name, {}).update({
            "top_values": top_values,
            "min_length": 4 if any(v["value"] == "True" for v in top_values) else 5,
            "max_length": 5,
            "avg_length": 4.5,
        })

    return column_stats

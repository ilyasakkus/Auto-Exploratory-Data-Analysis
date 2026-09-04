"""Datetime and timestamp column profiling (ranges, span, temporal distributions)."""

from typing import Dict, Any, List
from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from spark_auto_eda.config import EDAConfig


def profile_datetime_column(
    df: DataFrame,
    col_name: str,
    total_rows: int,
) -> Dict[str, Any]:
    """Compute temporal range, span, and day-of-week/monthly distribution."""
    safe_col = f"`{col_name}`"

    valid_df = df.filter(F.col(safe_col).isNotNull())

    # Day of week distribution (1=Sunday, 7=Saturday in Spark standard)
    dow_counts = (
        valid_df.groupBy(F.dayofweek(F.col(safe_col)).alias("dow"))
        .count()
        .collect()
    )
    dow_names = {1: "Sun", 2: "Mon", 3: "Tue", 4: "Wed", 5: "Thu", 6: "Fri", 7: "Sat"}
    dow_map = {row["dow"]: row["count"] for row in dow_counts if row["dow"] is not None}
    dow_distribution = [
        {"day": dow_names.get(i, f"D{i}"), "count": dow_map.get(i, 0)}
        for i in range(1, 8)
    ]

    # Month distribution
    month_counts = (
        valid_df.groupBy(F.month(F.col(safe_col)).alias("month"))
        .count()
        .collect()
    )
    month_names = {
        1: "Jan", 2: "Feb", 3: "Mar", 4: "Apr", 5: "May", 6: "Jun",
        7: "Jul", 8: "Aug", 9: "Sep", 10: "Oct", 11: "Nov", 12: "Dec"
    }
    month_map = {row["month"]: row["count"] for row in month_counts if row["month"] is not None}
    monthly_distribution = [
        {"month": month_names.get(i, f"M{i}"), "count": month_map.get(i, 0)}
        for i in range(1, 13)
    ]

    return {
        "dow_distribution": dow_distribution,
        "monthly_distribution": monthly_distribution,
    }


def profile_all_datetime_columns(
    df: DataFrame,
    column_stats: Dict[str, Dict[str, Any]],
    datetime_columns: List[str],
    total_rows: int,
    config: EDAConfig,
) -> Dict[str, Dict[str, Any]]:
    """Enrich datetime columns with distributions."""
    for col_name in datetime_columns:
        dt_info = profile_datetime_column(df, col_name, total_rows)
        column_stats.setdefault(col_name, {}).update(dt_info)

    return column_stats

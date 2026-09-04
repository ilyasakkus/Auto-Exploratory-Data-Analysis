"""Single-pass distributed metric aggregation engine for PySpark DataFrames."""

from typing import Dict, Any, List, Tuple
from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from pyspark.sql.types import (
    NumericType,
    FloatType,
    DoubleType,
    StringType,
    BooleanType,
    TimestampType,
    DateType,
)

from spark_auto_eda.config import EDAConfig


def classify_columns(df: DataFrame, exclude_columns: List[str] = None) -> Dict[str, List[str]]:
    """Categorize DataFrame columns by data type family."""
    exclude = set(exclude_columns or [])
    categories = {
        "numerical": [],
        "categorical": [],
        "datetime": [],
        "boolean": [],
        "unsupported": [],
    }

    for field in df.schema.fields:
        name = field.name
        if name in exclude:
            continue
        dtype = field.dataType

        if isinstance(dtype, (FloatType, DoubleType)):
            categories["numerical"].append(name)
        elif isinstance(dtype, NumericType):
            categories["numerical"].append(name)
        elif isinstance(dtype, StringType):
            categories["categorical"].append(name)
        elif isinstance(dtype, BooleanType):
            categories["boolean"].append(name)
        elif isinstance(dtype, (TimestampType, DateType)):
            categories["datetime"].append(name)
        else:
            categories["unsupported"].append(name)

    return categories


def estimate_memory_footprint(total_rows: int, schema) -> Dict[str, Any]:
    """Estimate in-memory footprint based on schema data types and row count."""
    type_sizes = {
        "byte": 1,
        "short": 2,
        "integer": 4,
        "int": 4,
        "long": 8,
        "bigint": 8,
        "float": 4,
        "double": 8,
        "boolean": 1,
        "date": 4,
        "timestamp": 8,
        "string": 32,  # estimated average string length + pointer
    }
    estimated_row_bytes = 0
    for field in schema.fields:
        t_name = field.dataType.typeName().lower()
        estimated_row_bytes += type_sizes.get(t_name, 16)

    total_bytes = estimated_row_bytes * total_rows
    if total_bytes < 1024 * 1024:
        formatted = f"{total_bytes / 1024:.2f} KB"
    elif total_bytes < 1024 * 1024 * 1024:
        formatted = f"{total_bytes / (1024 * 1024):.2f} MB"
    else:
        formatted = f"{total_bytes / (1024 * 1024 * 1024):.2f} GB"

    return {
        "estimated_bytes": total_bytes,
        "estimated_readable": formatted,
        "estimated_bytes_per_row": estimated_row_bytes,
    }


def compute_dataset_overview(
    df: DataFrame,
    config: EDAConfig,
    column_classification: Dict[str, List[str]],
) -> Dict[str, Any]:
    """Compute high-level dataset metrics (rows, columns, duplicates, memory)."""
    total_rows = df.count()
    num_cols = len(df.columns)
    partitions = df.rdd.getNumPartitions()

    # Distributed duplicate detection
    duplicate_rows = 0
    if total_rows > 0:
        distinct_rows = df.dropDuplicates().count()
        duplicate_rows = max(0, total_rows - distinct_rows)

    duplicate_pct = (duplicate_rows / total_rows * 100.0) if total_rows > 0 else 0.0

    memory_info = estimate_memory_footprint(total_rows, df.schema)

    return {
        "total_rows": total_rows,
        "total_columns": num_cols,
        "partitions": partitions,
        "duplicate_rows": duplicate_rows,
        "duplicate_percentage": round(duplicate_pct, 2),
        "numerical_columns": len(column_classification["numerical"]),
        "categorical_columns": len(column_classification["categorical"]),
        "datetime_columns": len(column_classification["datetime"]),
        "boolean_columns": len(column_classification["boolean"]),
        "unsupported_columns": len(column_classification["unsupported"]),
        "memory_info": memory_info,
    }


def build_single_pass_aggregations(
    df: DataFrame,
    config: EDAConfig,
    column_classification: Dict[str, List[str]],
) -> Dict[str, Any]:
    """Execute all column-level descriptive and missingness statistics in a single distributed Spark scan.

    This avoids triggering separate Spark jobs for each column or metric, drastically
    improving profiling performance on massive datasets.
    """
    total_rows = df.count()
    if total_rows == 0:
        return {}

    agg_exprs = []
    schema_map = {f.name: f.dataType for f in df.schema.fields}

    # 1. Missingness expressions for ALL columns
    for col_name in df.columns:
        if col_name in config.exclude_columns:
            continue
        safe_col = f"`{col_name}`"
        # Null count
        agg_exprs.append(
            F.count(F.when(F.col(safe_col).isNull(), 1)).alias(f"{col_name}__nulls")
        )

        dtype = schema_map.get(col_name)
        # NaN count (Floats and Doubles)
        if isinstance(dtype, (FloatType, DoubleType)):
            agg_exprs.append(
                F.count(F.when(F.isnan(F.col(safe_col)), 1)).alias(f"{col_name}__nans")
            )
        # Empty string count
        if isinstance(dtype, StringType):
            agg_exprs.append(
                F.count(F.when(F.trim(F.col(safe_col)) == "", 1)).alias(f"{col_name}__empty")
            )

        # Distinct count
        if config.approx_distinct:
            agg_exprs.append(
                F.approx_count_distinct(
                    F.col(safe_col),
                    rsd=config.approx_distinct_precision,
                ).alias(f"{col_name}__distinct")
            )
        else:
            agg_exprs.append(
                F.countDistinct(F.col(safe_col)).alias(f"{col_name}__distinct")
            )

    # 2. Numerical descriptive statistics
    for col_name in column_classification["numerical"]:
        safe_col = f"`{col_name}`"
        agg_exprs.extend([
            F.min(F.col(safe_col)).alias(f"{col_name}__min"),
            F.max(F.col(safe_col)).alias(f"{col_name}__max"),
            F.mean(F.col(safe_col)).alias(f"{col_name}__mean"),
            F.stddev(F.col(safe_col)).alias(f"{col_name}__stddev"),
            F.sum(F.col(safe_col)).alias(f"{col_name}__sum"),
            F.skewness(F.col(safe_col)).alias(f"{col_name}__skewness"),
            F.kurtosis(F.col(safe_col)).alias(f"{col_name}__kurtosis"),
            F.count(F.when(F.col(safe_col) == 0, 1)).alias(f"{col_name}__zeros"),
            F.count(F.when(F.col(safe_col) < 0, 1)).alias(f"{col_name}__negatives"),
            F.expr(
                f"percentile_approx({safe_col}, array(0.05, 0.25, 0.50, 0.75, 0.95), {config.percentile_accuracy})"
            ).alias(f"{col_name}__quantiles"),
        ])

    # 3. Datetime min/max
    for col_name in column_classification["datetime"]:
        safe_col = f"`{col_name}`"
        agg_exprs.extend([
            F.min(F.col(safe_col)).alias(f"{col_name}__min_time"),
            F.max(F.col(safe_col)).alias(f"{col_name}__max_time"),
        ])

    # Execute single distributed scan
    agg_row = df.agg(*agg_exprs).collect()[0].asDict()

    # Parse raw row into structured dictionary
    results: Dict[str, Dict[str, Any]] = {}
    for col_name in df.columns:
        if col_name in config.exclude_columns:
            continue
        col_res = {
            "total_count": total_rows,
            "null_count": agg_row.get(f"{col_name}__nulls", 0) or 0,
            "nan_count": agg_row.get(f"{col_name}__nans", 0) or 0,
            "empty_string_count": agg_row.get(f"{col_name}__empty", 0) or 0,
            "distinct_count": agg_row.get(f"{col_name}__distinct", 0) or 0,
        }

        # Calculate effective missing count (null + nan + empty)
        effective_missing = (
            col_res["null_count"]
            + col_res["nan_count"]
            + col_res["empty_string_count"]
        )
        col_res["missing_count"] = effective_missing
        col_res["missing_percentage"] = (
            round(effective_missing / total_rows * 100.0, 2) if total_rows > 0 else 0.0
        )
        col_res["distinct_ratio"] = (
            round(col_res["distinct_count"] / total_rows, 4) if total_rows > 0 else 0.0
        )

        if col_name in column_classification["numerical"]:
            quantiles = agg_row.get(f"{col_name}__quantiles") or [0, 0, 0, 0, 0]
            col_res["min"] = agg_row.get(f"{col_name}__min")
            col_res["max"] = agg_row.get(f"{col_name}__max")
            col_res["mean"] = agg_row.get(f"{col_name}__mean")
            col_res["stddev"] = agg_row.get(f"{col_name}__stddev")
            col_res["sum"] = agg_row.get(f"{col_name}__sum")
            col_res["skewness"] = agg_row.get(f"{col_name}__skewness")
            col_res["kurtosis"] = agg_row.get(f"{col_name}__kurtosis")
            col_res["zeros_count"] = agg_row.get(f"{col_name}__zeros", 0) or 0
            col_res["negatives_count"] = agg_row.get(f"{col_name}__negatives", 0) or 0

            # Quantiles & IQR
            p05, p25, p50, p75, p95 = (
                quantiles if len(quantiles) == 5 else [0, 0, 0, 0, 0]
            )
            col_res["p05"] = p05
            col_res["q1"] = p25
            col_res["median"] = p50
            col_res["q3"] = p75
            col_res["p95"] = p95
            iqr = (p75 - p25) if (p75 is not None and p25 is not None) else 0.0
            col_res["iqr"] = iqr

        elif col_name in column_classification["datetime"]:
            col_res["min_time"] = str(agg_row.get(f"{col_name}__min_time"))
            col_res["max_time"] = str(agg_row.get(f"{col_name}__max_time"))

        results[col_name] = col_res

    return results

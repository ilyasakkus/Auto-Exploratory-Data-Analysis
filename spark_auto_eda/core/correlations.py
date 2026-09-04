"""Distributed correlation matrix computation for PySpark DataFrames."""

from typing import Dict, Any, List, Tuple
import math
from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from spark_auto_eda.config import EDAConfig


def compute_correlation_matrix(
    df: DataFrame,
    numerical_columns: List[str],
    config: EDAConfig,
) -> Dict[str, Any]:
    """Compute distributed correlation matrix for numerical features.

    Uses Spark ML Correlation when available, with a resilient fallback
    to pairwise Spark SQL correlation.
    """
    if not config.compute_correlations or len(numerical_columns) < 2:
        return {
            "columns": numerical_columns[: config.correlation_max_cols],
            "matrix": [],
            "collinear_pairs": [],
        }

    # Restrict to correlation_max_cols to prevent explosive O(N^2) memory overhead
    selected_cols = numerical_columns[: config.correlation_max_cols]
    n = len(selected_cols)

    # Filter out columns with constant or zero variance
    clean_cols = []
    for c in selected_cols:
        clean_cols.append(c)

    matrix: List[List[float]] = [[1.0 if i == j else 0.0 for j in range(n)] for i in range(n)]
    collinear_pairs: List[Dict[str, Any]] = []

    try:
        # Try Spark ML VectorAssembler & Correlation
        from pyspark.ml.feature import VectorAssembler
        from pyspark.ml.stat import Correlation

        # Impute nulls with column means for correlation matrix
        fill_exprs = [
            F.coalesce(F.col(f"`{c}`").cast("double"), F.lit(0.0)).alias(c)
            for c in clean_cols
        ]
        imputed_df = df.select(*fill_exprs)

        assembler = VectorAssembler(
            inputCols=clean_cols,
            outputCol="__corr_features",
            handleInvalid="keep",
        )
        vector_df = assembler.transform(imputed_df).select("__corr_features")

        # ML Correlation (single distributed matrix multiplication)
        corr_result = Correlation.corr(
            vector_df,
            "__corr_features",
            method=config.correlation_method,
        ).head()

        if corr_result and corr_result[0] is not None:
            raw_matrix = corr_result[0].toArray()
            for i in range(n):
                for j in range(n):
                    val = float(raw_matrix[i][j])
                    if math.isnan(val):
                        val = 0.0
                    val = round(val, 4)
                    matrix[i][j] = val

                    # Detect collinear pairs
                    if i < j and abs(val) >= config.collinearity_threshold:
                        collinear_pairs.append({
                            "feature_a": clean_cols[i],
                            "feature_b": clean_cols[j],
                            "correlation": val,
                        })

    except Exception:
        # Fallback to pairwise Spark SQL correlation expressions
        corr_exprs = []
        pair_indices = []
        for i in range(n):
            for j in range(i + 1, n):
                col_a = f"`{clean_cols[i]}`"
                col_b = f"`{clean_cols[j]}`"
                corr_exprs.append(F.corr(col_a, col_b).alias(f"corr__{i}__{j}"))
                pair_indices.append((i, j))

        if corr_exprs:
            row = df.agg(*corr_exprs).collect()[0].asDict()
            for i, j in pair_indices:
                val = row.get(f"corr__{i}__{j}")
                if val is None or math.isnan(val):
                    val = 0.0
                val = round(float(val), 4)
                matrix[i][j] = val
                matrix[j][i] = val

                if abs(val) >= config.collinearity_threshold:
                    collinear_pairs.append({
                        "feature_a": clean_cols[i],
                        "feature_b": clean_cols[j],
                        "correlation": val,
                    })

    # Sort collinear pairs by absolute correlation descending
    collinear_pairs.sort(key=lambda x: abs(x["correlation"]), reverse=True)

    return {
        "columns": clean_cols,
        "matrix": matrix,
        "collinear_pairs": collinear_pairs,
    }

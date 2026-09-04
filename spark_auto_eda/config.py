"""Configuration module for SparkAutoEDA."""

from dataclasses import dataclass, field
from typing import List, Optional


@dataclass
class EDAConfig:
    """Configuration options for distributed exploratory data analysis.

    Attributes:
        sample_ratio: Fraction of DataFrame to analyze (default 1.0 = 100% full scan).
        sample_seed: Random seed for sampling if sample_ratio < 1.0.
        histogram_bins: Number of equal-width bins for numerical histograms.
        top_k_categories: Number of most frequent categorical values to track.
        compute_correlations: Whether to compute numerical correlation matrix.
        correlation_max_cols: Maximum number of numerical columns to include in correlation matrix.
        correlation_method: Correlation method ('pearson' or 'spearman').
        approx_distinct: Whether to use HyperLogLog++ approximation for distinct counts on big data.
        approx_distinct_precision: Relative standard error for approx_count_distinct (0.01 to 0.10).
        percentile_accuracy: Accuracy for percentile_approx (default 10000).
        high_null_threshold: Percentage of missing values to flag as alert (0.0 - 1.0).
        high_cardinality_ratio: Ratio of distinct values to total rows to flag as high cardinality.
        collinearity_threshold: Absolute correlation coefficient (|r|) to flag collinearity.
        outlier_iqr_multiplier: IQR multiplier for Tukey's outlier detection rule (default 1.5).
        sample_preview_rows: Number of top rows to include in the visual report preview.
        exclude_columns: List of column names to skip from profiling.
    """

    sample_ratio: float = 1.0
    sample_seed: Optional[int] = 42
    histogram_bins: int = 20
    top_k_categories: int = 10
    compute_correlations: bool = True
    correlation_max_cols: int = 30
    correlation_method: str = "pearson"
    approx_distinct: bool = True
    approx_distinct_precision: float = 0.05
    percentile_accuracy: int = 10000
    high_null_threshold: float = 0.20
    high_cardinality_ratio: float = 0.60
    collinearity_threshold: float = 0.90
    outlier_iqr_multiplier: float = 1.5
    sample_preview_rows: int = 10
    exclude_columns: List[str] = field(default_factory=list)

    def validate(self) -> None:
        """Validate configuration bounds."""
        if not (0.0 < self.sample_ratio <= 1.0):
            raise ValueError(f"sample_ratio must be in (0.0, 1.0], got {self.sample_ratio}")
        if self.histogram_bins < 2:
            raise ValueError(f"histogram_bins must be >= 2, got {self.histogram_bins}")
        if not (0.0 < self.high_null_threshold < 1.0):
            raise ValueError(f"high_null_threshold must be in (0, 1), got {self.high_null_threshold}")
        if not (0.0 < self.collinearity_threshold <= 1.0):
            raise ValueError(f"collinearity_threshold must be in (0, 1], got {self.collinearity_threshold}")

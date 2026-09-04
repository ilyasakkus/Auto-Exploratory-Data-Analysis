"""Core orchestrator and entry point for distributed PySpark exploratory data analysis."""

from typing import Optional, Dict, Any, List
from pyspark.sql import DataFrame
from spark_auto_eda.config import EDAConfig
from spark_auto_eda.core.aggregators import (
    classify_columns,
    compute_dataset_overview,
    build_single_pass_aggregations,
)
from spark_auto_eda.core.numerical import profile_all_numerical_columns
from spark_auto_eda.core.categorical import profile_all_categorical_columns
from spark_auto_eda.core.datetime import profile_all_datetime_columns
from spark_auto_eda.core.correlations import compute_correlation_matrix
from spark_auto_eda.core.quality_alerts import generate_quality_alerts


class AutoEDA:
    """Automated Exploratory Data Analysis Engine for large-scale PySpark DataFrames."""

    def __init__(self, df: DataFrame, config: Optional[EDAConfig] = None, **kwargs):
        """Initialize AutoEDA analyzer.

        Args:
            df: PySpark DataFrame to analyze.
            config: Optional EDAConfig instance. If not provided, default config is used.
            **kwargs: Overrides for EDAConfig parameters (e.g. sample_ratio=0.5).
        """
        self.df = df
        if config is not None:
            self.config = config
        else:
            self.config = EDAConfig(**kwargs) if kwargs else EDAConfig()

        self.config.validate()

    def run(self):
        """Execute distributed profiling across the PySpark cluster and return ProfileReport."""
        from spark_auto_eda.report.profile_report import ProfileReport

        # 1. Apply sampling if requested
        working_df = self.df
        total_original_rows = self.df.count()
        is_sampled = self.config.sample_ratio < 1.0

        if is_sampled:
            working_df = self.df.sample(
                fraction=self.config.sample_ratio,
                seed=self.config.sample_seed,
            )

        # 2. Classify columns
        column_classes = classify_columns(
            working_df,
            exclude_columns=self.config.exclude_columns,
        )

        # 3. Overview metrics
        overview = compute_dataset_overview(working_df, self.config, column_classes)
        overview["is_sampled"] = is_sampled
        overview["sample_ratio"] = self.config.sample_ratio
        overview["total_original_rows"] = total_original_rows

        # 4. Single-pass distributed metric calculation
        column_stats = build_single_pass_aggregations(
            working_df,
            self.config,
            column_classes,
        )

        # Attach column data types
        schema_dict = {f.name: f.dataType.simpleString() for f in self.df.schema.fields}
        for c_name, c_data in column_stats.items():
            c_data["data_type"] = schema_dict.get(c_name, "unknown")
            if c_name in column_classes["numerical"]:
                c_data["type_category"] = "numerical"
            elif c_name in column_classes["categorical"]:
                c_data["type_category"] = "categorical"
            elif c_name in column_classes["boolean"]:
                c_data["type_category"] = "boolean"
            elif c_name in column_classes["datetime"]:
                c_data["type_category"] = "datetime"
            else:
                c_data["type_category"] = "unsupported"

        # 5. Numerical profiling (histograms & outliers)
        column_stats = profile_all_numerical_columns(
            df=working_df,
            column_stats=column_stats,
            numerical_columns=column_classes["numerical"],
            config=self.config,
        )

        # 6. Categorical profiling (frequencies & lengths)
        column_stats = profile_all_categorical_columns(
            df=working_df,
            column_stats=column_stats,
            categorical_columns=column_classes["categorical"],
            boolean_columns=column_classes["boolean"],
            total_rows=overview["total_rows"],
            config=self.config,
        )

        # 7. Datetime profiling
        column_stats = profile_all_datetime_columns(
            df=working_df,
            column_stats=column_stats,
            datetime_columns=column_classes["datetime"],
            total_rows=overview["total_rows"],
            config=self.config,
        )

        # 8. Correlations
        correlations = compute_correlation_matrix(
            df=working_df,
            numerical_columns=column_classes["numerical"],
            config=self.config,
        )

        # 9. Quality alerts & health scoring
        quality = generate_quality_alerts(
            overview=overview,
            column_stats=column_stats,
            correlation_data=correlations,
            config=self.config,
        )

        # 10. Sample preview rows
        preview_rows = []
        if self.config.sample_preview_rows > 0 and overview["total_rows"] > 0:
            sample_df = self.df.limit(self.config.sample_preview_rows)
            # Convert to dict cleanly
            rows = sample_df.collect()
            for r in rows:
                row_dict = r.asDict()
                # Ensure values are JSON serializable strings or numbers
                clean_row = {}
                for k, v in row_dict.items():
                    if v is None:
                        clean_row[k] = None
                    elif isinstance(v, (int, float, bool, str)):
                        clean_row[k] = v
                    else:
                        clean_row[k] = str(v)
                preview_rows.append(clean_row)

        profile_data = {
            "overview": overview,
            "column_classes": column_classes,
            "column_stats": column_stats,
            "correlations": correlations,
            "quality": quality,
            "preview_rows": preview_rows,
            "columns": list(self.df.columns),
            "config": {
                "sample_ratio": self.config.sample_ratio,
                "histogram_bins": self.config.histogram_bins,
                "top_k_categories": self.config.top_k_categories,
                "compute_correlations": self.config.compute_correlations,
            },
        }

        return ProfileReport(data=profile_data)

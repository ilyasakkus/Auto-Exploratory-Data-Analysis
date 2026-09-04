"""SparkAutoEDA: Distributed automated Exploratory Data Analysis for large-scale PySpark DataFrames."""

from spark_auto_eda.config import EDAConfig
from spark_auto_eda.core.analyzer import AutoEDA
from spark_auto_eda.report.profile_report import ProfileReport

__version__ = "0.1.0"


def profile_data(df, config=None, **kwargs) -> ProfileReport:
    """Convenience helper function to profile a PySpark DataFrame in one line.

    Example:
        >>> from pyspark.sql import SparkSession
        >>> from spark_auto_eda import profile_data
        >>> spark = SparkSession.builder.getOrCreate()
        >>> df = spark.read.parquet("massive_data.parquet")
        >>> report = profile_data(df, sample_ratio=1.0)
        >>> report.to_html("report.html")

    Args:
        df: PySpark DataFrame to profile.
        config: Optional EDAConfig instance.
        **kwargs: Configuration overrides passed to EDAConfig.

    Returns:
        ProfileReport instance.
    """
    analyzer = AutoEDA(df=df, config=config, **kwargs)
    return analyzer.run()


__all__ = [
    "AutoEDA",
    "profile_data",
    "EDAConfig",
    "ProfileReport",
    "__version__",
]

"""Quickstart demonstration of SparkAutoEDA on PySpark."""

from pathlib import Path
from pyspark.sql import SparkSession
from spark_auto_eda import profile_data, AutoEDA, EDAConfig
from generate_sample_data import generate_synthetic_transactions


def main():
    print("🚀 Initializing PySpark local cluster session...")
    spark = (
        SparkSession.builder.appName("SparkAutoEDA-Quickstart")
        .master("local[*]")
        .config("spark.driver.memory", "4g")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    data_path = "sample_transactions.parquet"
    if not Path(data_path).exists():
        print("📁 Sample data not found, generating 100,000 synthetic records...")
        df = generate_synthetic_transactions(spark, num_rows=100_000, output_path=data_path)
    else:
        print(f"📁 Loading existing dataset from {data_path}...")
        df = spark.read.parquet(data_path)

    print(f"📊 Dataset Shape: {df.count():,} rows x {len(df.columns)} columns")

    # 1. Configure analysis options (optional)
    config = EDAConfig(
        sample_ratio=1.0,           # 100% full distributed scan
        compute_correlations=True,  # distributed Pearson correlation
        histogram_bins=20,          # 20 bins for numerical distributions
        top_k_categories=8,         # Top 8 most frequent categories
        high_null_threshold=0.20,   # Flag warning if nulls > 20%
    )

    print("⚡ Running distributed profiling with SparkAutoEDA...")
    report = profile_data(df, config=config)

    print("\n" + "=" * 50)
    print(f"📌 Profile Report Summary: {report}")
    print("=" * 50 + "\n")

    # Export to multi-format outputs
    html_file = "eda_report.html"
    md_file = "eda_report.md"
    json_file = "eda_report.json"

    print(f"💾 Saving interactive HTML dashboard to: {html_file}")
    report.to_html(html_file)

    print(f"💾 Saving GitHub-compatible Markdown report to: {md_file}")
    report.to_markdown(md_file)

    print(f"💾 Saving structured JSON profile to: {json_file}")
    report.to_json(json_file)

    print("\n🎉 Completed! Open 'eda_report.html' in your browser to explore the dashboard.")
    spark.stop()


if __name__ == "__main__":
    main()

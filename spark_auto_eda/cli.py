"""Command-line interface (CLI) for SparkAutoEDA."""

import argparse
import sys
from pathlib import Path
from spark_auto_eda import __version__, profile_data, EDAConfig


def get_spark_session(app_name: str = "SparkAutoEDA-CLI"):
    """Create or retrieve active PySpark session."""
    from pyspark.sql import SparkSession

    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .config("spark.driver.memory", "4g")
        .getOrCreate()
    )


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        prog="spark-auto-eda",
        description="⚡ SparkAutoEDA: Distributed automated data profiling for PySpark.",
    )
    parser.add_argument(
        "--version",
        "-v",
        action="version",
        version=f"spark-auto-eda {__version__}",
    )

    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # Analyze subcommand
    analyze_parser = subparsers.add_parser("analyze", help="Profile a dataset and generate report")
    analyze_parser.add_argument(
        "--input",
        "-i",
        required=True,
        help="Path to input dataset file or directory (Parquet, CSV, ORC, JSON)",
    )
    analyze_parser.add_argument(
        "--format",
        "-f",
        default="parquet",
        choices=["parquet", "csv", "orc", "json"],
        help="Input file format (default: parquet)",
    )
    analyze_parser.add_argument(
        "--delimiter",
        default=",",
        help="CSV delimiter character (default: ,)",
    )
    analyze_parser.add_argument(
        "--header",
        action="store_true",
        default=True,
        help="Whether CSV contains header line (default: True)",
    )
    analyze_parser.add_argument(
        "--output",
        "-o",
        default="eda_report.html",
        help="Output report file path (e.g. eda_report.html, eda_report.md, eda_report.json)",
    )
    analyze_parser.add_argument(
        "--report-type",
        choices=["html", "markdown", "json", "all"],
        default="html",
        help="Report export format (default: html)",
    )
    analyze_parser.add_argument(
        "--sample-ratio",
        type=float,
        default=1.0,
        help="Sampling ratio between 0.0 and 1.0 (default: 1.0 = 100% full scan)",
    )
    analyze_parser.add_argument(
        "--no-correlations",
        action="store_true",
        help="Skip correlation matrix calculation for maximum speed",
    )

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    if args.command == "analyze":
        input_path = args.input
        print(f"⚡ Starting SparkAutoEDA analysis for: {input_path}")
        spark = get_spark_session()
        spark.sparkContext.setLogLevel("WARN")

        # Load DataFrame
        fmt = args.format.lower()
        if fmt == "parquet":
            df = spark.read.parquet(input_path)
        elif fmt == "csv":
            df = spark.read.option("header", str(args.header).lower()).option("delimiter", args.delimiter).option("inferSchema", "true").csv(input_path)
        elif fmt == "orc":
            df = spark.read.orc(input_path)
        elif fmt == "json":
            df = spark.read.json(input_path)
        else:
            print(f"❌ Unsupported format: {fmt}")
            sys.exit(1)

        total_rows = df.count()
        print(f"📊 Loaded DataFrame with {total_rows:,} rows and {len(df.columns)} columns.")

        config = EDAConfig(
            sample_ratio=args.sample_ratio,
            compute_correlations=not args.no_correlations,
        )

        report = profile_data(df, config=config)

        # Output handling
        rep_type = args.report_type.lower()
        out_path = Path(args.output)

        if rep_type in ["html", "all"]:
            html_target = out_path.with_suffix(".html") if rep_type == "all" else out_path
            report.to_html(str(html_target))
            print(f"✅ Interactive HTML dashboard saved: {html_target}")

        if rep_type in ["markdown", "all"]:
            md_target = out_path.with_suffix(".md") if rep_type == "all" else out_path
            report.to_markdown(str(md_target))
            print(f"✅ Markdown report saved: {md_target}")

        if rep_type in ["json", "all"]:
            json_target = out_path.with_suffix(".json") if rep_type == "all" else out_path
            report.to_json(str(json_target))
            print(f"✅ JSON profile data saved: {json_target}")

        spark.stop()
        print("🎉 Analysis finished successfully!")


if __name__ == "__main__":
    main()

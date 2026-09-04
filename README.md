# ⚡ SparkAutoEDA: Distributed Automated EDA & Profiling for PySpark

<p align="center">
  <img src="https://img.shields.io/badge/Apache_Spark-3.2%20--%204.2+-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white" alt="Spark Support" />
  <img src="https://img.shields.io/badge/Python-3.9%20%7C%203.10%20%7C%203.11%20%7C%203.12-3776AB?style=for-the-badge&logo=python&logoColor=white" alt="Python Support" />
  <img src="https://img.shields.io/badge/License-MIT-blue.svg?style=for-the-badge" alt="License" />
  <img src="https://img.shields.io/badge/PRs-welcome-brightgreen.svg?style=for-the-badge" alt="PRs Welcome" />
</p>

<p align="center">
  <strong>Lightning-fast, distributed automated Exploratory Data Analysis & visual reporting for multi-million-row PySpark DataFrames.</strong>
</p>

---

## 💡 Why SparkAutoEDA?

Standard data profiling tools like `pandas-profiling` (`ydata-profiling`) are designed for single-node machines and **crash with Out-Of-Memory (OOM) errors** when analyzing datasets exceeding memory limits.

Conversely, naive PySpark analysis scripts repeatedly call `.describe()`, `.count()`, or individual column loops, triggering **hundreds of unnecessary distributed jobs and shuffle stages**, paralyzing Spark clusters.

**SparkAutoEDA** solves this by executing all descriptive statistics, approximate quantiles, null counts, and distinct estimations in a **single consolidated Catalyst aggregation pass**. It then generates an **interactive, self-contained HTML dashboard**, a **GitHub-compatible Markdown summary**, or a **machine-readable JSON profile**.

| Feature | `pandas-profiling` / `ydata` | Native Spark `.describe()` | **SparkAutoEDA** ⚡ |
| :--- | :---: | :---: | :---: |
| **Max Scale** | ~1-5 Million rows (Single node) | Terabytes | **Petabytes (Distributed Spark)** |
| **Out-Of-Memory Risk** | 🚨 Very High | Low | 🛡️ **Zero (Pure distributed execution)** |
| **Job Scheduling** | N/A (Local Pandas) | Inefficient (Multiple scans) | ⚡ **Minimal-Pass Catalyst Optimization** |
| **Interactive HTML Dashboard** | Yes | No | 🌟 **Yes (Standalone Dark/Light UI)** |
| **GitHub Markdown Export** | Basic | No | 📝 **Yes (Native GitHub Alerts & Tables)** |
| **Automated Data Health Score** | No | No | 🎯 **Yes (0 - 100 Scorecard & Alerts)** |
| **Distributed Correlation Heatmap** | High memory | Pairwise manual | 📊 **Yes (Distributed Spark ML Matrix)** |
| **Notebook Integration** | Yes | Plain Text | 📓 **Yes (Rich `_repr_html_()` support)** |

---

## 🚀 Key Features

* **⚡ Single-Pass Distributed Engine**: Consolidates missingness, distinct counts (HyperLogLog++), descriptive statistics (mean, stddev, min, max, skewness, kurtosis), and approximate quantiles into unified Spark SQL Catalyst expressions.
* **🎨 Modern Visual Dashboard**: Responsive standalone HTML with Dark & Light modes, glassmorphism aesthetics, embedded interactive distribution charts (Chart.js), correlation heatmaps, and column search.
* **🚨 Automated Data Quality Auditing**: Rule-based health score (0-100) flagging critical missingness, single-value constant features, extreme outliers (Tukey's IQR), collinear feature pairs ($|r| \ge 0.90$), and dataset duplicates.
* **📝 Multi-Format Reports**:
  * **Interactive HTML**: For data scientists, executives, and stakeholder presentations.
  * **GitHub Markdown**: For CI/CD data pipelines, automated PR comments, and documentation.
  * **Structured JSON**: For programmatic validation gates and data drift tracking.
* **🛠️ Dual Interface**: Use as an intuitive Python library (`AutoEDA(df).run()`) or as a standalone CLI tool (`spark-auto-eda analyze`).

---

## 📦 Installation

```bash
pip install spark-auto-eda
```

Or install from source with development dependencies:

```bash
git clone https://github.com/open-source/spark-auto-eda.git
cd spark-auto-eda
pip install -e ".[dev]"
```

---

## ⚡ Quickstart

### 1. Python API

```python
from pyspark.sql import SparkSession
from spark_auto_eda import profile_data, EDAConfig

# Initialize your SparkSession
spark = SparkSession.builder.appName("AutoEDA-Demo").getOrCreate()

# Load any PySpark DataFrame (Parquet, Delta, CSV, ORC, Iceberg)
df = spark.read.parquet("s3://my-bucket/massive_transactions.parquet")

# Run automated distributed profiling
report = profile_data(
    df,
    sample_ratio=1.0,           # 1.0 = 100% full cluster scan
    compute_correlations=True,  # Distributed Pearson correlation matrix
    histogram_bins=20,          # Distribution histogram bins
)

# Export reports
report.to_html("eda_report.html")        # Interactive visual dashboard
report.to_markdown("eda_report.md")      # GitHub-flavored Markdown
report.to_json("eda_report.json")        # Machine-readable JSON profile
```

### 2. Interactive Notebooks (Jupyter & Databricks)

Inside a Jupyter Notebook or Databricks cell:

```python
from spark_auto_eda import profile_data

report = profile_data(df)
report  # Automatically renders the interactive HTML dashboard directly in your notebook!
```

### 3. Command-Line Interface (CLI)

```bash
# Analyze Parquet dataset
spark-auto-eda analyze --input /path/to/data.parquet --output report.html --format html

# Analyze CSV with custom delimiter and export both Markdown and HTML
spark-auto-eda analyze --input /path/to/data.csv --delimiter ";" --output eda_report.md --report-type all
```

---

## ⚙️ Configuration Options

Fine-tune profiling behavior via `EDAConfig`:

```python
from spark_auto_eda import EDAConfig, AutoEDA

config = EDAConfig(
    sample_ratio=1.0,                 # Fraction of DataFrame to analyze (0.0 to 1.0)
    compute_correlations=True,        # Whether to compute correlation matrix
    correlation_max_cols=30,          # Limit columns to prevent O(N^2) memory overhead
    correlation_method="pearson",     # 'pearson' or 'spearman'
    histogram_bins=20,                # Number of equal-width histogram bins
    top_k_categories=10,              # Top frequent values for categorical features
    approx_distinct=True,             # HyperLogLog++ approximation for distinct counts
    approx_distinct_precision=0.05,   # Relative standard error for HyperLogLog (5%)
    high_null_threshold=0.20,         # Alert threshold for missing values (20%)
    collinearity_threshold=0.90,      # Alert threshold for collinear feature pairs (|r| >= 0.90)
    outlier_iqr_multiplier=1.5,       # Tukey's outlier multiplier
    sample_preview_rows=10,           # Rows to display in data preview table
    exclude_columns=["sensitive_id"], # Columns to exclude from profiling
)

report = AutoEDA(df, config=config).run()
```

---

## 🏗️ Architecture

```
                                 PySpark DataFrame
                                         │
                                         ▼
                     ┌──────────────────────────────────────┐
                     │          Column Classifier           │
                     │  (Numerical / Categorical / Datetime)│
                     └──────────────────┬───────────────────┘
                                        │
                                        ▼
    ┌───────────────────────────────────────────────────────────────────────┐
    │                   Single-Pass Catalyst Aggregator                     │
    │  - Total Rows & PySpark Partitions Count                              │
    │  - Null / NaN / Empty String Counts                                   │
    │  - Distinct Count Estimation (HyperLogLog++)                          │
    │  - Mean, StdDev, Min, Max, Sum, Skewness, Kurtosis                   │
    │  - Distributed Quantiles (p05, p25, median p50, p75, p95) via Spark   │
    └───────────────────────────────────┬───────────────────────────────────┘
                                        │
             ┌──────────────────────────┼──────────────────────────┐
             ▼                          ▼                          ▼
┌─────────────────────────┐┌─────────────────────────┐┌─────────────────────────┐
│  Distributed Histograms ││  Categorical Frequencies││ Correlation Heatmap     │
│  (Spark width_bucket)   ││  (Top-K value counts)   ││ (Spark ML Matrix)       │
└────────────┬────────────┘└────────────┬────────────┘└────────────┬────────────┘
             │                          │                          │
             └──────────────────────────┼──────────────────────────┘
                                        │
                                        ▼
                     ┌──────────────────────────────────────┐
                     │      Data Health & Alert Engine      │
                     │  - Missingness / Duplicates / Outlier│
                     │  - Collinearity / Cardinality / Zeros│
                     │  - Health Score (0 - 100)            │
                     └──────────────────┬───────────────────┘
                                        │
                                        ▼
                     ┌──────────────────────────────────────┐
                     │            ProfileReport             │
                     │   .to_html()  .to_markdown() .to_json│
                     └──────────────────────────────────────┘
```

---

## 🧪 Testing

Run the automated test suite with pytest:

```bash
pytest -v tests/
```

To run the end-to-end benchmark demo with 100,000 synthetic records:

```bash
python examples/basic_usage.py
```

---

## 🤝 Contributing

Contributions are warmly welcomed! Please read [CONTRIBUTING.md](CONTRIBUTING.md) to set up your development environment and submit pull requests.

---

## 📄 License

This project is licensed under the terms of the [MIT License](LICENSE).

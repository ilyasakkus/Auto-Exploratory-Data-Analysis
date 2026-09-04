# Contributing to SparkAutoEDA

Thank you for your interest in contributing to **SparkAutoEDA**! We are committed to building an exceptional, high-performance distributed EDA tool for Apache Spark.

## Development Setup

1. **Fork and clone the repository**:
   ```bash
   git clone https://github.com/your-username/spark-auto-eda.git
   cd spark-auto-eda
   ```

2. **Create and activate a virtual environment (Python >= 3.9)**:
   ```bash
   python3 -m venv .venv
   source .venv/bin/activate
   ```

3. **Install development dependencies**:
   ```bash
   pip install -e ".[dev]"
   ```

4. **Run the test suite**:
   ```bash
   pytest -v tests/
   ```

## Contribution Workflow

1. Create a feature branch:
   ```bash
   git checkout -b feature/awesome-feature
   ```
2. Write modular, typed code with docstrings.
3. Add unit tests in `tests/` covering new functionality.
4. Ensure all tests pass.
5. Submit a descriptive Pull Request!

## Coding Guidelines

- Follow **PEP 8** conventions.
- Maintain **distributed efficiency**: Avoid `collect()` on large dataframes. Consolidate aggregations into single-pass Spark SQL expressions whenever possible.
- Include unit tests for every new feature or bug fix.

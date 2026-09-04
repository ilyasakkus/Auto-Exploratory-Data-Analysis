"""Tests for HTML, Markdown, and JSON report generation."""

from pathlib import Path
from spark_auto_eda import profile_data


def test_full_pipeline_and_reports(sample_df, tmp_path):
    report = profile_data(sample_df)

    # 1. HTML Export
    html_path = tmp_path / "test_report.html"
    html_content = report.to_html(str(html_path))
    assert Path(html_path).exists()
    assert "SparkAutoEDA Dashboard" in html_content
    assert "Executive Overview" in html_content or "Total Records" in html_content

    # 2. Markdown Export
    md_path = tmp_path / "test_report.md"
    md_content = report.to_markdown(str(md_path))
    assert Path(md_path).exists()
    assert "# ⚡ SparkAutoEDA: Automated Dataset Profile Report" in md_content
    assert "Executive Overview" in md_content

    # 3. JSON Export
    json_path = tmp_path / "test_report.json"
    json_content = report.to_json(str(json_path))
    assert Path(json_path).exists()
    assert '"overview"' in json_content
    assert '"health_score"' in json_content

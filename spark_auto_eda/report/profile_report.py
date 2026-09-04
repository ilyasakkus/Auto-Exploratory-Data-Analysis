"""ProfileReport container class for SparkAutoEDA results."""

from pathlib import Path
from typing import Dict, Any, Optional
from spark_auto_eda.report.html_renderer import render_html
from spark_auto_eda.report.markdown_renderer import render_markdown
from spark_auto_eda.report.json_renderer import render_json


class ProfileReport:
    """Container holding profiled metrics with multi-format export capabilities."""

    def __init__(self, data: Dict[str, Any]):
        self._data = data

    @property
    def data(self) -> Dict[str, Any]:
        """Return the raw profiling results dictionary."""
        return self._data

    def to_dict(self) -> Dict[str, Any]:
        """Return results as Python dictionary."""
        return self._data

    def to_html(self, output_path: Optional[str] = None) -> str:
        """Render interactive HTML dashboard and optionally write to file.

        Args:
            output_path: Optional file path (e.g. 'eda_report.html').
        Returns:
            HTML string content.
        """
        html_str = render_html(self._data)
        if output_path:
            path = Path(output_path)
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(html_str, encoding="utf-8")
        return html_str

    def to_markdown(self, output_path: Optional[str] = None) -> str:
        """Render GitHub-compatible Markdown report and optionally write to file.

        Args:
            output_path: Optional file path (e.g. 'eda_report.md').
        Returns:
            Markdown string content.
        """
        md_str = render_markdown(self._data)
        if output_path:
            path = Path(output_path)
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(md_str, encoding="utf-8")
        return md_str

    def to_json(self, output_path: Optional[str] = None, indent: int = 2) -> str:
        """Serialize profile to JSON format and optionally write to file.

        Args:
            output_path: Optional file path (e.g. 'eda_report.json').
            indent: Indentation level for formatting.
        Returns:
            JSON string.
        """
        json_str = render_json(self._data, indent=indent)
        if output_path:
            path = Path(output_path)
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(json_str, encoding="utf-8")
        return json_str

    def _repr_html_(self) -> str:
        """Rich HTML display for Jupyter and Databricks notebooks."""
        return self.to_html()

    def __repr__(self) -> str:
        ov = self._data.get("overview", {})
        q = self._data.get("quality", {})
        return (
            f"<ProfileReport: {ov.get('total_rows', 0):,} rows, "
            f"{ov.get('total_columns', 0)} columns | "
            f"Health Score: {q.get('health_score', 'N/A')}/100 ({q.get('health_status', 'N/A')})>"
        )

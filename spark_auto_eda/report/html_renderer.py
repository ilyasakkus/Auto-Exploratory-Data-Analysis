"""HTML dashboard report renderer for SparkAutoEDA using Jinja2."""

import json
from pathlib import Path
from typing import Dict, Any
from jinja2 import Environment, FileSystemLoader


def render_html(profile_data: Dict[str, Any]) -> str:
    """Render interactive HTML dashboard using Jinja2 template."""
    template_dir = Path(__file__).resolve().parent.parent / "templates"
    env = Environment(loader=FileSystemLoader(str(template_dir)), autoescape=True)
    template = env.get_template("dashboard.html.jinja")

    # Build chart payload for numerical histograms
    chart_payload = []
    columns = profile_data.get("columns", [])
    col_stats = profile_data.get("column_stats", {})

    for idx, col_name in enumerate(columns):
        st = col_stats.get(col_name, {})
        if st.get("type_category") == "numerical":
            hist = st.get("histogram", {})
            chart_payload.append({
                "id": f"chart-{idx}",
                "column": col_name,
                "labels": hist.get("labels", []),
                "counts": hist.get("counts", []),
            })

    html_content = template.render(
        data=profile_data,
        chart_payload=json.dumps(chart_payload),
    )

    return html_content

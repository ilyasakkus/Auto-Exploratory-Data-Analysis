"""Tests for quality alerts and scoring."""

from spark_auto_eda.config import EDAConfig
from spark_auto_eda.core.quality_alerts import generate_quality_alerts


def test_generate_quality_alerts():
    config = EDAConfig(high_null_threshold=0.20)
    overview = {"total_rows": 100, "duplicate_rows": 0, "duplicate_percentage": 0.0}
    column_stats = {
        "col_clean": {
            "missing_percentage": 0.0,
            "distinct_count": 50,
            "distinct_ratio": 0.5,
        },
        "col_missing": {
            "missing_percentage": 65.0,  # Critical
            "distinct_count": 10,
            "distinct_ratio": 0.1,
        },
    }
    correlation_data = {"collinear_pairs": []}

    quality = generate_quality_alerts(overview, column_stats, correlation_data, config)

    assert quality["total_alerts"] >= 1
    assert quality["critical_count"] == 1
    assert quality["health_score"] < 100
    assert quality["health_status"] in ["Good", "Fair", "Excellent"]

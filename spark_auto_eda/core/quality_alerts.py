"""Data quality health score and automated rule-based alert engine."""

from typing import Dict, Any, List
from spark_auto_eda.config import EDAConfig


def generate_quality_alerts(
    overview: Dict[str, Any],
    column_stats: Dict[str, Dict[str, Any]],
    correlation_data: Dict[str, Any],
    config: EDAConfig,
) -> Dict[str, Any]:
    """Inspect profiled metrics to identify anomalies, redundancies, and quality risks."""
    alerts: List[Dict[str, Any]] = []
    total_rows = overview.get("total_rows", 0)

    # 1. Dataset-level duplicate alert
    dup_pct = overview.get("duplicate_percentage", 0.0)
    if dup_pct > 10.0:
        alerts.append({
            "type": "DUPLICATES",
            "severity": "CRITICAL",
            "column": "Dataset",
            "message": f"High duplicate rows detected: {overview.get('duplicate_rows'):,} rows ({dup_pct}%) are exact duplicates.",
        })
    elif dup_pct > 0.0:
        alerts.append({
            "type": "DUPLICATES",
            "severity": "WARNING",
            "column": "Dataset",
            "message": f"Duplicate rows detected: {overview.get('duplicate_rows'):,} rows ({dup_pct}%) are duplicates.",
        })

    # 2. Column-level alerts
    for col_name, stats in column_stats.items():
        missing_pct = stats.get("missing_percentage", 0.0)
        distinct_count = stats.get("distinct_count", 0)
        distinct_ratio = stats.get("distinct_ratio", 0.0)

        # High missingness
        if missing_pct >= 50.0:
            alerts.append({
                "type": "HIGH_MISSING",
                "severity": "CRITICAL",
                "column": col_name,
                "message": f"Critical missing values: {missing_pct}% of entries are null or empty.",
            })
        elif missing_pct >= (config.high_null_threshold * 100.0):
            alerts.append({
                "type": "HIGH_MISSING",
                "severity": "WARNING",
                "column": col_name,
                "message": f"Elevated missing values: {missing_pct}% of entries are null or empty.",
            })

        # Constant / Zero Variance column
        if total_rows > 1 and distinct_count <= 1:
            alerts.append({
                "type": "CONSTANT_COLUMN",
                "severity": "WARNING",
                "column": col_name,
                "message": f"Constant feature: column contains only {distinct_count} unique value.",
            })

        # High cardinality in non-numerical columns
        if "histogram" not in stats and distinct_ratio >= config.high_cardinality_ratio and total_rows > 100:
            alerts.append({
                "type": "HIGH_CARDINALITY",
                "severity": "INFO",
                "column": col_name,
                "message": f"High cardinality ({distinct_count:,} unique values, {round(distinct_ratio*100, 1)}% ratio). Likely an identifier or primary key.",
            })

        # Extreme Outliers in numerical columns
        outlier_pct = stats.get("outlier_percentage", 0.0)
        if outlier_pct >= 10.0:
            alerts.append({
                "type": "OUTLIERS",
                "severity": "WARNING",
                "column": col_name,
                "message": f"High outlier density: {outlier_pct}% of values fall outside Tukey's IQR bounds [{stats.get('lower_bound')}, {stats.get('upper_bound')}].",
            })

        # High zero concentration
        if "zeros_count" in stats and total_rows > 0:
            zero_pct = (stats["zeros_count"] / total_rows) * 100.0
            if zero_pct >= 60.0:
                alerts.append({
                    "type": "SPARSE_ZEROS",
                    "severity": "INFO",
                    "column": col_name,
                    "message": f"Sparse column: {round(zero_pct, 1)}% of values are exactly zero.",
                })

    # 3. Collinearity alerts
    for pair in correlation_data.get("collinear_pairs", []):
        r = pair["correlation"]
        alerts.append({
            "type": "HIGH_COLLINEARITY",
            "severity": "WARNING",
            "column": f"{pair['feature_a']} & {pair['feature_b']}",
            "message": f"High collinearity detected (Pearson r = {r:.2f}). Consider removing one to prevent multicollinearity.",
        })

    # 4. Calculate Data Health Score (0 - 100)
    score = 100
    for alert in alerts:
        sev = alert["severity"]
        if sev == "CRITICAL":
            score -= 12
        elif sev == "WARNING":
            score -= 4
        elif sev == "INFO":
            score -= 1

    score = max(0, min(100, score))

    if score >= 90:
        health_status = "Excellent"
        badge_color = "#10B981"  # emerald
    elif score >= 75:
        health_status = "Good"
        badge_color = "#3B82F6"  # blue
    elif score >= 50:
        health_status = "Fair"
        badge_color = "#F59E0B"  # amber
    else:
        health_status = "Poor"
        badge_color = "#EF4444"  # red

    return {
        "health_score": score,
        "health_status": health_status,
        "badge_color": badge_color,
        "total_alerts": len(alerts),
        "critical_count": sum(1 for a in alerts if a["severity"] == "CRITICAL"),
        "warning_count": sum(1 for a in alerts if a["severity"] == "WARNING"),
        "info_count": sum(1 for a in alerts if a["severity"] == "INFO"),
        "alerts": alerts,
    }

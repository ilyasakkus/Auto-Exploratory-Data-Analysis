"""JSON export renderer for SparkAutoEDA profile reports."""

import json
import math
from typing import Any, Dict


def clean_json_value(val: Any) -> Any:
    """Recursively clean data for valid standard JSON serialization."""
    if isinstance(val, float):
        if math.isnan(val) or math.isinf(val):
            return None
        return round(val, 4)
    elif isinstance(val, dict):
        return {str(k): clean_json_value(v) for k, v in val.items()}
    elif isinstance(val, (list, tuple)):
        return [clean_json_value(v) for v in val]
    elif hasattr(val, "item"):  # numpy scalar
        return clean_json_value(val.item())
    return val


def render_json(profile_data: Dict[str, Any], indent: int = 2) -> str:
    """Serialize profile data into a clean, formatted JSON string."""
    cleaned = clean_json_value(profile_data)
    return json.dumps(cleaned, indent=indent, default=str)

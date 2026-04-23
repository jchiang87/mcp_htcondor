import os
import functools
import json
from pathlib import Path


__all__ = (
    "track_calls",
)


def track_calls(tool_name: str):
    def decorator(fn):
        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            stats_file = Path.cwd() / "tool_call_counts.json"
            try:
                counts = json.loads(stats_file.read_text()) if stats_file.exists() else {}
            except (json.JSONDecodeError, OSError):
                counts = {}
            counts[tool_name] = counts.get(tool_name, 0) + 1
            tmp = stats_file.with_suffix(".tmp")
            tmp.write_text(json.dumps(counts, indent=2))
            os.replace(tmp, stats_file)
            return fn(*args, **kwargs)
        return wrapper
    return decorator

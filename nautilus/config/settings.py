import os

from nautilus.config.loader import load_databases_from_env


def get_settings():
    max_row_limit = int(os.environ.get("NAUTILUS_MAX_ROW_LIMIT", "0"))
    if max_row_limit <= 0:
        max_row_limit = int(os.environ.get("NAUTILUS_MAX_ROWS", "200"))
    if max_row_limit <= 0:
        max_row_limit = 200
    max_row_limit = min(max(1, max_row_limit), 10000)

    default_row_limit = int(os.environ.get("NAUTILUS_DEFAULT_ROW_LIMIT", "50"))
    if default_row_limit <= 0:
        default_row_limit = 50
    default_row_limit = min(max(1, default_row_limit), max_row_limit)

    timeout_ms = int(os.environ.get("NAUTILUS_QUERY_TIMEOUT_MS", "0"))
    if timeout_ms <= 0:
        sec = int(os.environ.get("NAUTILUS_QUERY_TIMEOUT_SECONDS", "0"))
        timeout_ms = int(sec * 1000) if sec > 0 else 5000
    timeout_ms = min(max(1000, timeout_ms), 600000)

    read_only_mode = os.environ.get("NAUTILUS_READ_ONLY_MODE", "true").lower() not in ("false", "0", "no")

    return {
        "query_max_length": int(os.environ.get("NAUTILUS_QUERY_MAX_LENGTH", "2000")),
        "default_row_limit": default_row_limit,
        "max_row_limit": max_row_limit,
        "query_timeout_ms": timeout_ms,
        "read_only_mode": read_only_mode,
        "databases": load_databases_from_env()
    }

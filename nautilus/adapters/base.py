"""Helpers compartilhados pelos adapters — resolução de timeouts por conexão."""

DEFAULT_QUERY_TIMEOUT_MS = 30000
DEFAULT_CONNECT_TIMEOUT_MS = 5000


def query_timeout_ms(configs, connection_id):
    """Timeout de statement (ms) da conexão, com fallback de fábrica."""
    cfg = configs.get(connection_id) or {}
    return int(cfg.get("query_timeout_ms") or DEFAULT_QUERY_TIMEOUT_MS)


def connect_timeout_s(configs, connection_id):
    """Timeout para abrir a conexão, em segundos inteiros (drivers usam s)."""
    cfg = configs.get(connection_id) or {}
    ms = int(cfg.get("connect_timeout_ms") or DEFAULT_CONNECT_TIMEOUT_MS)
    return max(1, round(ms / 1000.0))


def query_timeout_s(configs, connection_id):
    """Timeout de statement em segundos inteiros (drivers sem granularidade de ms)."""
    ms = query_timeout_ms(configs, connection_id)
    return max(1, round(ms / 1000.0))

import os
import logging

from nautilus.config.loader import (
    DEFAULT_CONFIG_PATH,
    FACTORY_DEFAULTS,
    ConfigError,
    load_config,
    _resolve_defaults,
)

logger = logging.getLogger("nautilus")


def _settings_from(defaults, databases):
    max_row_limit = min(max(1, defaults["max_rows"]), 10000)
    default_row_limit = min(max(1, defaults["default_rows"]), max_row_limit)
    return {
        "query_max_length": defaults["query_max_length"],
        "default_row_limit": default_row_limit,
        "max_row_limit": max_row_limit,
        "query_timeout_ms": defaults["query_timeout_ms"],
        "connect_timeout_ms": defaults["connect_timeout_ms"],
        "read_only_mode": defaults["read_only"],
        "databases": databases,
    }


def get_settings():
    path = os.environ.get("NAUTILUS_CONFIG", DEFAULT_CONFIG_PATH)
    explicit = "NAUTILUS_CONFIG" in os.environ

    if not os.path.exists(path):
        if explicit:
            raise ConfigError(f"Arquivo de configuração não encontrado: {path}")
        logger.warning(
            f"Nenhum arquivo de configuração em '{path}'. "
            f"Defina NAUTILUS_CONFIG ou crie {DEFAULT_CONFIG_PATH}. Servidor sobe sem conexões."
        )
        return _settings_from(_resolve_defaults(None), {})

    cfg = load_config(path)
    return _settings_from(cfg["defaults"], cfg["databases"])

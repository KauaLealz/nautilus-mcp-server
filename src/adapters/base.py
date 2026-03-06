"""
Classe base e helpers compartilhados pelos adapters.
"""
from src.domain.models import ConnectionInfo

from src.config.settings import DatabaseConfig


def connection_info_from_config(connection_id: str, config: DatabaseConfig) -> ConnectionInfo:
    """Constrói ConnectionInfo a partir de DatabaseConfig."""
    return ConnectionInfo(
        connection_id=connection_id,
        type=config.type,
        read_only=config.read_only,
    )

"""
Histórico em memória das últimas N queries de leitura (para auditoria/debug).
"""
from collections import deque
import time

from src.domain.models import QueryHistoryEntry

_MAX_ENTRIES = 100
_history: deque[QueryHistoryEntry] = deque(maxlen=_MAX_ENTRIES)


def append(connection_id: str, query: str, row_count: int | None = None) -> None:
    entry = QueryHistoryEntry(
        connection_id=connection_id,
        query=query,
        timestamp=time.time(),
        row_count=row_count,
    )
    _history.append(entry)


def list_entries(limit: int = 10) -> list[QueryHistoryEntry]:
    """Retorna as últimas N entradas (mais recente primeiro)."""
    return list(_history)[-(limit):][::-1]

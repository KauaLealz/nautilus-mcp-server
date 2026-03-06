"""
Pendências de write para human-in-the-loop (confirm_write).
"""
import time
import uuid

from src.domain.models import PendingWrite

_store: dict[str, PendingWrite] = {}


def add(connection_id: str, command: str) -> str:
    pid = str(uuid.uuid4())[:8]
    _store[pid] = PendingWrite(
        id=pid,
        connection_id=connection_id,
        command=command,
        created_at=time.time(),
    )
    return pid


def get(pending_id: str) -> PendingWrite | None:
    return _store.get(pending_id)


def pop(pending_id: str) -> PendingWrite | None:
    return _store.pop(pending_id, None)

"""
Carrega queries salvas de um arquivo JSON (nome -> template com placeholders).
"""
import json
import os
from pathlib import Path

from src.domain.models import SavedQuery

_QUERIES: dict[str, SavedQuery] = {}
_LOADED = False


def _default_path() -> Path:
    return Path(os.getenv("NAUTILUS_SAVED_QUERIES_JSON", "saved_queries.json"))


def load(path: Path | None = None) -> dict[str, SavedQuery]:
    global _QUERIES, _LOADED
    if _LOADED and not path:
        return _QUERIES
    p = path or _default_path()
    if not p.is_file():
        _LOADED = True
        return _QUERIES
    try:
        with open(p, encoding="utf-8") as f:
            data = json.load(f)
    except Exception:
        _LOADED = True
        return _QUERIES
    for item in data.get("queries", []):
        name = item.get("name")
        if not name:
            continue
        template = item.get("query_template", "")
        params = item.get("parameters", [])
        desc = item.get("description", "")
        _QUERIES[name] = SavedQuery(name=name, description=desc, query_template=template, parameters=params)
    _LOADED = True
    return _QUERIES


def list_saved() -> list[SavedQuery]:
    return list(load().values())


def get(name: str) -> SavedQuery | None:
    return load().get(name)


def execute_saved_query(name: str, params: dict[str, str]) -> str:
    """Substitui placeholders no template. params: {'param1': 'value1', ...}."""
    sq = get(name)
    if not sq:
        raise KeyError(f"Query salva não encontrada: {name}")
    q = sq.query_template
    for p in sq.parameters:
        q = q.replace("{{" + p + "}}", str(params.get(p, "")))
    return q

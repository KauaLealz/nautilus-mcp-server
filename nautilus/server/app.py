import os
import sys
import logging
import threading
from bson import ObjectId
from datetime import datetime
from decimal import Decimal

from mcp.server.fastmcp import FastMCP

from nautilus.config import get_settings
from nautilus.config.loader import DEFAULT_CONFIG_PATH
from nautilus.core.validator import SqlQueryValidator
from nautilus.adapters import (
    SqliteAdapter, PostgresAdapter, MysqlAdapter, MssqlAdapter, OracleAdapter,
    MongodbAdapter, RedisAdapter
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    stream=sys.stderr
)
logger = logging.getLogger("nautilus")


mcp = FastMCP(
    "Nautilus",
    instructions="Nautilus MCP multicloud: PostgreSQL, MySQL/MariaDB, SQLite, SQL Server, Oracle, MongoDB, Redis. Somente leitura. Primeiro use db_list_connections para obter os connection_id. Tools: db_list_connections, db_list_resources, db_get_metadata, db_describe_indexes, db_query_sql, db_fetch_documents, db_read_cache, db_peek_sample. Limites: padrão 50 linhas/documentos, máximo 200; timeout padrão 30000 ms, configurável por conexão no YAML. Conexões via arquivo YAML (env NAUTILUS_CONFIG, padrão nautilus.yaml); senhas cifradas (enc:...) decifradas com a chave NAUTILUS_KEY. Recurso nautilus://connections: JSON com a mesma lista de conexões."
)


def _config_path() -> str:
    return os.environ.get("NAUTILUS_CONFIG", DEFAULT_CONFIG_PATH)


def _config_mtime():
    try:
        return os.path.getmtime(_config_path())
    except OSError:
        return None


def _build_state():
    settings = get_settings()
    databases = settings["databases"]

    sqlite_conns = {cid: cfg for cid, cfg in databases.items() if cfg["type"] == "sqlite"}
    postgres_conns = {cid: cfg for cid, cfg in databases.items() if cfg["type"] == "postgresql"}
    mysql_conns = {cid: cfg for cid, cfg in databases.items() if cfg["type"] == "mysql"}
    mssql_conns = {cid: cfg for cid, cfg in databases.items() if cfg["type"] == "sqlserver"}
    oracle_conns = {cid: cfg for cid, cfg in databases.items() if cfg["type"] == "oracle"}
    mongodb_conns = {cid: cfg for cid, cfg in databases.items() if cfg["type"] == "mongodb"}
    redis_conns = {cid: cfg for cid, cfg in databases.items() if cfg["type"] == "redis"}

    adapters = {
        "sqlite": SqliteAdapter(sqlite_conns) if sqlite_conns else None,
        "postgresql": PostgresAdapter(postgres_conns) if postgres_conns else None,
        "mysql": MysqlAdapter(mysql_conns) if mysql_conns else None,
        "sqlserver": MssqlAdapter(mssql_conns) if mssql_conns else None,
        "oracle": OracleAdapter(oracle_conns) if oracle_conns else None,
        "mongodb": MongodbAdapter(mongodb_conns, settings["query_timeout_ms"]) if mongodb_conns else None,
        "redis": RedisAdapter(redis_conns, settings["query_timeout_ms"]) if redis_conns else None,
    }

    sql_validator = SqlQueryValidator(
        max_length=settings["query_max_length"],
        max_rows_cap=settings["max_row_limit"]
    )

    return {
        "settings": settings,
        "databases": databases,
        "adapters": adapters,
        "sql_validator": sql_validator,
    }


_state_lock = threading.Lock()
_state = None
_state_mtime = None


def get_state():
    """Devolve o estado atual (settings/databases/adapters), recarregando do
    YAML quando o arquivo mudou desde a última leitura — sem precisar
    reiniciar o servidor. Conexões abertas pelos adapters antigos são
    fechadas antes de serem descartadas."""
    global _state, _state_mtime
    mtime = _config_mtime()
    with _state_lock:
        if _state is None or mtime != _state_mtime:
            old_state = _state
            _state = _build_state()
            _state_mtime = mtime
            if old_state:
                logger.info("Config do Nautilus mudou — recarregando conexões.")
                for adapter in old_state["adapters"].values():
                    if adapter and hasattr(adapter, "close_all"):
                        try:
                            adapter.close_all()
                        except Exception:
                            pass
        return _state


def resolve_connection_id(connection_id: str) -> str:
    databases = get_state()["databases"]
    target = connection_id.strip().lower()
    for cid in databases:
        if cid.lower() == target:
            return cid
    raise ValueError(f"Conexao nao encontrada: {connection_id}")


def get_db_adapter(cid: str):
    state = get_state()
    cfg = state["databases"][cid]
    t = cfg["type"]
    adapter = state["adapters"].get(t)
    if not adapter:
        raise ValueError(f"Tipo de conexao nao suportado ou nao configurado: {t}")
    return adapter, t


def deep_sanitize(value):
    if value is None:
        return None
    if isinstance(value, ObjectId):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, bytes):
        try:
            return value.decode("utf-8")
        except Exception:
            return value.hex()
    if isinstance(value, list):
        return [deep_sanitize(v) for v in value]
    if isinstance(value, dict):
        return {k: deep_sanitize(v) for k, v in value.items()}
    return value


def main():
    # Importar registra as tools e o resource no `mcp` via decorator.
    from nautilus.server import resources, tools  # noqa: F401
    mcp.run()


if __name__ == "__main__":
    main()

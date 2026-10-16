import sys
import logging
from bson import ObjectId
from datetime import datetime
from decimal import Decimal

from mcp.server.fastmcp import FastMCP

from nautilus.config import get_settings
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
    instructions="Nautilus MCP multicloud: PostgreSQL, MySQL/MariaDB, SQLite, SQL Server, Oracle, MongoDB, Redis. Somente leitura. Primeiro use db_list_connections para obter os connection_id. Tools: db_list_connections, db_list_resources, db_get_metadata, db_describe_indexes, db_query_sql, db_fetch_documents, db_read_cache, db_peek_sample. Limites: padrão 50 linhas/documentos, máximo 200; timeout padrão 5000 ms (NAUTILUS_QUERY_TIMEOUT_MS). Conexões via env DATABASES__<id>__*. Recurso nautilus://connections: JSON com a mesma lista de conexões."
)
settings = get_settings()
databases = settings["databases"]

sqlite_conns = {cid: cfg for cid, cfg in databases.items() if cfg["type"] == "sqlite"}
postgres_conns = {cid: cfg for cid, cfg in databases.items() if cfg["type"] == "postgresql"}
mysql_conns = {cid: cfg for cid, cfg in databases.items() if cfg["type"] == "mysql"}
mssql_conns = {cid: cfg for cid, cfg in databases.items() if cfg["type"] == "sqlserver"}
oracle_conns = {cid: cfg for cid, cfg in databases.items() if cfg["type"] == "oracle"}
mongodb_conns = {cid: cfg for cid, cfg in databases.items() if cfg["type"] == "mongodb"}
redis_conns = {cid: cfg for cid, cfg in databases.items() if cfg["type"] == "redis"}

sqlite_adapter = SqliteAdapter(sqlite_conns) if sqlite_conns else None
postgres_adapter = PostgresAdapter(postgres_conns) if postgres_conns else None
mysql_adapter = MysqlAdapter(mysql_conns) if mysql_conns else None
mssql_adapter = MssqlAdapter(mssql_conns) if mssql_conns else None
oracle_adapter = OracleAdapter(oracle_conns) if oracle_conns else None
mongodb_adapter = MongodbAdapter(mongodb_conns, settings["query_timeout_ms"]) if mongodb_conns else None
redis_adapter = RedisAdapter(redis_conns, settings["query_timeout_ms"]) if redis_conns else None

sql_validator = SqlQueryValidator(
    max_length=settings["query_max_length"],
    max_rows_cap=settings["max_row_limit"]
)


def resolve_connection_id(connection_id: str) -> str:
    target = connection_id.strip().lower()
    for cid in databases:
        if cid.lower() == target:
            return cid
    raise ValueError(f"Conexao nao encontrada: {connection_id}")


def get_db_adapter(cid: str):
    cfg = databases[cid]
    t = cfg["type"]
    if t == "sqlite" and sqlite_adapter:
        return sqlite_adapter, t
    if t == "postgresql" and postgres_adapter:
        return postgres_adapter, t
    if t == "mysql" and mysql_adapter:
        return mysql_adapter, t
    if t == "sqlserver" and mssql_adapter:
        return mssql_adapter, t
    if t == "oracle" and oracle_adapter:
        return oracle_adapter, t
    if t == "mongodb" and mongodb_adapter:
        return mongodb_adapter, t
    if t == "redis" and redis_adapter:
        return redis_adapter, t
    raise ValueError(f"Tipo de conexao nao suportado ou nao configurado: {t}")


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

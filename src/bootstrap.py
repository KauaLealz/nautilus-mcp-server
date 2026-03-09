"""
Bootstrap: monta adapters, validators e use cases; injeta nas tools.
Segregação por tipo de banco e registro de connection_id -> adapter.
"""
from typing import Any

from src.config.settings import get_settings
from src.domain.query_safety import SqlQueryValidator
from src.adapters.sql.postgres import PostgresAdapter
from src.adapters.sql.mysql import MysqlAdapter
from src.adapters.sql.sqlserver import SqlServerAdapter
from src.adapters.sql.oracle import OracleAdapter
from src.adapters.nosql.mongodb import MongodbAdapter
from src.adapters.nosql.redis_adapter import RedisAdapter
from src.use_cases.connection import ConnectionUseCase
from src.use_cases.execute_query import ExecuteQueryUseCase
from src.use_cases.introspect_schema import IntrospectSchemaUseCase
from src.domain.models import ConnectionInfo


def _build_adapters() -> tuple[dict[str, Any], dict[str, Any]]:
    """Cria um adapter por tipo e mapeia connection_id -> adapter."""
    settings = get_settings()
    databases = settings.databases
    async def _test_connection_fail(_cid: str) -> bool:
        return False

    if not databases:
        return {}, {
            "list_all_connections": lambda: [],
            "test_connection": _test_connection_fail,
            "get_sql_adapter": lambda cid: None,
            "adapters_list": [],
        }

    connection_to_adapter: dict[str, Any] = {}
    adapters_list: list[Any] = []

    # PostgreSQL
    pg_configs = {cid: cfg for cid, cfg in databases.items() if cfg.type == "postgresql"}
    if pg_configs:
        pg_adapter = PostgresAdapter(pg_configs)
        adapters_list.append(pg_adapter)
        for cid in pg_configs:
            connection_to_adapter[cid] = pg_adapter

    # MySQL
    mysql_configs = {cid: cfg for cid, cfg in databases.items() if cfg.type == "mysql"}
    if mysql_configs:
        mysql_adapter = MysqlAdapter(mysql_configs, timeout_seconds=settings.query_timeout_seconds)
        adapters_list.append(mysql_adapter)
        for cid in mysql_configs:
            connection_to_adapter[cid] = mysql_adapter

    # SQL Server
    sqlserver_configs = {cid: cfg for cid, cfg in databases.items() if cfg.type == "sqlserver"}
    if sqlserver_configs:
        sqlserver_adapter = SqlServerAdapter(sqlserver_configs)
        adapters_list.append(sqlserver_adapter)
        for cid in sqlserver_configs:
            connection_to_adapter[cid] = sqlserver_adapter

    # Oracle
    oracle_configs = {cid: cfg for cid, cfg in databases.items() if cfg.type == "oracle"}
    if oracle_configs:
        oracle_adapter = OracleAdapter(oracle_configs, timeout_seconds=settings.query_timeout_seconds)
        adapters_list.append(oracle_adapter)
        for cid in oracle_configs:
            connection_to_adapter[cid] = oracle_adapter

    # MongoDB
    mongo_configs = {cid: cfg for cid, cfg in databases.items() if cfg.type == "mongodb"}
    if mongo_configs:
        mongo_adapter = MongodbAdapter(mongo_configs, timeout_seconds=settings.query_timeout_seconds)
        adapters_list.append(mongo_adapter)
        for cid in mongo_configs:
            connection_to_adapter[cid] = mongo_adapter

    # Redis
    redis_configs = {cid: cfg for cid, cfg in databases.items() if cfg.type == "redis"}
    if redis_configs:
        redis_adapter = RedisAdapter(redis_configs, timeout_seconds=settings.query_timeout_seconds)
        adapters_list.append(redis_adapter)
        for cid in redis_configs:
            connection_to_adapter[cid] = redis_adapter

    def list_all_connections() -> list[ConnectionInfo]:
        result: list[ConnectionInfo] = []
        seen: set[str] = set()
        for adapter in adapters_list:
            for info in adapter.list_connections():
                if info.connection_id not in seen:
                    seen.add(info.connection_id)
                    result.append(info)
        return sorted(result, key=lambda c: c.connection_id)

    async def test_connection(connection_id: str) -> bool:
        adapter, resolved_key = _resolve_adapter_and_key(connection_id)
        if not adapter:
            return False
        return await adapter.test_connection(resolved_key)

    def get_sql_adapter(connection_id: str):
        """Retorna o adapter que atende ao connection_id (para SQL) ou None."""
        adapter, _ = _resolve_adapter_and_key(connection_id)
        return adapter

    def get_adapter(connection_id: str):
        """Retorna o adapter (qualquer tipo) para o connection_id ou None."""
        adapter, _ = _resolve_adapter_and_key(connection_id)
        return adapter

    def _resolve_adapter_and_key(connection_id: str) -> tuple[Any, str | None]:
        """Resolve connection_id de forma case-insensitive. Retorna (adapter, key_usado) ou (None, None)."""
        if not connection_id or not str(connection_id).strip():
            return None, None
        cid = str(connection_id).strip()
        key = next((k for k in connection_to_adapter if k.lower() == cid.lower()), None)
        if key is None:
            return None, None
        return connection_to_adapter[key], key

    return connection_to_adapter, {
        "list_all_connections": list_all_connections,
        "test_connection": test_connection,
        "get_sql_adapter": get_sql_adapter,
        "get_adapter": get_adapter,
        "adapters_list": adapters_list,
    }


def get_connection_use_case() -> ConnectionUseCase:
    """Factory do use case de conexão."""
    _, deps = _build_adapters()
    return ConnectionUseCase(
        list_connections_fn=deps["list_all_connections"],
        test_connection_fn=deps["test_connection"],
    )


def get_execute_query_use_case() -> ExecuteQueryUseCase:
    """Factory do use case de execução de query."""
    settings = get_settings()
    _, deps = _build_adapters()
    validator = SqlQueryValidator(
        max_length=settings.query_max_length,
        allow_write=settings.allow_write,
        max_rows_cap=settings.max_rows,
    )
    return ExecuteQueryUseCase(
        get_sql_adapter_fn=deps["get_sql_adapter"],
        validator=validator,
        max_rows=settings.max_rows,
        timeout_seconds=settings.query_timeout_seconds,
    )


def get_introspect_schema_use_case() -> IntrospectSchemaUseCase:
    """Factory do use case de introspectação de schema."""
    _, deps = _build_adapters()
    return IntrospectSchemaUseCase(get_sql_adapter_fn=deps["get_sql_adapter"])


def get_adapter(connection_id: str):
    """Retorna o adapter (SQL ou NoSQL) para o connection_id, ou None."""
    _, deps = _build_adapters()
    return deps["get_adapter"](connection_id)

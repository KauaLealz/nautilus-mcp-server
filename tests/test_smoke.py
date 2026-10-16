"""Smoke test da reorganização: o pacote importa e o servidor registra as mesmas tools."""
import asyncio

EXPECTED_TOOLS = {
    "db_list_connections",
    "db_list_resources",
    "db_get_metadata",
    "db_describe_indexes",
    "db_query_sql",
    "db_fetch_documents",
    "db_read_cache",
    "db_peek_sample",
}


def test_adapters_importable():
    from nautilus.adapters import (
        SqliteAdapter, PostgresAdapter, MysqlAdapter, MssqlAdapter,
        OracleAdapter, MongodbAdapter, RedisAdapter,
    )
    for cls in (SqliteAdapter, PostgresAdapter, MysqlAdapter, MssqlAdapter,
                OracleAdapter, MongodbAdapter, RedisAdapter):
        assert hasattr(cls, "test_connection")


def test_server_registers_all_tools():
    from nautilus.server import app, tools, resources  # noqa: F401
    tool_list = asyncio.run(app.mcp.list_tools())
    names = {t.name for t in tool_list}
    assert EXPECTED_TOOLS.issubset(names), f"faltam tools: {EXPECTED_TOOLS - names}"


def test_server_registers_connections_resource():
    from nautilus.server import app, resources  # noqa: F401
    resource_list = asyncio.run(app.mcp.list_resources())
    uris = {str(r.uri) for r in resource_list}
    assert "nautilus://connections" in uris

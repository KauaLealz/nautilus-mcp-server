"""
Tools MCP de descoberta: list_databases, get_table_sample, get_schema_summary, export_schema_json,
list_indexes, list_views, get_foreign_keys, get_table_relationships, get_column_stats, get_row_count.
"""
import json

from src.bootstrap import get_adapter
from src.config.settings import get_settings
from src.utils.error_handler import ErrorHandler
from src.utils.formatter import (
    ResultFormatter,
    format_schema_summary,
    format_indexes,
    format_views,
    format_foreign_keys,
    format_table_relationships,
    format_column_stats,
)


def _get_sql_adapter(connection_id: str):
    adapter = get_adapter(connection_id)
    if adapter is None or not hasattr(adapter, "list_tables"):
        return None
    return adapter


def register_discovery_tools(mcp):
    """Registra tools de descoberta de schema e dados."""

    @mcp.tool()
    async def list_databases(connection_id: str):
        """Lista os databases disponíveis na conexão SQL (PostgreSQL, MySQL, SQL Server)."""
        try:
            adapter = _get_sql_adapter(connection_id.strip())
            if not adapter or not hasattr(adapter, "list_databases"):
                return f"Conexão '{connection_id}' não encontrada ou não suporta list_databases."
            dbs = await adapter.list_databases(connection_id.strip())
            return "Databases:\n" + "\n".join(f"  - {d}" for d in dbs) if dbs else "Nenhum database listado."
        except Exception as e:
            return ErrorHandler.format_for_agent(ErrorHandler.handle(e, "list_databases"))

    @mcp.tool()
    async def get_table_sample(
        connection_id: str,
        table_name: str,
        schema: str | None = None,
        limit: int = 5,
    ):
        """Retorna N linhas de amostra de uma tabela (sem montar SELECT)."""
        try:
            adapter = _get_sql_adapter(connection_id.strip())
            if not adapter or not hasattr(adapter, "get_table_sample"):
                return f"Conexão '{connection_id}' não suporta get_table_sample."
            cap = get_settings().max_rows
            result = await adapter.get_table_sample(
                connection_id.strip(), table_name.strip(),
                schema=schema.strip() if schema else None,
                limit=min(max(1, limit), cap),
            )
            return ResultFormatter.format_query_result(result, max_display_rows=limit)
        except Exception as e:
            return ErrorHandler.format_for_agent(ErrorHandler.handle(e, "get_table_sample"))

    @mcp.tool()
    async def get_schema_summary(
        connection_id: str,
        schema: str | None = None,
        include_row_count: bool = False,
    ):
        """Resumo de todas as tabelas do schema (nome, quantidade de colunas e opcionalmente de linhas)."""
        try:
            adapter = _get_sql_adapter(connection_id.strip())
            if not adapter or not hasattr(adapter, "get_schema_summary"):
                return f"Conexão '{connection_id}' não suporta get_schema_summary."
            summaries = await adapter.get_schema_summary(
                connection_id.strip(),
                schema=schema.strip() if schema else None,
                include_row_count=include_row_count,
            )
            return format_schema_summary(summaries)
        except Exception as e:
            return ErrorHandler.format_for_agent(ErrorHandler.handle(e, "get_schema_summary"))

    @mcp.tool()
    async def export_schema_json(connection_id: str, schema: str | None = None):
        """Exporta o schema (tabelas e colunas) como JSON para uso no contexto do agente."""
        try:
            adapter = _get_sql_adapter(connection_id.strip())
            if not adapter or not hasattr(adapter, "export_schema_json"):
                return f"Conexão '{connection_id}' não suporta export_schema_json."
            data = await adapter.export_schema_json(
                connection_id.strip(),
                schema=schema.strip() if schema else None,
            )
            return json.dumps(data, ensure_ascii=False, indent=2)
        except Exception as e:
            return ErrorHandler.format_for_agent(ErrorHandler.handle(e, "export_schema_json"))

    @mcp.tool()
    async def list_indexes(
        connection_id: str,
        table_name: str,
        schema: str | None = None,
    ):
        """Lista índices de uma tabela (nome, colunas, se é único)."""
        try:
            adapter = _get_sql_adapter(connection_id.strip())
            if not adapter or not hasattr(adapter, "list_indexes"):
                return f"Conexão '{connection_id}' não suporta list_indexes."
            indexes = await adapter.list_indexes(
                connection_id.strip(),
                table_name.strip(),
                schema=schema.strip() if schema else None,
            )
            return format_indexes(indexes)
        except Exception as e:
            return ErrorHandler.format_for_agent(ErrorHandler.handle(e, "list_indexes"))

    @mcp.tool()
    async def list_views(connection_id: str, schema: str | None = None):
        """Lista views do schema e, quando disponível, a definição."""
        try:
            adapter = _get_sql_adapter(connection_id.strip())
            if not adapter or not hasattr(adapter, "list_views"):
                return f"Conexão '{connection_id}' não suporta list_views."
            views = await adapter.list_views(
                connection_id.strip(),
                schema=schema.strip() if schema else None,
            )
            return format_views(views)
        except Exception as e:
            return ErrorHandler.format_for_agent(ErrorHandler.handle(e, "list_views"))

    @mcp.tool()
    async def get_foreign_keys(
        connection_id: str,
        table_name: str,
        schema: str | None = None,
    ):
        """Lista chaves estrangeiras de uma tabela (tabela/coluna referenciada)."""
        try:
            adapter = _get_sql_adapter(connection_id.strip())
            if not adapter or not hasattr(adapter, "get_foreign_keys"):
                return f"Conexão '{connection_id}' não suporta get_foreign_keys."
            fks = await adapter.get_foreign_keys(
                connection_id.strip(),
                table_name.strip(),
                schema=schema.strip() if schema else None,
            )
            return format_foreign_keys(fks)
        except Exception as e:
            return ErrorHandler.format_for_agent(ErrorHandler.handle(e, "get_foreign_keys"))

    @mcp.tool()
    async def get_table_relationships(connection_id: str, schema: str | None = None):
        """Grafo de relacionamentos: tabela A -> tabela B (por FK)."""
        try:
            adapter = _get_sql_adapter(connection_id.strip())
            if not adapter or not hasattr(adapter, "get_table_relationships"):
                return f"Conexão '{connection_id}' não suporta get_table_relationships."
            rels = await adapter.get_table_relationships(
                connection_id.strip(),
                schema=schema.strip() if schema else None,
            )
            return format_table_relationships(rels)
        except Exception as e:
            return ErrorHandler.format_for_agent(ErrorHandler.handle(e, "get_table_relationships"))

    @mcp.tool()
    async def get_row_count(
        connection_id: str,
        table_name: str,
        schema: str | None = None,
        where_clause: str | None = None,
    ):
        """Contagem de linhas de uma tabela (opcionalmente com filtro WHERE)."""
        try:
            adapter = _get_sql_adapter(connection_id.strip())
            if not adapter or not hasattr(adapter, "get_row_count"):
                return f"Conexão '{connection_id}' não suporta get_row_count."
            cnt = await adapter.get_row_count(
                connection_id.strip(),
                table_name.strip(),
                schema=schema.strip() if schema else None,
                where_clause=where_clause.strip() if where_clause else None,
            )
            return f"Total de linhas: {cnt}"
        except Exception as e:
            return ErrorHandler.format_for_agent(ErrorHandler.handle(e, "get_row_count"))

    @mcp.tool()
    async def get_column_stats(
        connection_id: str,
        table_name: str,
        schema: str | None = None,
        column_names: str | None = None,
    ):
        """Estatísticas de colunas (min, max, avg, count, nulls, distinct). Colunas numéricas e texto."""
        try:
            adapter = _get_sql_adapter(connection_id.strip())
            if not adapter or not hasattr(adapter, "get_column_stats"):
                return f"Conexão '{connection_id}' não suporta get_column_stats."
            cols = [c.strip() for c in column_names.split(",")] if column_names and column_names.strip() else None
            stats = await adapter.get_column_stats(
                connection_id.strip(),
                table_name.strip(),
                schema=schema.strip() if schema else None,
                column_names=cols,
            )
            return format_column_stats(stats)
        except Exception as e:
            return ErrorHandler.format_for_agent(ErrorHandler.handle(e, "get_column_stats"))

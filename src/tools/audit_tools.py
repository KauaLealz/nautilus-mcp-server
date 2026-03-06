"""
Tools MCP de auditoria e ajuda: query_history, get_connection_capabilities, suggest_tables.
"""
import time

from src.bootstrap import get_adapter, get_connection_use_case
from src.utils.error_handler import ErrorHandler
from src.utils.query_history import list_entries as history_list_entries


def register_audit_tools(mcp):
    """Registra tools de histórico, capacidades e sugestão de tabelas."""

    @mcp.tool()
    async def query_history(limit: int = 10) -> str:
        """Lista as últimas N queries de leitura executadas (connection_id, query, timestamp, row_count)."""
        try:
            entries = history_list_entries(limit=min(limit, 50))
            if not entries:
                return "Nenhuma query no histórico."
            lines = ["Últimas queries (mais recente primeiro):", ""]
            for e in entries:
                ts = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(e.timestamp))
                rc = f", {e.row_count} linhas" if e.row_count is not None else ""
                lines.append(f"  [{ts}] {e.connection_id}{rc}")
                lines.append(f"    {e.query[:100]}..." if len(e.query) > 100 else f"    {e.query}")
            return "\n".join(lines)
        except Exception as e:
            return ErrorHandler.format_for_agent(ErrorHandler.handle(e, "query_history"))

    @mcp.tool()
    async def get_connection_capabilities(connection_id: str) -> str:
        """Retorna as capacidades suportadas pela conexão (list_tables, describe_table, explain, list_indexes, etc.)."""
        try:
            connections = get_connection_use_case().list_connections()
            conn_info = next((c for c in connections if c.connection_id == connection_id.strip()), None)
            if not conn_info:
                return f"Conexão '{connection_id}' não encontrada."
            adapter = get_adapter(connection_id.strip())
            if not adapter:
                return f"Sem adapter para '{connection_id}'."
            caps = []
            for method in (
                "list_connections", "test_connection", "list_tables", "describe_table",
                "execute_read_only", "list_databases", "get_table_sample", "get_schema_summary",
                "export_schema_json", "explain_query_sql", "validate_query_sql",
                "list_indexes", "list_views", "get_foreign_keys", "get_table_relationships",
                "get_row_count", "get_column_stats", "suggest_tables",
            ):
                if hasattr(adapter, method):
                    caps.append(f"  - {method}")
            nosql = []
            if hasattr(adapter, "list_collections"):
                nosql.extend(["list_collections", "find_documents", "aggregate"])
            if hasattr(adapter, "get_key"):
                nosql.extend(["redis_get", "redis_keys", "key_type", "key_ttl", "mget"])
            for m in nosql:
                caps.append(f"  - {m}")
            return f"Capacidades para '{connection_id}' (tipo={conn_info.type}):\n" + "\n".join(sorted(set(caps)))
        except Exception as e:
            return ErrorHandler.format_for_agent(ErrorHandler.handle(e, "get_connection_capabilities"))

    @mcp.tool()
    async def suggest_tables(connection_id: str, search_term: str, schema: str | None = None) -> str:
        """Sugere tabelas/colunas cujo nome contém o termo (ex.: 'vendas', 'email'). Apenas SQL."""
        try:
            adapter = get_adapter(connection_id.strip())
            if not adapter or not hasattr(adapter, "suggest_tables"):
                return f"Conexão '{connection_id}' não suporta suggest_tables (apenas SQL)."
            rows = await adapter.suggest_tables(
                connection_id.strip(),
                search_term.strip(),
                schema=schema.strip() if schema else None,
            )
            if not rows:
                return f"Nenhuma tabela ou coluna encontrada para o termo '{search_term}'."
            lines = [f"Tabelas/colunas que contêm '{search_term}':", ""]
            for sch, tbl, col in rows[:100]:
                lines.append(f"  - {sch}.{tbl}.{col}")
            if len(rows) > 100:
                lines.append(f"\n... e mais {len(rows) - 100} resultados.")
            return "\n".join(lines)
        except Exception as e:
            return ErrorHandler.format_for_agent(ErrorHandler.handle(e, "suggest_tables"))

"""
Tools MCP de comparação: compare_schemas, run_same_query.
"""
from src.bootstrap import get_adapter, get_execute_query_use_case
from src.utils.error_handler import ErrorHandler
from src.utils.formatter import ResultFormatter
from src.domain.query_safety import SqlQueryValidator
from src.config.settings import get_settings


def register_comparison_tools(mcp):
    """Registra tools para comparar schemas e executar a mesma query em múltiplas conexões."""

    @mcp.tool()
    async def compare_schemas(connection_id_1: str, connection_id_2: str, schema: str | None = None) -> str:
        """Compara os schemas de duas conexões SQL do mesmo tipo (tabelas/colunas). Lista diferenças em texto."""
        try:
            adapter1 = get_adapter(connection_id_1.strip())
            adapter2 = get_adapter(connection_id_2.strip())
            if not adapter1 or not adapter2 or not hasattr(adapter1, "export_schema_json") or not hasattr(adapter2, "export_schema_json"):
                return "Ambas as conexões devem ser SQL e suportar export_schema_json."
            s1 = await adapter1.export_schema_json(connection_id_1.strip(), schema=schema.strip() if schema else None)
            s2 = await adapter2.export_schema_json(connection_id_2.strip(), schema=schema.strip() if schema else None)
            tables1 = {f"{t['schema']}.{t['table']}": set(f"{c['name']}:{c['data_type']}" for c in t["columns"]) for t in s1["tables"]}
            tables2 = {f"{t['schema']}.{t['table']}": set(f"{c['name']}:{c['data_type']}" for c in t["columns"]) for t in s2["tables"]}
            only_1 = set(tables1) - set(tables2)
            only_2 = set(tables2) - set(tables1)
            common = set(tables1) & set(tables2)
            diff_cols = []
            for t in common:
                if tables1[t] != tables2[t]:
                    diff_cols.append((t, tables1[t] - tables2[t], tables2[t] - tables1[t]))
            lines = [
                f"Comparação: {connection_id_1} vs {connection_id_2}",
                "",
                f"Só em {connection_id_1}: {', '.join(sorted(only_1)) or 'nenhuma'}",
                f"Só em {connection_id_2}: {', '.join(sorted(only_2)) or 'nenhuma'}",
                "",
            ]
            if diff_cols:
                lines.append("Diferenças de colunas por tabela:")
                for tbl, only_a, only_b in diff_cols:
                    lines.append(f"  {tbl}: em 1 não em 2: {only_a}; em 2 não em 1: {only_b}")
            return "\n".join(lines)
        except Exception as e:
            return ErrorHandler.format_for_agent(ErrorHandler.handle(e, "compare_schemas"))

    @mcp.tool()
    async def run_same_query(connection_ids: str, query: str) -> str:
        """Executa a mesma query (read-only) em 2+ conexões. connection_ids: separados por vírgula. Retorna resultados lado a lado (contagem por conexão)."""
        try:
            validator = SqlQueryValidator(max_length=get_settings().query_max_length, allow_write=get_settings().allow_write)
            validator.sanitize_or_raise(query.strip())
            ids = [cid.strip() for cid in connection_ids.split(",") if cid.strip()][:5]
            if len(ids) < 2:
                return "Forneça pelo menos 2 connection_ids separados por vírgula."
            use_case = get_execute_query_use_case()
            lines = ["Resultados por conexão:", ""]
            for cid in ids:
                try:
                    result = await use_case.execute(cid, query.strip())
                    lines.append(f"  {cid}: {result.row_count} linhas")
                    lines.append(ResultFormatter.format_query_result(result, max_display_rows=5))
                    lines.append("")
                except Exception as ex:
                    lines.append(f"  {cid}: ERRO - {ex}")
                    lines.append("")
            return "\n".join(lines)
        except Exception as e:
            return ErrorHandler.format_for_agent(ErrorHandler.handle(e, "run_same_query"))

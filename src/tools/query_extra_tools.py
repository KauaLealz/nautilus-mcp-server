"""
Tools MCP extras para SQL: explain_query_sql, validate_query_sql, execute_query_sql_as_csv, execute_query_sql_as_json.
"""
from src.bootstrap import get_adapter, get_execute_query_use_case
from src.utils.error_handler import ErrorHandler
from src.utils.formatter import query_result_to_csv, query_result_to_json
from src.domain.query_safety import QuerySafetyError, SqlQueryValidator
from src.config.settings import get_settings


def register_query_extra_tools(mcp):
    """Registra explain, validate e export CSV/JSON."""

    @mcp.tool()
    async def explain_query_sql(connection_id: str, query: str) -> str:
        """Retorna o plano de execução da query (EXPLAIN) sem executá-la. Ajuda a otimizar e detectar full table scan."""
        try:
            adapter = get_adapter(connection_id.strip())
            if not adapter or not hasattr(adapter, "explain_query_sql"):
                return f"Conexão '{connection_id}' não suporta explain."
            s = get_settings()
            validator = SqlQueryValidator(max_length=s.query_max_length, allow_write=s.allow_write, max_rows_cap=s.max_rows)
            validator.sanitize_or_raise(query.strip())
            plan = await adapter.explain_query_sql(connection_id.strip(), query.strip())
            return f"Plano de execução:\n{plan}"
        except QuerySafetyError as e:
            return str(e)
        except Exception as e:
            return ErrorHandler.format_for_agent(ErrorHandler.handle(e, "explain_query_sql"))

    @mcp.tool()
    async def validate_query_sql(connection_id: str, query: str) -> str:
        """Valida apenas a sintaxe da query (sem executar). Retorna se é válida ou mensagem de erro."""
        try:
            adapter = get_adapter(connection_id.strip())
            if not adapter or not hasattr(adapter, "validate_query_sql"):
                return f"Conexão '{connection_id}' não suporta validate_query_sql."
            s = get_settings()
            validator = SqlQueryValidator(max_length=s.query_max_length, allow_write=s.allow_write, max_rows_cap=s.max_rows)
            validator.sanitize_or_raise(query.strip())
            ok = await adapter.validate_query_sql(connection_id.strip(), query.strip())
            return "Query válida." if ok else "Query inválida."
        except QuerySafetyError as e:
            return str(e)
        except Exception as e:
            return ErrorHandler.format_for_agent(ErrorHandler.handle(e, "validate_query_sql"))

    @mcp.tool()
    async def execute_query_sql_as_csv(
        connection_id: str,
        query: str,
        max_rows: int | None = None,
    ) -> str:
        """Executa uma query SQL de leitura e retorna o resultado em CSV. max_rows: limite desejado (respeitando o cap do servidor)."""
        try:
            use_case = get_execute_query_use_case()
            result = await use_case.execute(connection_id.strip(), query.strip(), max_rows=max_rows)
            return query_result_to_csv(result)
        except QuerySafetyError as e:
            return str(e)
        except Exception as e:
            return ErrorHandler.format_for_agent(ErrorHandler.handle(e, "execute_query_sql_as_csv"))

    @mcp.tool()
    async def execute_query_sql_as_json(
        connection_id: str,
        query: str,
        max_rows: int | None = None,
    ) -> str:
        """Executa uma query SQL de leitura e retorna o resultado como JSON array. max_rows: limite desejado (respeitando o cap)."""
        try:
            use_case = get_execute_query_use_case()
            result = await use_case.execute(connection_id.strip(), query.strip(), max_rows=max_rows)
            return query_result_to_json(result)
        except QuerySafetyError as e:
            return str(e)
        except Exception as e:
            return ErrorHandler.format_for_agent(ErrorHandler.handle(e, "execute_query_sql_as_json"))

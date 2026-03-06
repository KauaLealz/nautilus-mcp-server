"""
Tools MCP para queries salvas: list_saved_queries, execute_saved_query.
"""
import json

from src.config.saved_queries import list_saved, get, execute_saved_query as resolve_saved
from src.bootstrap import get_execute_query_use_case
from src.utils.error_handler import ErrorHandler
from src.utils.formatter import ResultFormatter


def register_saved_queries_tools(mcp):
    """Registra tools de queries salvas (arquivo saved_queries.json)."""

    @mcp.tool()
    async def list_saved_queries() -> str:
        """Lista as queries salvas disponíveis (nome, descrição, parâmetros). Configure em saved_queries.json."""
        try:
            queries = list_saved()
            if not queries:
                return "Nenhuma query salva. Crie o arquivo saved_queries.json com chave 'queries' e lista de {name, description, query_template, parameters}."
            lines = ["Queries salvas:", ""]
            for q in queries:
                params = ", ".join(q.parameters) if q.parameters else "nenhum"
                lines.append(f"  - {q.name}: {q.description or '(sem descrição)'} | Parâmetros: {params}")
            return "\n".join(lines)
        except Exception as e:
            return ErrorHandler.format_for_agent(ErrorHandler.handle(e, "list_saved_queries"))

    @mcp.tool()
    async def execute_saved_query(
        connection_id: str,
        query_name: str,
        params_json: str = "{}",
    ) -> str:
        """Executa uma query salva substituindo placeholders. params_json: ex. {"param1": "valor1"}. Placeholders no template: {{param1}}."""
        try:
            try:
                params = json.loads(params_json) if params_json.strip() else {}
            except json.JSONDecodeError:
                return "params_json inválido. Use um objeto JSON, ex: {\"param1\": \"valor1\"}."
            sql = resolve_saved(query_name, params)
            use_case = get_execute_query_use_case()
            result = await use_case.execute(connection_id.strip(), sql)
            return ResultFormatter.format_query_result(result)
        except KeyError as e:
            return f"Query salva não encontrada ou parâmetro faltando: {e}"
        except Exception as e:
            return ErrorHandler.format_for_agent(ErrorHandler.handle(e, "execute_saved_query"))

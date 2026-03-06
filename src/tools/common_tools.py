"""
Tools MCP comuns: listar conexões e testar conectividade.
"""
from src.bootstrap import get_connection_use_case
from src.utils.error_handler import ErrorHandler
from src.utils.formatter import ResultFormatter


def register_common_tools(mcp):
    """Registra as tools comuns no FastMCP."""

    @mcp.tool()
    async def list_connections() -> str:
        """
        Lista todas as conexões de banco de dados configuradas no servidor.
        Retorna connection_id, tipo (postgresql, mysql, etc.) e se é somente leitura.
        Use o connection_id retornado nas outras tools (execute_query_sql, list_tables, etc.).
        """
        try:
            use_case = get_connection_use_case()
            connections = use_case.list_connections()
            return ResultFormatter.format_connections(connections)
        except Exception as e:
            info = ErrorHandler.handle(e, "list_connections")
            return ErrorHandler.format_for_agent(info)

    @mcp.tool()
    async def test_connection(connection_id: str) -> str:
        """
        Testa se uma conexão está acessível.
        Args:
            connection_id: ID da conexão (retornado por list_connections).
        Returns:
            Mensagem indicando sucesso ou falha.
        """
        try:
            if not connection_id or not connection_id.strip():
                return "Erro: connection_id não pode ser vazio."
            use_case = get_connection_use_case()
            ok = await use_case.test_connection(connection_id.strip())
            if ok:
                return f"Conexão '{connection_id}' está acessível."
            return f"Conexão '{connection_id}' não encontrada ou inacessível. Verifique list_connections e as credenciais no .env."
        except Exception as e:
            info = ErrorHandler.handle(e, "test_connection")
            return ErrorHandler.format_for_agent(info)

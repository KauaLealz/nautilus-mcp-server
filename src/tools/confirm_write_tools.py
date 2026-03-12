"""
Tools MCP para write com confirmação (human-in-the-loop): request_pending_write, execute_confirmed_write.
"""
from src.config.settings import get_settings
from src.utils.error_handler import ErrorHandler
from src.utils.pending_writes import add as pending_add, get as pending_get, pop as pending_pop
from src.bootstrap import get_adapter


def register_confirm_write_tools(mcp):
    """Registra tools de write com confirmação (ALLOW_WRITE e NAUTILUS_CONFIRM_WRITE_TOKEN)."""

    @mcp.tool()
    async def request_pending_write(connection_id: str, command: str):
        """Registra um comando de escrita pendente de confirmação. Retorna um pending_id. Use execute_confirmed_write(pending_id, token) para executar com o token configurado (NAUTILUS_CONFIRM_WRITE_TOKEN)."""
        try:
            settings = get_settings()
            if not settings.allow_write:
                return "Write não está habilitado. Configure NAUTILUS_ALLOW_WRITE=true."
            if not command or not command.strip():
                return "Comando não pode ser vazio."
            pid = pending_add(connection_id.strip(), command.strip())
            return f"Write pendente registrado. ID: {pid}. Para executar, chame execute_confirmed_write(pending_id=\"{pid}\", token=\"<seu_token>\"). Token deve ser o valor de NAUTILUS_CONFIRM_WRITE_TOKEN."
        except Exception as e:
            return ErrorHandler.format_for_agent(ErrorHandler.handle(e, "request_pending_write"))

    @mcp.tool()
    async def execute_confirmed_write(pending_id: str, token: str):
        """Executa um write previamente registrado com request_pending_write, se o token coincidir com NAUTILUS_CONFIRM_WRITE_TOKEN."""
        try:
            settings = get_settings()
            if not settings.allow_write:
                return "Write não está habilitado."
            if not settings.confirm_write_token or token != settings.confirm_write_token:
                return "Token inválido ou NAUTILUS_CONFIRM_WRITE_TOKEN não configurado."
            pending = pending_get(pending_id.strip())
            if not pending:
                return f"Pendência '{pending_id}' não encontrada ou já executada."
            adapter = get_adapter(pending.connection_id)
            if not adapter:
                return f"Conexão '{pending.connection_id}' não encontrada."
            if not hasattr(adapter, "execute_sql_raw"):
                return f"Adapter para '{pending.connection_id}' não suporta execução de write (execute_sql_raw)."
            await adapter.execute_sql_raw(pending.connection_id, pending.command)
            pending_pop(pending_id.strip())
            return f"Write executado com sucesso (pending_id={pending_id})."
        except Exception as e:
            return ErrorHandler.format_for_agent(ErrorHandler.handle(e, "execute_confirmed_write"))

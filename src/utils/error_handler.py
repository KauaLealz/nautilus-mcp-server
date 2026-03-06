"""
Tratamento centralizado de erros para o MCP Server.
Equivalente ao ErrorService do ProjPro: formata erros para o agente sem expor detalhes internos.
"""
from typing import Any

from src.domain.query_safety import QuerySafetyError


class ErrorHandler:
    """Trata exceções e retorna mensagens seguras para o usuário/agente."""

    @staticmethod
    def handle(error: Exception, context: str = "") -> dict[str, Any]:
        """
        Analisa a exceção e retorna um dicionário com tipo, mensagem e detalhes (para log).
        """
        error_type = type(error).__name__
        message = str(error)

        if isinstance(error, QuerySafetyError):
            return {
                "type": "QUERY_SAFETY_ERROR",
                "message": message,
                "context": context,
                "user_message": message,
            }
        if isinstance(error, ConnectionError):
            return {
                "type": "CONNECTION_ERROR",
                "message": f"Falha de conexão: {message}",
                "context": context,
                "user_message": "Não foi possível conectar ao banco. Verifique se o connection_id está correto e se o serviço está acessível.",
            }
        if isinstance(error, TimeoutError):
            return {
                "type": "TIMEOUT_ERROR",
                "message": message,
                "context": context,
                "user_message": "A operação excedeu o tempo limite. Tente uma query mais simples ou aumente o timeout.",
            }
        if isinstance(error, ValueError):
            return {
                "type": "VALUE_ERROR",
                "message": message,
                "context": context,
                "user_message": f"Parâmetro inválido: {message}",
            }
        if isinstance(error, KeyError):
            return {
                "type": "KEY_ERROR",
                "message": message,
                "context": context,
                "user_message": f"Configuração ou recurso não encontrado: {message}",
            }
        # Erro genérico: não expor stack ao usuário
        return {
            "type": "ERROR",
            "message": message,
            "context": context,
            "user_message": "Ocorreu um erro ao processar a solicitação. Verifique os parâmetros e tente novamente.",
        }

    @staticmethod
    def format_for_agent(error_info: dict[str, Any], include_details: bool = False) -> str:
        """Formata a mensagem para retorno ao agente (sem stack trace)."""
        msg = error_info.get("user_message", error_info.get("message", "Erro desconhecido."))
        if include_details and error_info.get("context"):
            msg += f" (Contexto: {error_info['context']})"
        return msg

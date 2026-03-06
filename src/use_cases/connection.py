"""
Use case: listar conexões e testar conectividade.
"""
from collections.abc import Awaitable, Callable

from src.domain.models import ConnectionInfo


class ConnectionUseCase:
    """Lista conexões configuradas e testa se estão acessíveis."""

    def __init__(
        self,
        list_connections_fn: Callable[[], list[ConnectionInfo]],
        test_connection_fn: Callable[[str], Awaitable[bool]],
    ):
        """
        list_connections_fn: () -> list[ConnectionInfo]
        test_connection_fn: (connection_id: str) -> Awaitable[bool]
        """
        self._list_connections = list_connections_fn
        self._test_connection = test_connection_fn

    def list_connections(self) -> list[ConnectionInfo]:
        """Retorna todas as conexões configuradas (sem credenciais)."""
        return self._list_connections()

    async def test_connection(self, connection_id: str) -> bool:
        """Testa se a conexão está acessível."""
        return await self._test_connection(connection_id)

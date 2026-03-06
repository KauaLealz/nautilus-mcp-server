"""
Ports (interfaces) do domínio.
Contratos que os adapters devem implementar para cada tipo de banco.
"""
from abc import ABC, abstractmethod
from typing import Protocol

from src.domain.models import ConnectionInfo, QueryResult, TableInfo


class ConnectionProvider(Protocol):
    """Fornece teste de conectividade e lista de conexões configuradas."""

    def list_connections(self) -> list[ConnectionInfo]:
        """Retorna todas as conexões configuradas (sem credenciais)."""
        ...

    def get_connection_info(self, connection_id: str) -> ConnectionInfo | None:
        """Retorna informações de uma conexão por id, ou None se não existir."""
        ...

    async def test_connection(self, connection_id: str) -> bool:
        """Testa se a conexão está acessível. Retorna True se OK."""
        ...


class SqlQueryExecutor(Protocol):
    """Executa queries SQL de leitura (SELECT) com limites e timeout."""

    async def execute_read_only(
        self,
        connection_id: str,
        query: str,
        *,
        max_rows: int = 500,
        timeout_seconds: int = 30,
    ) -> QueryResult:
        """
        Executa uma query de leitura.
        Deve usar parâmetros preparados quando houver valores do usuário.
        """
        ...


class SchemaIntrospector(Protocol):
    """Introspectação de schema para bancos SQL."""

    async def list_tables(
        self,
        connection_id: str,
        schema: str | None = None,
    ) -> list[tuple[str, str]]:
        """
        Lista tabelas. Retorna lista de (schema_name, table_name).
        """
        ...

    async def describe_table(
        self,
        connection_id: str,
        table_name: str,
        schema: str | None = None,
    ) -> TableInfo | None:
        """Retorna metadados da tabela (colunas, tipos) ou None se não existir."""
        ...


# NoSQL: interfaces específicas (MongoDB, Redis) podem ser definidas em ports_nosql
# ou nos próprios use cases para não sobrecarregar o core SQL.

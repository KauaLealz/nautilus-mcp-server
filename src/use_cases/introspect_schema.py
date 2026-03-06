"""
Use case: introspectação de schema (listar tabelas, descrever tabela).
"""
from src.domain.models import TableInfo


class IntrospectSchemaUseCase:
    """Lista tabelas e descreve colunas para bancos SQL."""

    def __init__(self, get_sql_adapter_fn: callable):
        """
        get_sql_adapter_fn: (connection_id: str) -> objeto com list_tables e describe_table, ou None
        """
        self._get_adapter = get_sql_adapter_fn

    async def list_tables(
        self,
        connection_id: str,
        schema: str | None = None,
    ) -> list[tuple[str, str]]:
        """Lista (schema_name, table_name) para o connection_id."""
        adapter = self._get_adapter(connection_id)
        if adapter is None:
            raise KeyError(f"Conexão não encontrada ou não é SQL: {connection_id}")
        if not hasattr(adapter, "list_tables"):
            raise ValueError(f"Adapter para {connection_id} não suporta list_tables.")
        return await adapter.list_tables(connection_id, schema=schema)

    async def describe_table(
        self,
        connection_id: str,
        table_name: str,
        schema: str | None = None,
    ) -> TableInfo | None:
        """Retorna metadados da tabela ou None se não existir."""
        adapter = self._get_adapter(connection_id)
        if adapter is None:
            raise KeyError(f"Conexão não encontrada ou não é SQL: {connection_id}")
        if not hasattr(adapter, "describe_table"):
            raise ValueError(f"Adapter para {connection_id} não suporta describe_table.")
        return await adapter.describe_table(connection_id, table_name, schema=schema)

"""
Use case: executar query SQL de leitura com validação de segurança.
"""
from src.domain.models import QueryResult
from src.domain.query_safety import SqlQueryValidator, QuerySafetyError


class ExecuteQueryUseCase:
    """Valida e executa queries SQL de leitura através do adapter correspondente."""

    def __init__(
        self,
        get_sql_adapter_fn: callable,
        validator: SqlQueryValidator,
        max_rows: int = 500,
        timeout_seconds: int = 30,
    ):
        """
        get_sql_adapter_fn: (connection_id: str) -> objeto com execute_read_only(connection_id, query, max_rows, timeout_seconds)
        validator: validador de segurança da query
        """
        self._get_adapter = get_sql_adapter_fn
        self._validator = validator
        self._max_rows = max_rows
        self._timeout = timeout_seconds

    async def execute(
        self,
        connection_id: str,
        query: str,
        max_rows: int | None = None,
    ) -> QueryResult:
        """
        Valida a query e executa no banco. max_rows é o desejado pelo agente,
        limitado pelo cap configurado (nunca retorna mais que self._max_rows).
        """
        self._validator.sanitize_or_raise(query)
        effective_rows = min(max_rows if max_rows is not None else self._max_rows, self._max_rows)
        effective_rows = max(1, effective_rows)
        adapter = self._get_adapter(connection_id)
        if adapter is None:
            raise KeyError(f"Conexão não encontrada ou não é SQL: {connection_id}")
        return await adapter.execute_read_only(
            connection_id,
            query,
            max_rows=effective_rows,
            timeout_seconds=self._timeout,
        )

"""
Tools MCP para bancos SQL: listar tabelas, descrever tabela, executar query.
"""
from src.bootstrap import get_execute_query_use_case, get_introspect_schema_use_case
from src.utils.error_handler import ErrorHandler
from src.utils.formatter import ResultFormatter
from src.utils.query_history import append as history_append
from src.domain.query_safety import QuerySafetyError


def register_sql_tools(mcp):
    """Registra as tools SQL no FastMCP."""

    @mcp.tool()
    async def list_tables(
        connection_id: str,
        schema: str | None = None,
    ):
        """
        Lista as tabelas de um banco SQL (PostgreSQL, MySQL, SQL Server).
        Use esta tool antes de montar queries para conhecer o schema real.
        Args:
            connection_id: ID da conexão (de list_connections).
            schema: Nome do schema (opcional). Em PostgreSQL é 'public' por padrão.
        Returns:
            Lista de tabelas no formato schema.tabela.
        """
        try:
            if not connection_id or not connection_id.strip():
                return "Erro: connection_id não pode ser vazio."
            use_case = get_introspect_schema_use_case()
            tables = await use_case.list_tables(connection_id.strip(), schema=schema)
            return ResultFormatter.format_tables(tables)
        except Exception as e:
            info = ErrorHandler.handle(e, "list_tables")
            return ErrorHandler.format_for_agent(info)

    @mcp.tool()
    async def describe_table(
        connection_id: str,
        table_name: str,
        schema: str | None = None,
    ):
        """
        Descreve as colunas de uma tabela SQL (nome, tipo, nullable).
        Use esta tool para montar queries corretas com os nomes reais das colunas.
        Args:
            connection_id: ID da conexão (de list_connections).
            table_name: Nome da tabela.
            schema: Nome do schema (opcional).
        Returns:
            Metadados da tabela (colunas e tipos).
        """
        try:
            if not connection_id or not connection_id.strip():
                return "Erro: connection_id não pode ser vazio."
            if not table_name or not table_name.strip():
                return "Erro: table_name não pode ser vazio."
            use_case = get_introspect_schema_use_case()
            info = await use_case.describe_table(
                connection_id.strip(),
                table_name.strip(),
                schema=schema.strip() if schema and schema.strip() else None,
            )
            if info is None:
                return f"Tabela '{table_name}' não encontrada no banco."
            return ResultFormatter.format_table_info(info)
        except Exception as e:
            info = ErrorHandler.handle(e, "describe_table")
            return ErrorHandler.format_for_agent(info)

    @mcp.tool()
    async def execute_query_sql(
        connection_id: str,
        query: str,
        max_rows: int | None = None,
    ):
        """
        Executa uma query SQL de leitura (SELECT ou WITH ... SELECT).
        Apenas leitura é permitida; queries com INSERT/UPDATE/DELETE/DDL são rejeitadas.
        Use list_tables e describe_table antes para montar queries corretas.
        Args:
            connection_id: ID da conexão (de list_connections).
            query: Query SQL (apenas SELECT). LIMIT/TOP/FETCH na query não podem exceder o cap configurado.
            max_rows: Quantas linhas deseja receber (opcional). Limitado pelo máximo do servidor (ex.: 500). Se não informar, usa o padrão.
        Returns:
            Resultado em formato de tabela (markdown-style).
        """
        try:
            if not connection_id or not connection_id.strip():
                return "Erro: connection_id não pode ser vazio."
            if not query or not query.strip():
                return "Erro: query não pode ser vazia."
            use_case = get_execute_query_use_case()
            result = await use_case.execute(
                connection_id.strip(),
                query.strip(),
                max_rows=max_rows,
            )
            history_append(connection_id.strip(), query.strip(), result.row_count)
            return ResultFormatter.format_query_result(result)
        except QuerySafetyError as e:
            return str(e)
        except Exception as e:
            info = ErrorHandler.handle(e, "execute_query_sql")
            return ErrorHandler.format_for_agent(info)

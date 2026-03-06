"""
Modelos de domínio do Nautilus.
DTOs e estruturas compartilhadas entre adapters e use cases.
"""
from typing import Any

from pydantic import BaseModel, ConfigDict


class ConnectionInfo(BaseModel):
    """Informação pública de uma conexão (sem credenciais)."""

    connection_id: str
    type: str  # postgresql | mysql | sqlserver | mongodb | redis
    read_only: bool = True


class ColumnInfo(BaseModel):
    """Metadado de uma coluna (introspectação de schema)."""

    name: str
    data_type: str
    nullable: bool = True


class TableInfo(BaseModel):
    """Metadado de uma tabela (introspectação de schema)."""

    schema_name: str
    table_name: str
    columns: list[ColumnInfo] = []

    model_config = ConfigDict(extra="ignore")


class QueryResult(BaseModel):
    """Resultado de uma query SQL de leitura."""

    columns: list[str]
    rows: list[list[Any]]
    row_count: int

    model_config = ConfigDict(extra="ignore")


class IndexInfo(BaseModel):
    """Metadado de um índice."""

    index_name: str
    columns: list[str]
    is_unique: bool = False


class ViewInfo(BaseModel):
    """Metadado de uma view."""

    schema_name: str
    view_name: str
    definition: str | None = None


class ForeignKeyInfo(BaseModel):
    """Informação de chave estrangeira."""

    constraint_name: str
    from_schema: str
    from_table: str
    from_columns: list[str]
    to_schema: str
    to_table: str
    to_columns: list[str]


class TableRelationship(BaseModel):
    """Relacionamento entre tabelas (A -> B por FK)."""

    from_table: str
    to_table: str
    constraint_name: str


class SchemaTableSummary(BaseModel):
    """Resumo de uma tabela no schema (nome, colunas, opcional row_count)."""

    schema_name: str
    table_name: str
    column_count: int
    row_count: int | None = None


class ColumnStat(BaseModel):
    """Estatística de coluna (min, max, avg, count, nulls, distinct)."""

    column_name: str
    stat_type: str  # min, max, avg, count, null_count, distinct_count, sample
    value: Any


class ConnectionCapability(BaseModel):
    """Capacidade suportada por uma conexão."""

    name: str
    supported: bool = True


class QueryHistoryEntry(BaseModel):
    """Entrada do histórico de queries (somente leitura)."""

    connection_id: str
    query: str
    timestamp: float
    row_count: int | None = None


class PendingWrite(BaseModel):
    """Write pendente de confirmação (human-in-the-loop)."""

    id: str
    connection_id: str
    command: str
    created_at: float


class SavedQuery(BaseModel):
    """Query salva (nome + SQL com placeholders)."""

    name: str
    description: str = ""
    query_template: str
    parameters: list[str] = []  # nomes dos placeholders

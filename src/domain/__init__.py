"""Domínio: ports, modelos e validação de segurança."""

from src.domain.models import (
    ConnectionInfo,
    QueryResult,
    TableInfo,
    ColumnInfo,
    IndexInfo,
    ViewInfo,
    ForeignKeyInfo,
    TableRelationship,
    SchemaTableSummary,
    ColumnStat,
    QueryHistoryEntry,
    PendingWrite,
    SavedQuery,
)
from src.domain.ports import (
    ConnectionProvider,
    SqlQueryExecutor,
    SchemaIntrospector,
)
from src.domain.query_safety import SqlQueryValidator, QuerySafetyError

__all__ = [
    "ConnectionInfo",
    "QueryResult",
    "TableInfo",
    "ColumnInfo",
    "IndexInfo",
    "ViewInfo",
    "ForeignKeyInfo",
    "TableRelationship",
    "SchemaTableSummary",
    "ColumnStat",
    "QueryHistoryEntry",
    "PendingWrite",
    "SavedQuery",
    "ConnectionProvider",
    "SqlQueryExecutor",
    "SchemaIntrospector",
    "SqlQueryValidator",
    "QuerySafetyError",
]

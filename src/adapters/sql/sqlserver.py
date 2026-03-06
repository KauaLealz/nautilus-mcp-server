"""
Adapter para SQL Server (pyodbc).
Execução bloqueante em thread para não bloquear o event loop.
"""
import asyncio
from typing import Any

import pyodbc

from src.config.settings import DatabaseConfig
from src.domain.models import (
    ColumnInfo,
    ColumnStat,
    ConnectionInfo,
    ForeignKeyInfo,
    IndexInfo,
    QueryResult,
    SchemaTableSummary,
    TableInfo,
    TableRelationship,
    ViewInfo,
)
from src.adapters.base import connection_info_from_config


def _connection_string(url: str) -> str:
    """Remove prefixo odbc:// se presente; pyodbc usa a string direto."""
    if url.startswith("odbc://"):
        return url[7:]
    return url


def _sync_test_connection(conn_str: str) -> bool:
    try:
        conn = pyodbc.connect(conn_str)
        conn.close()
        return True
    except Exception:
        return False


def _sync_execute_read_only(
    conn_str: str, query: str, max_rows: int, timeout_seconds: int
) -> QueryResult:
    conn = pyodbc.connect(conn_str, timeout=timeout_seconds)
    try:
        conn.timeout = timeout_seconds
        cursor = conn.cursor()
        cursor.execute(query)
        columns = [d[0] for d in cursor.description] if cursor.description else []
        rows = cursor.fetchmany(max_rows)
        row_list = [list(r) for r in rows]
        cursor.close()
        return QueryResult(columns=columns, rows=row_list, row_count=len(row_list))
    finally:
        conn.close()


def _sync_list_tables(conn_str: str, schema: str | None) -> list[tuple[str, str]]:
    conn = pyodbc.connect(conn_str)
    try:
        cursor = conn.cursor()
        if schema:
            cursor.execute(
                """
                SELECT TABLE_SCHEMA, TABLE_NAME FROM INFORMATION_SCHEMA.TABLES
                WHERE TABLE_SCHEMA = ?
                ORDER BY TABLE_SCHEMA, TABLE_NAME
                """,
                (schema,),
            )
        else:
            cursor.execute(
                """
                SELECT TABLE_SCHEMA, TABLE_NAME FROM INFORMATION_SCHEMA.TABLES
                WHERE TABLE_TYPE = 'BASE TABLE'
                ORDER BY TABLE_SCHEMA, TABLE_NAME
                """
            )
        rows = cursor.fetchall()
        return [(r[0], r[1]) for r in rows]
    finally:
        conn.close()


def _sync_describe_table(
    conn_str: str, table_name: str, schema: str | None
) -> TableInfo | None:
    conn = pyodbc.connect(conn_str)
    try:
        cursor = conn.cursor()
        if schema:
            cursor.execute(
                """
                SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, DATA_TYPE, IS_NULLABLE
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_NAME = ? AND TABLE_SCHEMA = ?
                ORDER BY ORDINAL_POSITION
                """,
                (table_name, schema),
            )
        else:
            cursor.execute(
                """
                SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, DATA_TYPE, IS_NULLABLE
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_NAME = ?
                ORDER BY ORDINAL_POSITION
                """,
                (table_name,),
            )
        rows = cursor.fetchall()
        if not rows:
            return None
        schema_name, tbl, _ = rows[0][0], rows[0][1], rows[0][2]
        columns = [
            ColumnInfo(
                name=r[2],
                data_type=r[3],
                nullable=r[4] == "YES",
            )
            for r in rows
        ]
        return TableInfo(schema_name=schema_name, table_name=tbl, columns=columns)
    finally:
        conn.close()


def _sync_list_databases(conn_str: str) -> list[str]:
    conn = pyodbc.connect(conn_str)
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sys.databases WHERE name NOT IN ('master','tempdb','msdb') ORDER BY name")
        return [r[0] for r in cursor.fetchall()]
    finally:
        conn.close()


def _sync_get_table_sample(conn_str: str, schema: str, table_name: str, limit: int) -> QueryResult:
    conn = pyodbc.connect(conn_str)
    try:
        cursor = conn.cursor()
        cursor.execute(f"SELECT TOP ({min(limit, 100)} ) * FROM [{schema}].[{table_name}]")
        columns = [d[0] for d in cursor.description] if cursor.description else []
        rows = [list(r) for r in cursor.fetchall()]
        return QueryResult(columns=columns, rows=rows, row_count=len(rows))
    finally:
        conn.close()


def _sync_get_schema_summary(conn_str: str, schema: str | None, include_row_count: bool) -> list[SchemaTableSummary]:
    conn = pyodbc.connect(conn_str)
    try:
        cursor = conn.cursor()
        if schema:
            cursor.execute(
                "SELECT t.TABLE_SCHEMA, t.TABLE_NAME, (SELECT count(*) FROM INFORMATION_SCHEMA.COLUMNS c WHERE c.TABLE_SCHEMA=t.TABLE_SCHEMA AND c.TABLE_NAME=t.TABLE_NAME) AS col_count FROM INFORMATION_SCHEMA.TABLES t WHERE t.TABLE_SCHEMA = ? AND t.TABLE_TYPE = 'BASE TABLE' ORDER BY t.TABLE_SCHEMA, t.TABLE_NAME",
                (schema,),
            )
        else:
            cursor.execute(
                "SELECT t.TABLE_SCHEMA, t.TABLE_NAME, (SELECT count(*) FROM INFORMATION_SCHEMA.COLUMNS c WHERE c.TABLE_SCHEMA=t.TABLE_SCHEMA AND c.TABLE_NAME=t.TABLE_NAME) AS col_count FROM INFORMATION_SCHEMA.TABLES t WHERE t.TABLE_TYPE = 'BASE TABLE' ORDER BY t.TABLE_SCHEMA, t.TABLE_NAME"
            )
        rows = cursor.fetchall()
        result = []
        for r in rows:
            row_count = None
            if include_row_count:
                try:
                    c2 = conn.cursor()
                    c2.execute(f"SELECT count(*) FROM [{r[0]}].[{r[1]}]")
                    row_count = c2.fetchone()[0]
                    c2.close()
                except Exception:
                    pass
            result.append(SchemaTableSummary(schema_name=r[0], table_name=r[1], column_count=r[2], row_count=row_count))
        return result
    finally:
        conn.close()


def _sync_explain_query(conn_str: str, query: str) -> str:
    conn = pyodbc.connect(conn_str)
    try:
        cursor = conn.cursor()
        try:
            cursor.execute("SET SHOWPLAN_TEXT ON")
            cursor.execute(query.strip().rstrip(";"))
            rows = cursor.fetchall()
            return "\n".join(str(r[0]) for r in rows) if rows else ""
        except Exception as e:
            return str(e)
        finally:
            try:
                cursor.execute("SET SHOWPLAN_TEXT OFF")
            except Exception:
                pass
    finally:
        conn.close()


def _sync_list_indexes(conn_str: str, table_name: str, schema: str) -> list[IndexInfo]:
    conn = pyodbc.connect(conn_str)
    try:
        cursor = conn.cursor()
        full_name = f"[{schema}].[{table_name}]"
        cursor.execute(
            "SELECT i.name, c.name, i.is_unique FROM sys.indexes i JOIN sys.index_columns ic ON i.object_id=ic.object_id AND i.index_id=ic.index_id JOIN sys.columns c ON ic.object_id=c.object_id AND ic.column_id=c.column_id WHERE i.object_id = OBJECT_ID(?) AND i.name IS NOT NULL ORDER BY i.name, ic.key_ordinal",
            (full_name,),
        )
        rows = cursor.fetchall()
        by_name: dict[str, IndexInfo] = {}
        for r in rows:
            iname, col, uniq = r[0], r[1], bool(r[2])
            if iname not in by_name:
                by_name[iname] = IndexInfo(index_name=iname, columns=[], is_unique=uniq)
            by_name[iname].columns.append(col)
        return list(by_name.values())
    finally:
        conn.close()


def _sync_list_views(conn_str: str, schema: str | None) -> list[ViewInfo]:
    conn = pyodbc.connect(conn_str)
    try:
        cursor = conn.cursor()
        if schema:
            cursor.execute("SELECT TABLE_SCHEMA, TABLE_NAME, VIEW_DEFINITION FROM INFORMATION_SCHEMA.VIEWS WHERE TABLE_SCHEMA = ? ORDER BY TABLE_SCHEMA, TABLE_NAME", (schema,))
        else:
            cursor.execute("SELECT TABLE_SCHEMA, TABLE_NAME, VIEW_DEFINITION FROM INFORMATION_SCHEMA.VIEWS ORDER BY TABLE_SCHEMA, TABLE_NAME")
        return [ViewInfo(schema_name=r[0], view_name=r[1], definition=r[2]) for r in cursor.fetchall()]
    finally:
        conn.close()


def _sync_get_foreign_keys(conn_str: str, table_name: str, schema: str) -> list[ForeignKeyInfo]:
    conn = pyodbc.connect(conn_str)
    try:
        cursor = conn.cursor()
        cursor.execute(
            """SELECT fk.object_id, fk.name, OBJECT_SCHEMA_NAME(fk.parent_object_id), OBJECT_NAME(fk.parent_object_id),
               OBJECT_SCHEMA_NAME(fk.referenced_object_id), OBJECT_NAME(fk.referenced_object_id)
               FROM sys.foreign_keys fk WHERE OBJECT_NAME(fk.parent_object_id) = ? AND OBJECT_SCHEMA_NAME(fk.parent_object_id) = ?""",
            (table_name, schema),
        )
        rows = cursor.fetchall()
        result = []
        for r in rows:
            fk_oid, cname, from_sch, from_tbl, to_sch, to_tbl = r[0], r[1], r[2], r[3], r[4], r[5]
            cursor2 = conn.cursor()
            cursor2.execute(
                "SELECT COL_NAME(fkc.parent_object_id, fkc.parent_column_id), COL_NAME(fkc.referenced_object_id, fkc.referenced_column_id) FROM sys.foreign_key_columns fkc WHERE fkc.constraint_object_id = ?",
                (fk_oid,),
            )
            pairs = cursor2.fetchall()
            from_cols = [p[0] for p in pairs]
            to_cols = [p[1] for p in pairs]
            result.append(ForeignKeyInfo(constraint_name=cname, from_schema=from_sch, from_table=from_tbl, from_columns=from_cols, to_schema=to_sch, to_table=to_tbl, to_columns=to_cols))
        return result
    except Exception:
        return []
    finally:
        conn.close()


def _sync_get_table_relationships(conn_str: str, schema: str) -> list[TableRelationship]:
    conn = pyodbc.connect(conn_str)
    try:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT fk.name, OBJECT_NAME(fk.parent_object_id), OBJECT_NAME(fk.referenced_object_id) FROM sys.foreign_keys fk WHERE OBJECT_SCHEMA_NAME(fk.parent_object_id) = ?",
            (schema,),
        )
        return [TableRelationship(from_table=r[1], to_table=r[2], constraint_name=r[0]) for r in cursor.fetchall()]
    finally:
        conn.close()


def _sync_get_row_count(conn_str: str, schema: str, table_name: str, where_clause: str | None) -> int:
    conn = pyodbc.connect(conn_str)
    try:
        cursor = conn.cursor()
        where = f" WHERE {where_clause}" if where_clause else ""
        cursor.execute(f"SELECT count(*) FROM [{schema}].[{table_name}]{where}")
        return cursor.fetchone()[0] or 0
    finally:
        conn.close()


def _sync_get_column_stats(conn_str: str, schema: str, table_name: str, info: TableInfo, column_names: list[str] | None) -> list[ColumnStat]:
    result = []
    numeric_types = ("int", "bigint", "smallint", "decimal", "numeric", "float", "real")
    cols = [c for c in info.columns if column_names is None or c.name in column_names][:20]
    conn = pyodbc.connect(conn_str)
    try:
        for col in cols:
            try:
                cursor = conn.cursor()
                if col.data_type in numeric_types:
                    cursor.execute(f"SELECT count(*), count([{col.name}]), min([{col.name}]), max([{col.name}]), avg(cast([{col.name}] as float)) FROM [{schema}].[{table_name}]")
                    row = cursor.fetchone()
                    if row and row[0]:
                        result.append(ColumnStat(column_name=col.name, stat_type="count", value=row[0]))
                        result.append(ColumnStat(column_name=col.name, stat_type="null_count", value=row[0] - (row[1] or 0)))
                        if row[2] is not None:
                            result.append(ColumnStat(column_name=col.name, stat_type="min", value=row[2]))
                            result.append(ColumnStat(column_name=col.name, stat_type="max", value=row[3]))
                            result.append(ColumnStat(column_name=col.name, stat_type="avg", value=row[4]))
                else:
                    cursor.execute(f"SELECT count(*), count(distinct [{col.name}]) FROM [{schema}].[{table_name}]")
                    row = cursor.fetchone()
                    if row:
                        result.append(ColumnStat(column_name=col.name, stat_type="count", value=row[0]))
                        result.append(ColumnStat(column_name=col.name, stat_type="distinct_count", value=row[1]))
            except Exception:
                pass
        return result
    finally:
        conn.close()


def _sync_suggest_tables(conn_str: str, search_term: str, schema: str | None) -> list[tuple[str, str, str]]:
    conn = pyodbc.connect(conn_str)
    try:
        term = f"%{search_term}%"
        cursor = conn.cursor()
        if schema:
            cursor.execute(
                "SELECT DISTINCT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE (TABLE_NAME LIKE ? OR COLUMN_NAME LIKE ?) AND TABLE_SCHEMA = ? ORDER BY TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME",
                (term, term, schema),
            )
        else:
            cursor.execute(
                "SELECT DISTINCT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME LIKE ? OR COLUMN_NAME LIKE ? ORDER BY TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME",
                (term, term),
            )
        return [tuple(r) for r in cursor.fetchall()]
    finally:
        conn.close()


class SqlServerAdapter:
    """Adapter para SQL Server via pyodbc (operações em thread)."""

    def __init__(self, connections: dict[str, DatabaseConfig]):
        self._connections = {
            k: v for k, v in connections.items() if v.type == "sqlserver"
        }
        self._conn_strings = {cid: _connection_string(c.url) for cid, c in self._connections.items()}

    def list_connections(self) -> list[ConnectionInfo]:
        return [connection_info_from_config(cid, c) for cid, c in self._connections.items()]

    def get_connection_info(self, connection_id: str) -> ConnectionInfo | None:
        config = self._connections.get(connection_id)
        if not config:
            return None
        return connection_info_from_config(connection_id, config)

    async def test_connection(self, connection_id: str) -> bool:
        conn_str = self._conn_strings.get(connection_id)
        if not conn_str:
            return False
        return await asyncio.to_thread(_sync_test_connection, conn_str)

    async def execute_read_only(
        self,
        connection_id: str,
        query: str,
        *,
        max_rows: int = 500,
        timeout_seconds: int = 30,
    ) -> QueryResult:
        conn_str = self._conn_strings.get(connection_id)
        if not conn_str:
            raise KeyError(f"Conexão não encontrada: {connection_id}")
        return await asyncio.to_thread(
            _sync_execute_read_only,
            conn_str,
            query,
            max_rows,
            timeout_seconds,
        )

    async def list_tables(
        self,
        connection_id: str,
        schema: str | None = None,
    ) -> list[tuple[str, str]]:
        conn_str = self._conn_strings.get(connection_id)
        if not conn_str:
            raise KeyError(f"Conexão não encontrada: {connection_id}")
        return await asyncio.to_thread(_sync_list_tables, conn_str, schema)

    async def describe_table(
        self,
        connection_id: str,
        table_name: str,
        schema: str | None = None,
    ) -> TableInfo | None:
        conn_str = self._conn_strings.get(connection_id)
        if not conn_str:
            raise KeyError(f"Conexão não encontrada: {connection_id}")
        return await asyncio.to_thread(
            _sync_describe_table,
            conn_str,
            table_name,
            schema,
        )

    async def list_databases(self, connection_id: str) -> list[str]:
        conn_str = self._conn_strings.get(connection_id)
        if not conn_str:
            raise KeyError(f"Conexão não encontrada: {connection_id}")
        return await asyncio.to_thread(_sync_list_databases, conn_str)

    async def get_table_sample(
        self,
        connection_id: str,
        table_name: str,
        schema: str | None = None,
        limit: int = 5,
    ) -> QueryResult:
        conn_str = self._conn_strings.get(connection_id)
        if not conn_str:
            raise KeyError(f"Conexão não encontrada: {connection_id}")
        schema = schema or "dbo"
        return await asyncio.to_thread(_sync_get_table_sample, conn_str, schema, table_name, limit)

    async def get_schema_summary(
        self,
        connection_id: str,
        schema: str | None = None,
        include_row_count: bool = False,
    ) -> list[SchemaTableSummary]:
        conn_str = self._conn_strings.get(connection_id)
        if not conn_str:
            raise KeyError(f"Conexão não encontrada: {connection_id}")
        return await asyncio.to_thread(_sync_get_schema_summary, conn_str, schema, include_row_count)

    async def export_schema_json(
        self,
        connection_id: str,
        schema: str | None = None,
    ) -> dict[str, Any]:
        tables = await self.list_tables(connection_id, schema=schema)
        tables_list = []
        for sch, tbl in tables:
            info = await self.describe_table(connection_id, tbl, schema=sch)
            if info:
                tables_list.append({
                    "schema": info.schema_name,
                    "table": info.table_name,
                    "columns": [{"name": c.name, "data_type": c.data_type, "nullable": c.nullable} for c in info.columns],
                })
        return {"tables": tables_list}

    async def explain_query_sql(self, connection_id: str, query: str) -> str:
        conn_str = self._conn_strings.get(connection_id)
        if not conn_str:
            raise KeyError(f"Conexão não encontrada: {connection_id}")
        return await asyncio.to_thread(_sync_explain_query, conn_str, query)

    async def validate_query_sql(self, connection_id: str, query: str) -> bool:
        try:
            await self.explain_query_sql(connection_id, query)
            return True
        except Exception:
            return False

    async def list_indexes(
        self,
        connection_id: str,
        table_name: str,
        schema: str | None = None,
    ) -> list[IndexInfo]:
        conn_str = self._conn_strings.get(connection_id)
        if not conn_str:
            raise KeyError(f"Conexão não encontrada: {connection_id}")
        schema = schema or "dbo"
        return await asyncio.to_thread(_sync_list_indexes, conn_str, table_name, schema)

    async def list_views(
        self,
        connection_id: str,
        schema: str | None = None,
    ) -> list[ViewInfo]:
        conn_str = self._conn_strings.get(connection_id)
        if not conn_str:
            raise KeyError(f"Conexão não encontrada: {connection_id}")
        return await asyncio.to_thread(_sync_list_views, conn_str, schema)

    async def get_foreign_keys(
        self,
        connection_id: str,
        table_name: str,
        schema: str | None = None,
    ) -> list[ForeignKeyInfo]:
        conn_str = self._conn_strings.get(connection_id)
        if not conn_str:
            raise KeyError(f"Conexão não encontrada: {connection_id}")
        schema = schema or "dbo"
        return await asyncio.to_thread(_sync_get_foreign_keys, conn_str, table_name, schema)

    async def get_table_relationships(
        self,
        connection_id: str,
        schema: str | None = None,
    ) -> list[TableRelationship]:
        conn_str = self._conn_strings.get(connection_id)
        if not conn_str:
            raise KeyError(f"Conexão não encontrada: {connection_id}")
        schema = schema or "dbo"
        return await asyncio.to_thread(_sync_get_table_relationships, conn_str, schema)

    async def get_row_count(
        self,
        connection_id: str,
        table_name: str,
        schema: str | None = None,
        where_clause: str | None = None,
    ) -> int:
        conn_str = self._conn_strings.get(connection_id)
        if not conn_str:
            raise KeyError(f"Conexão não encontrada: {connection_id}")
        schema = schema or "dbo"
        return await asyncio.to_thread(_sync_get_row_count, conn_str, schema, table_name, where_clause)

    async def get_column_stats(
        self,
        connection_id: str,
        table_name: str,
        schema: str | None = None,
        column_names: list[str] | None = None,
    ) -> list[ColumnStat]:
        conn_str = self._conn_strings.get(connection_id)
        if not conn_str:
            raise KeyError(f"Conexão não encontrada: {connection_id}")
        info = await self.describe_table(connection_id, table_name, schema=schema)
        if not info:
            return []
        schema_name = schema or "dbo"
        return await asyncio.to_thread(_sync_get_column_stats, conn_str, schema_name, table_name, info, column_names)

    async def suggest_tables(
        self,
        connection_id: str,
        search_term: str,
        schema: str | None = None,
    ) -> list[tuple[str, str, str]]:
        conn_str = self._conn_strings.get(connection_id)
        if not conn_str:
            raise KeyError(f"Conexão não encontrada: {connection_id}")
        return await asyncio.to_thread(_sync_suggest_tables, conn_str, search_term, schema)

    async def execute_sql_raw(self, connection_id: str, query: str) -> None:
        conn_str = self._conn_strings.get(connection_id)
        if not conn_str:
            raise KeyError(f"Conexão não encontrada: {connection_id}")

        def _run():
            conn = pyodbc.connect(conn_str)
            try:
                cursor = conn.cursor()
                cursor.execute(query.strip().rstrip(";"))
                conn.commit()
            finally:
                conn.close()
        await asyncio.to_thread(_run)

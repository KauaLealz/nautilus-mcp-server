"""
Adapter para PostgreSQL (asyncpg).
Implementa ConnectionProvider, SqlQueryExecutor e SchemaIntrospector.
"""
from typing import Any

import asyncpg

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


class PostgresAdapter:
    """Adapter para PostgreSQL: conexão, execução de leitura e introspectação de schema."""

    def __init__(self, connections: dict[str, DatabaseConfig]):
        """
        connections: dicionário connection_id -> DatabaseConfig apenas para tipo postgresql.
        """
        self._connections = {k: v for k, v in connections.items() if v.type == "postgresql"}

    def list_connections(self) -> list[ConnectionInfo]:
        return [connection_info_from_config(cid, c) for cid, c in self._connections.items()]

    def get_connection_info(self, connection_id: str) -> ConnectionInfo | None:
        config = self._connections.get(connection_id)
        if not config:
            return None
        return connection_info_from_config(connection_id, config)

    async def test_connection(self, connection_id: str) -> bool:
        config = self._connections.get(connection_id)
        if not config:
            return False
        try:
            conn = await asyncpg.connect(config.url)
            try:
                await conn.fetchval("SELECT 1")
                return True
            finally:
                await conn.close()
        except Exception:
            return False

    async def execute_read_only(
        self,
        connection_id: str,
        query: str,
        *,
        max_rows: int = 500,
        timeout_seconds: int = 30,
    ) -> QueryResult:
        config = self._connections.get(connection_id)
        if not config:
            raise KeyError(f"Conexão não encontrada: {connection_id}")
        conn = await asyncpg.connect(config.url, command_timeout=timeout_seconds)
        try:
            q = query.strip().rstrip(";")
            if " LIMIT " not in q.upper().split("--")[0]:
                q = f"{q} LIMIT {max_rows}"
            rows = await conn.fetch(q)
            if not rows:
                return QueryResult(columns=[], rows=[], row_count=0)
            columns = list(rows[0].keys())
            row_list = [[row[c] for c in columns] for row in rows]
            return QueryResult(columns=columns, rows=row_list, row_count=len(row_list))
        finally:
            await conn.close()

    async def list_tables(
        self,
        connection_id: str,
        schema: str | None = None,
    ) -> list[tuple[str, str]]:
        config = self._connections.get(connection_id)
        if not config:
            raise KeyError(f"Conexão não encontrada: {connection_id}")
        conn = await asyncpg.connect(config.url)
        try:
            schema_filter = "AND table_schema = $1" if schema else ""
            params = (schema,) if schema else ()
            q = f"""
                SELECT table_schema, table_name
                FROM information_schema.tables
                WHERE table_schema NOT IN ('pg_catalog', 'information_schema') {schema_filter}
                ORDER BY table_schema, table_name
            """
            rows = await conn.fetch(q, *params)
            return [(r["table_schema"], r["table_name"]) for r in rows]
        finally:
            await conn.close()

    async def describe_table(
        self,
        connection_id: str,
        table_name: str,
        schema: str | None = None,
    ) -> TableInfo | None:
        config = self._connections.get(connection_id)
        if not config:
            raise KeyError(f"Conexão não encontrada: {connection_id}")
        conn = await asyncpg.connect(config.url)
        try:
            schema_condition = "AND table_schema = $2" if schema else ""
            params: tuple[Any, ...] = (table_name,) if not schema else (table_name, schema)
            q = f"""
                SELECT table_schema, table_name, column_name, data_type, is_nullable
                FROM information_schema.columns
                WHERE table_name = $1 {schema_condition}
                ORDER BY ordinal_position
            """
            rows = await conn.fetch(q, *params)
            if not rows:
                return None
            schema_name = rows[0]["table_schema"]
            table = rows[0]["table_name"]
            columns = [
                ColumnInfo(
                    name=r["column_name"],
                    data_type=r["data_type"],
                    nullable=r["is_nullable"] == "YES",
                )
                for r in rows
            ]
            return TableInfo(schema_name=schema_name, table_name=table, columns=columns)
        finally:
            await conn.close()

    async def list_databases(self, connection_id: str) -> list[str]:
        config = self._connections.get(connection_id)
        if not config:
            raise KeyError(f"Conexão não encontrada: {connection_id}")
        conn = await asyncpg.connect(config.url)
        try:
            rows = await conn.fetch(
                "SELECT datname FROM pg_database WHERE NOT datistemplate ORDER BY datname"
            )
            return [r["datname"] for r in rows]
        finally:
            await conn.close()

    async def get_table_sample(
        self,
        connection_id: str,
        table_name: str,
        schema: str | None = None,
        limit: int = 5,
    ) -> QueryResult:
        config = self._connections.get(connection_id)
        if not config:
            raise KeyError(f"Conexão não encontrada: {connection_id}")
        schema = schema or "public"
        conn = await asyncpg.connect(config.url)
        try:
            # Identifiers seguros
            q = f'SELECT * FROM "{schema}"."{table_name}" LIMIT {min(limit, 100)}'
            rows = await conn.fetch(q)
            if not rows:
                return QueryResult(columns=[], rows=[], row_count=0)
            columns = list(rows[0].keys())
            row_list = [[row[c] for c in columns] for row in rows]
            return QueryResult(columns=columns, rows=row_list, row_count=len(row_list))
        finally:
            await conn.close()

    async def get_schema_summary(
        self,
        connection_id: str,
        schema: str | None = None,
        include_row_count: bool = False,
    ) -> list[SchemaTableSummary]:
        config = self._connections.get(connection_id)
        if not config:
            raise KeyError(f"Conexão não encontrada: {connection_id}")
        conn = await asyncpg.connect(config.url)
        try:
            schema_filter = "AND table_schema = $1" if schema else "AND table_schema NOT IN ('pg_catalog', 'information_schema')"
            params = (schema,) if schema else ()
            q = f"""
                SELECT table_schema, table_name,
                       (SELECT count(*) FROM information_schema.columns c
                        WHERE c.table_schema = t.table_schema AND c.table_name = t.table_name) AS col_count
                FROM information_schema.tables t
                WHERE table_type = 'BASE TABLE' {schema_filter}
                ORDER BY table_schema, table_name
            """
            rows = await conn.fetch(q, *params)
            result = []
            for r in rows:
                row_count = None
                if include_row_count:
                    try:
                        cnt = await conn.fetchval(
                            f'SELECT count(*) FROM "{r["table_schema"]}"."{r["table_name"]}"'
                        )
                        row_count = cnt
                    except Exception:
                        pass
                result.append(
                    SchemaTableSummary(
                        schema_name=r["table_schema"],
                        table_name=r["table_name"],
                        column_count=r["col_count"],
                        row_count=row_count,
                    )
                )
            return result
        finally:
            await conn.close()

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
        config = self._connections.get(connection_id)
        if not config:
            raise KeyError(f"Conexão não encontrada: {connection_id}")
        conn = await asyncpg.connect(config.url)
        try:
            explain_query = f"EXPLAIN (FORMAT TEXT) {query.strip().rstrip(';')}"
            rows = await conn.fetch(explain_query)
            return "\n".join(r["QUERY PLAN"] for r in rows)
        finally:
            await conn.close()

    async def validate_query_sql(self, connection_id: str, query: str) -> bool:
        config = self._connections.get(connection_id)
        if not config:
            raise KeyError(f"Conexão não encontrada: {connection_id}")
        conn = await asyncpg.connect(config.url)
        try:
            await conn.fetch(f"PREPARE _nautilus_validate AS {query.strip().rstrip(';')}")
            await conn.fetch("DEALLOCATE _nautilus_validate")
            return True
        except Exception:
            return False
        finally:
            await conn.close()

    async def list_indexes(
        self,
        connection_id: str,
        table_name: str,
        schema: str | None = None,
    ) -> list[IndexInfo]:
        config = self._connections.get(connection_id)
        if not config:
            raise KeyError(f"Conexão não encontrada: {connection_id}")
        schema = schema or "public"
        conn = await asyncpg.connect(config.url)
        try:
            q = """
                SELECT i.relname AS index_name, a.attname AS column_name,
                       ix.indisunique AS is_unique
                FROM pg_index ix
                JOIN pg_class t ON t.oid = ix.indrelid
                JOIN pg_class i ON i.oid = ix.indexrelid
                JOIN pg_namespace n ON n.oid = t.relnamespace
                JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(ix.indkey) AND a.attnum > 0 AND NOT a.attisdropped
                WHERE n.nspname = $1 AND t.relname = $2
                ORDER BY i.relname, array_position(ix.indkey, a.attnum)
            """
            rows = await conn.fetch(q, schema, table_name)
            by_index: dict[str, IndexInfo] = {}
            for r in rows:
                iname = r["index_name"]
                if iname not in by_index:
                    by_index[iname] = IndexInfo(index_name=iname, columns=[], is_unique=r["is_unique"])
                by_index[iname].columns.append(r["column_name"])
            return list(by_index.values())
        finally:
            await conn.close()

    async def list_views(
        self,
        connection_id: str,
        schema: str | None = None,
    ) -> list[ViewInfo]:
        config = self._connections.get(connection_id)
        if not config:
            raise KeyError(f"Conexão não encontrada: {connection_id}")
        conn = await asyncpg.connect(config.url)
        try:
            schema_filter = "AND table_schema = $1" if schema else "AND table_schema NOT IN ('pg_catalog', 'information_schema')"
            params = (schema,) if schema else ()
            q = f"""
                SELECT table_schema, table_name, view_definition
                FROM information_schema.views
                WHERE 1=1 {schema_filter}
                ORDER BY table_schema, table_name
            """
            rows = await conn.fetch(q, *params)
            return [
                ViewInfo(
                    schema_name=r["table_schema"],
                    view_name=r["table_name"],
                    definition=r.get("view_definition"),
                )
                for r in rows
            ]
        finally:
            await conn.close()

    async def get_foreign_keys(
        self,
        connection_id: str,
        table_name: str,
        schema: str | None = None,
    ) -> list[ForeignKeyInfo]:
        config = self._connections.get(connection_id)
        if not config:
            raise KeyError(f"Conexão não encontrada: {connection_id}")
        schema = schema or "public"
        conn = await asyncpg.connect(config.url)
        try:
            q = """
                SELECT
                    c.conname AS constraint_name,
                    n1.nspname AS from_schema, t1.relname AS from_table,
                    (SELECT array_agg(a.attname ORDER BY u.attposition)
                     FROM pg_attribute a
                     JOIN unnest(c.conkey) WITH ORDINALITY AS u(attnum, attposition) ON a.attnum = u.attnum
                     WHERE a.attrelid = c.conrelid AND a.attnum > 0 AND NOT a.attisdropped) AS from_cols,
                    n2.nspname AS to_schema, t2.relname AS to_table,
                    (SELECT array_agg(a.attname ORDER BY u.attposition)
                     FROM pg_attribute a
                     JOIN unnest(c.confkey) WITH ORDINALITY AS u(attnum, attposition) ON a.attnum = u.attnum
                     WHERE a.attrelid = c.confrelid AND a.attnum > 0 AND NOT a.attisdropped) AS to_cols
                FROM pg_constraint c
                JOIN pg_class t1 ON t1.oid = c.conrelid
                JOIN pg_namespace n1 ON n1.oid = t1.relnamespace
                JOIN pg_class t2 ON t2.oid = c.confrelid
                JOIN pg_namespace n2 ON n2.oid = t2.relnamespace
                WHERE c.contype = 'f' AND n1.nspname = $1 AND t1.relname = $2
            """
            rows = await conn.fetch(q, schema, table_name)
            result = []
            for r in rows:
                from_cols = list(r["from_cols"]) if r["from_cols"] else []
                to_cols = list(r["to_cols"]) if r["to_cols"] else []
                result.append(
                    ForeignKeyInfo(
                        constraint_name=r["constraint_name"],
                        from_schema=r["from_schema"],
                        from_table=r["from_table"],
                        from_columns=from_cols,
                        to_schema=r["to_schema"],
                        to_table=r["to_table"],
                        to_columns=to_cols,
                    )
                )
            return result
        finally:
            await conn.close()

    async def get_table_relationships(
        self,
        connection_id: str,
        schema: str | None = None,
    ) -> list[TableRelationship]:
        config = self._connections.get(connection_id)
        if not config:
            raise KeyError(f"Conexão não encontrada: {connection_id}")
        schema = schema or "public"
        conn = await asyncpg.connect(config.url)
        try:
            q = """
                SELECT c.conname, t1.relname AS from_t, t2.relname AS to_t
                FROM pg_constraint c
                JOIN pg_class t1 ON t1.oid = c.conrelid
                JOIN pg_namespace n1 ON n1.oid = t1.relnamespace
                JOIN pg_class t2 ON t2.oid = c.confrelid
                WHERE c.contype = 'f' AND n1.nspname = $1
            """
            rows = await conn.fetch(q, schema)
            return [
                TableRelationship(
                    from_table=r["from_t"],
                    to_table=r["to_t"],
                    constraint_name=r["conname"],
                )
                for r in rows
            ]
        finally:
            await conn.close()

    async def get_row_count(
        self,
        connection_id: str,
        table_name: str,
        schema: str | None = None,
        where_clause: str | None = None,
    ) -> int:
        config = self._connections.get(connection_id)
        if not config:
            raise KeyError(f"Conexão não encontrada: {connection_id}")
        schema = schema or "public"
        conn = await asyncpg.connect(config.url)
        try:
            where = f" WHERE {where_clause}" if where_clause else ""
            q = f'SELECT count(*) FROM "{schema}"."{table_name}"{where}'
            return await conn.fetchval(q) or 0
        finally:
            await conn.close()

    async def get_column_stats(
        self,
        connection_id: str,
        table_name: str,
        schema: str | None = None,
        column_names: list[str] | None = None,
    ) -> list[ColumnStat]:
        config = self._connections.get(connection_id)
        if not config:
            raise KeyError(f"Conexão não encontrada: {connection_id}")
        schema = schema or "public"
        conn = await asyncpg.connect(config.url)
        result = []
        try:
            info = await self.describe_table(connection_id, table_name, schema=schema)
            if not info:
                return []
            cols = [c for c in info.columns if column_names is None or c.name in column_names][:20]
            for col in cols:
                safe_col = f'"{col.name}"'
                safe_table = f'"{schema}"."{table_name}"'
                try:
                    if col.data_type in ("integer", "bigint", "smallint", "numeric", "real", "double precision"):
                        row = await conn.fetchrow(
                            f"SELECT count(*) AS cnt, count({safe_col}) AS non_null, min({safe_col}) AS mn, max({safe_col}) AS mx, avg({safe_col})::numeric AS av FROM {safe_table}"
                        )
                        if row:
                            result.append(ColumnStat(column_name=col.name, stat_type="count", value=row["cnt"]))
                            result.append(ColumnStat(column_name=col.name, stat_type="null_count", value=row["cnt"] - row["non_null"]))
                            if row["mn"] is not None:
                                result.append(ColumnStat(column_name=col.name, stat_type="min", value=row["mn"]))
                                result.append(ColumnStat(column_name=col.name, stat_type="max", value=row["mx"]))
                                result.append(ColumnStat(column_name=col.name, stat_type="avg", value=float(row["av"]) if row["av"] else None))
                    else:
                        row = await conn.fetchrow(
                            f"SELECT count(*) AS cnt, count(DISTINCT {safe_col}) AS d FROM {safe_table}"
                        )
                        if row:
                            result.append(ColumnStat(column_name=col.name, stat_type="count", value=row["cnt"]))
                            result.append(ColumnStat(column_name=col.name, stat_type="distinct_count", value=row["d"]))
                except Exception:
                    pass
            return result
        finally:
            await conn.close()

    async def suggest_tables(
        self,
        connection_id: str,
        search_term: str,
        schema: str | None = None,
    ) -> list[tuple[str, str, str]]:
        """Retorna (schema_name, table_name, column_name) onde tabela ou coluna contém o termo."""
        config = self._connections.get(connection_id)
        if not config:
            raise KeyError(f"Conexão não encontrada: {connection_id}")
        conn = await asyncpg.connect(config.url)
        try:
            term = f"%{search_term}%"
            schema_filter = "AND table_schema = $2" if schema else ""
            params = (term,) if not schema else (term, schema)
            q = f"""
                SELECT DISTINCT table_schema, table_name, column_name
                FROM information_schema.columns
                WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
                  AND (table_name ILIKE $1 OR column_name ILIKE $1) {schema_filter}
                ORDER BY table_schema, table_name, column_name
            """
            rows = await conn.fetch(q, *params)
            return [(r["table_schema"], r["table_name"], r["column_name"]) for r in rows]
        finally:
            await conn.close()

    async def execute_sql_raw(self, connection_id: str, query: str) -> None:
        """Executa um comando SQL sem validação (apenas para confirm_write com token)."""
        config = self._connections.get(connection_id)
        if not config:
            raise KeyError(f"Conexão não encontrada: {connection_id}")
        conn = await asyncpg.connect(config.url)
        try:
            await conn.execute(query.strip().rstrip(";"))
        finally:
            await conn.close()

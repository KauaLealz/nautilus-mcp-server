"""
Adapter para MySQL (aiomysql).
Implementa ConnectionProvider, SqlQueryExecutor e SchemaIntrospector.
"""
from urllib.parse import urlparse, unquote

import aiomysql

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


def _row(r: dict) -> dict:
    """Normaliza chaves do cursor para minúsculas (MySQL pode retornar TABLE_SCHEMA etc.)."""
    return {k.lower(): v for k, v in r.items()} if r else {}


def _parse_mysql_url(url: str) -> dict:
    """Extrai host, port, user, password, db de uma URL mysql://."""
    parsed = urlparse(url)
    if parsed.scheme not in ("mysql", "mysql+pymysql"):
        raise ValueError(f"URL inválida para MySQL: {parsed.scheme}")
    password = unquote(parsed.password) if parsed.password else ""
    port = parsed.port or 3306
    return {
        "host": parsed.hostname or "localhost",
        "port": port,
        "user": unquote(parsed.username) if parsed.username else "",
        "password": password,
        "db": (parsed.path or "/").lstrip("/") or None,
    }


class MysqlAdapter:
    """Adapter para MySQL: conexão, execução de leitura e introspectação de schema."""

    def __init__(self, connections: dict[str, DatabaseConfig], timeout_seconds: int = 30):
        self._connections = {k: v for k, v in connections.items() if v.type == "mysql"}
        self._timeout = timeout_seconds

    def _connect_kwargs(self, config) -> dict:
        kwargs = self._connect_kwargs(config)
        kwargs["connect_timeout"] = min(10, self._timeout)
        kwargs["read_timeout"] = self._timeout
        return kwargs

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
            kwargs = self._connect_kwargs(config)
            conn = await aiomysql.connect(**kwargs)
            conn.close()
            return True
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
        kwargs = self._connect_kwargs(config)
        conn = await aiomysql.connect(**kwargs)
        try:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                await cur.execute(query, ())
                rows = await cur.fetchmany(max_rows)
            if not rows:
                return QueryResult(columns=[], rows=[], row_count=0)
            columns = list(rows[0].keys())
            row_list = [[row[c] for c in columns] for row in rows]
            return QueryResult(columns=columns, rows=row_list, row_count=len(row_list))
        finally:
            conn.close()

    async def list_tables(
        self,
        connection_id: str,
        schema: str | None = None,
    ) -> list[tuple[str, str]]:
        config = self._connections.get(connection_id)
        if not config:
            raise KeyError(f"Conexão não encontrada: {connection_id}")
        kwargs = self._connect_kwargs(config)
        conn = await aiomysql.connect(**kwargs)
        try:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                if schema:
                    await cur.execute(
                        "SELECT table_schema, table_name FROM information_schema.tables "
                        "WHERE table_schema = %s ORDER BY table_schema, table_name",
                        (schema,),
                    )
                else:
                    await cur.execute(
                        "SELECT table_schema, table_name FROM information_schema.tables "
                        "WHERE table_schema NOT IN ('information_schema', 'mysql', 'performance_schema', 'sys') "
                        "ORDER BY table_schema, table_name"
                    )
                rows = await cur.fetchall()
            return [(_row(r)["table_schema"], _row(r)["table_name"]) for r in rows]
        finally:
            conn.close()

    async def describe_table(
        self,
        connection_id: str,
        table_name: str,
        schema: str | None = None,
    ) -> TableInfo | None:
        config = self._connections.get(connection_id)
        if not config:
            raise KeyError(f"Conexão não encontrada: {connection_id}")
        kwargs = self._connect_kwargs(config)
        conn = await aiomysql.connect(**kwargs)
        try:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                if schema:
                    await cur.execute(
                        "SELECT table_schema, table_name, column_name, data_type, is_nullable "
                        "FROM information_schema.columns WHERE table_name = %s AND table_schema = %s "
                        "ORDER BY ordinal_position",
                        (table_name, schema),
                    )
                else:
                    await cur.execute(
                        "SELECT table_schema, table_name, column_name, data_type, is_nullable "
                        "FROM information_schema.columns WHERE table_name = %s "
                        "ORDER BY ordinal_position",
                        (table_name,),
                    )
                rows = await cur.fetchall()
            if not rows:
                return None
            r0 = _row(rows[0])
            schema_name = r0["table_schema"]
            table = r0["table_name"]
            columns = [
                ColumnInfo(
                    name=_row(r)["column_name"],
                    data_type=_row(r)["data_type"],
                    nullable=_row(r)["is_nullable"] == "YES",
                )
                for r in rows
            ]
            return TableInfo(schema_name=schema_name, table_name=table, columns=columns)
        finally:
            conn.close()

    async def list_databases(self, connection_id: str) -> list[str]:
        config = self._connections.get(connection_id)
        if not config:
            raise KeyError(f"Conexão não encontrada: {connection_id}")
        kwargs = self._connect_kwargs(config)
        conn = await aiomysql.connect(**kwargs)
        try:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                await cur.execute("SELECT schema_name FROM information_schema.schemata WHERE schema_name NOT IN ('information_schema', 'mysql', 'performance_schema', 'sys') ORDER BY schema_name")
                rows = await cur.fetchall()
            return [_row(r)["schema_name"] for r in rows]
        finally:
            conn.close()

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
        kwargs = self._connect_kwargs(config)
        schema = schema or (kwargs.get("db") or "public")
        conn = await aiomysql.connect(**kwargs)
        try:
            limit = min(limit, 100)
            async with conn.cursor(aiomysql.DictCursor) as cur:
                await cur.execute(f"SELECT * FROM `{schema}`.`{table_name}` LIMIT %s", (limit,))
                rows = await cur.fetchall()
            if not rows:
                return QueryResult(columns=[], rows=[], row_count=0)
            r0 = _row(rows[0])
            columns = list(r0.keys())
            row_list = [[_row(row).get(c) for c in columns] for row in rows]
            return QueryResult(columns=columns, rows=row_list, row_count=len(row_list))
        finally:
            conn.close()

    async def get_schema_summary(
        self,
        connection_id: str,
        schema: str | None = None,
        include_row_count: bool = False,
    ) -> list[SchemaTableSummary]:
        config = self._connections.get(connection_id)
        if not config:
            raise KeyError(f"Conexão não encontrada: {connection_id}")
        kwargs = self._connect_kwargs(config)
        conn = await aiomysql.connect(**kwargs)
        try:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                if schema:
                    await cur.execute(
                        "SELECT t.table_schema, t.table_name, (SELECT count(*) FROM information_schema.columns c WHERE c.table_schema=t.table_schema AND c.table_name=t.table_name) AS col_count FROM information_schema.tables t WHERE t.table_schema = %s AND t.table_type = 'BASE TABLE' ORDER BY t.table_schema, t.table_name",
                        (schema,),
                    )
                else:
                    await cur.execute(
                        "SELECT t.table_schema, t.table_name, (SELECT count(*) FROM information_schema.columns c WHERE c.table_schema=t.table_schema AND c.table_name=t.table_name) AS col_count FROM information_schema.tables t WHERE t.table_schema NOT IN ('information_schema','mysql','performance_schema','sys') AND t.table_type = 'BASE TABLE' ORDER BY t.table_schema, t.table_name"
                    )
                rows = await cur.fetchall()
            result = []
            for r in rows:
                r = _row(r)
                row_count = None
                if include_row_count:
                    try:
                        async with conn.cursor(aiomysql.DictCursor) as c2:
                            await c2.execute(f"SELECT count(*) AS cnt FROM `{r['table_schema']}`.`{r['table_name']}`")
                            row_count = _row(await c2.fetchone()).get("cnt")
                    except Exception:
                        pass
                result.append(SchemaTableSummary(schema_name=r["table_schema"], table_name=r["table_name"], column_count=r["col_count"], row_count=row_count))
            return result
        finally:
            conn.close()

    async def export_schema_json(
        self,
        connection_id: str,
        schema: str | None = None,
    ) -> dict:
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
        kwargs = self._connect_kwargs(config)
        conn = await aiomysql.connect(**kwargs)
        try:
            q = query.strip().rstrip(";")
            async with conn.cursor(aiomysql.DictCursor) as cur:
                await cur.execute(f"EXPLAIN {q}")
                rows = await cur.fetchall()
            lines = []
            for r in rows:
                lines.append(str(dict(r)))
            return "\n".join(lines)
        finally:
            conn.close()

    async def validate_query_sql(self, connection_id: str, query: str) -> bool:
        config = self._connections.get(connection_id)
        if not config:
            raise KeyError(f"Conexão não encontrada: {connection_id}")
        kwargs = self._connect_kwargs(config)
        conn = await aiomysql.connect(**kwargs)
        try:
            async with conn.cursor() as cur:
                await cur.execute(f"PREPARE stmt FROM %s", (query.strip().rstrip(";"),))
                await cur.execute("DEALLOCATE PREPARE stmt")
            return True
        except Exception:
            return False
        finally:
            conn.close()

    async def list_indexes(
        self,
        connection_id: str,
        table_name: str,
        schema: str | None = None,
    ) -> list[IndexInfo]:
        config = self._connections.get(connection_id)
        if not config:
            raise KeyError(f"Conexão não encontrada: {connection_id}")
        kwargs = self._connect_kwargs(config)
        schema = schema or kwargs.get("db") or "public"
        conn = await aiomysql.connect(**kwargs)
        try:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                await cur.execute("SHOW INDEX FROM `%s` IN `%s`" % (table_name, schema))
                rows = await cur.fetchall()
            by_name: dict[str, IndexInfo] = {}
            for r in rows:
                r = _row(r)
                iname = r.get("key_name")
                if iname not in by_name:
                    by_name[iname] = IndexInfo(index_name=iname, columns=[], is_unique=(r.get("non_unique") == 0))
                by_name[iname].columns.append(r.get("column_name"))
            return list(by_name.values())
        finally:
            conn.close()

    async def list_views(self, connection_id: str, schema: str | None = None) -> list[ViewInfo]:
        config = self._connections.get(connection_id)
        if not config:
            raise KeyError(f"Conexão não encontrada: {connection_id}")
        kwargs = self._connect_kwargs(config)
        conn = await aiomysql.connect(**kwargs)
        try:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                if schema:
                    await cur.execute("SELECT table_schema, table_name, view_definition FROM information_schema.views WHERE table_schema = %s ORDER BY table_schema, table_name", (schema,))
                else:
                    await cur.execute("SELECT table_schema, table_name, view_definition FROM information_schema.views WHERE table_schema NOT IN ('information_schema','mysql','performance_schema','sys') ORDER BY table_schema, table_name")
                rows = await cur.fetchall()
            return [ViewInfo(schema_name=_row(r)["table_schema"], view_name=_row(r)["table_name"], definition=_row(r).get("view_definition")) for r in rows]
        finally:
            conn.close()

    async def get_foreign_keys(
        self,
        connection_id: str,
        table_name: str,
        schema: str | None = None,
    ) -> list[ForeignKeyInfo]:
        config = self._connections.get(connection_id)
        if not config:
            raise KeyError(f"Conexão não encontrada: {connection_id}")
        kwargs = self._connect_kwargs(config)
        schema = schema or kwargs.get("db") or "public"
        conn = await aiomysql.connect(**kwargs)
        try:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                await cur.execute(
                    "SELECT constraint_name, table_schema AS from_schema, table_name AS from_table, column_name AS from_col, referenced_table_schema AS to_schema, referenced_table_name AS to_table, referenced_column_name AS to_col FROM information_schema.key_column_usage WHERE table_schema = %s AND table_name = %s AND referenced_table_name IS NOT NULL ORDER BY ordinal_position",
                    (schema, table_name),
                )
                rows = await cur.fetchall()
            by_constraint: dict[str, list[dict]] = {}
            for r in rows:
                r = _row(r)
                cname = r.get("constraint_name")
                if cname not in by_constraint:
                    by_constraint[cname] = []
                by_constraint[cname].append(r)
            result = []
            for cname, grp in by_constraint.items():
                r0 = grp[0]
                result.append(ForeignKeyInfo(
                    constraint_name=cname,
                    from_schema=r0["from_schema"],
                    from_table=r0["from_table"],
                    from_columns=[x["from_col"] for x in grp],
                    to_schema=r0["to_schema"],
                    to_table=r0["to_table"],
                    to_columns=[x["to_col"] for x in grp],
                ))
            return result
        finally:
            conn.close()

    async def get_table_relationships(
        self,
        connection_id: str,
        schema: str | None = None,
    ) -> list[TableRelationship]:
        config = self._connections.get(connection_id)
        if not config:
            raise KeyError(f"Conexão não encontrada: {connection_id}")
        kwargs = self._connect_kwargs(config)
        schema = schema or kwargs.get("db") or "public"
        conn = await aiomysql.connect(**kwargs)
        try:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                await cur.execute(
                    "SELECT constraint_name, table_name AS from_t, referenced_table_name AS to_t FROM information_schema.referential_constraints rc JOIN information_schema.key_column_usage k ON rc.constraint_name = k.constraint_name AND rc.table_schema = k.table_schema WHERE rc.table_schema = %s GROUP BY constraint_name, table_name, referenced_table_name",
                    (schema,),
                )
                rows = await cur.fetchall()
            return [TableRelationship(from_table=_row(r)["from_t"], to_table=_row(r)["to_t"], constraint_name=_row(r)["constraint_name"]) for r in rows]
        finally:
            conn.close()

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
        kwargs = self._connect_kwargs(config)
        schema = schema or kwargs.get("db") or "public"
        conn = await aiomysql.connect(**kwargs)
        try:
            where = f" WHERE {where_clause}" if where_clause else ""
            async with conn.cursor(aiomysql.DictCursor) as cur:
                await cur.execute(f"SELECT count(*) AS cnt FROM `{schema}`.`{table_name}`{where}")
                r = await cur.fetchone()
            return _row(r).get("cnt") or 0
        finally:
            conn.close()

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
        info = await self.describe_table(connection_id, table_name, schema=schema)
        if not info:
            return []
        kwargs = self._connect_kwargs(config)
        schema = schema or kwargs.get("db") or "public"
        conn = await aiomysql.connect(**kwargs)
        result = []
        cols = [c for c in info.columns if column_names is None or c.name in column_names][:20]
        try:
            for col in cols:
                try:
                    if col.data_type in ("int", "bigint", "smallint", "decimal", "float", "double"):
                        async with conn.cursor(aiomysql.DictCursor) as cur:
                            await cur.execute(f"SELECT count(*) AS cnt, count(`{col.name}`) AS non_null, min(`{col.name}`) AS mn, max(`{col.name}`) AS mx, avg(`{col.name}`) AS av FROM `{schema}`.`{table_name}`")
                            row = _row(await cur.fetchone())
                        if row and row.get("cnt"):
                            result.append(ColumnStat(column_name=col.name, stat_type="count", value=row["cnt"]))
                            result.append(ColumnStat(column_name=col.name, stat_type="null_count", value=row["cnt"] - row.get("non_null", 0)))
                            if row.get("mn") is not None:
                                result.append(ColumnStat(column_name=col.name, stat_type="min", value=row["mn"]))
                                result.append(ColumnStat(column_name=col.name, stat_type="max", value=row["mx"]))
                                result.append(ColumnStat(column_name=col.name, stat_type="avg", value=float(row["av"]) if row.get("av") else None))
                    else:
                        async with conn.cursor(aiomysql.DictCursor) as cur:
                            await cur.execute(f"SELECT count(*) AS cnt, count(DISTINCT `{col.name}`) AS d FROM `{schema}`.`{table_name}`")
                            row = _row(await cur.fetchone())
                        if row:
                            result.append(ColumnStat(column_name=col.name, stat_type="count", value=row.get("cnt")))
                            result.append(ColumnStat(column_name=col.name, stat_type="distinct_count", value=row.get("d")))
                except Exception:
                    pass
            return result
        finally:
            conn.close()

    async def suggest_tables(
        self,
        connection_id: str,
        search_term: str,
        schema: str | None = None,
    ) -> list[tuple[str, str, str]]:
        config = self._connections.get(connection_id)
        if not config:
            raise KeyError(f"Conexão não encontrada: {connection_id}")
        kwargs = self._connect_kwargs(config)
        conn = await aiomysql.connect(**kwargs)
        try:
            term = f"%{search_term}%"
            async with conn.cursor(aiomysql.DictCursor) as cur:
                if schema:
                    await cur.execute(
                        "SELECT DISTINCT table_schema, table_name, column_name FROM information_schema.columns WHERE table_schema NOT IN ('information_schema','mysql','performance_schema','sys') AND (table_name LIKE %s OR column_name LIKE %s) AND table_schema = %s ORDER BY table_schema, table_name, column_name",
                        (term, term, schema),
                    )
                else:
                    await cur.execute(
                        "SELECT DISTINCT table_schema, table_name, column_name FROM information_schema.columns WHERE table_schema NOT IN ('information_schema','mysql','performance_schema','sys') AND (table_name LIKE %s OR column_name LIKE %s) ORDER BY table_schema, table_name, column_name",
                        (term, term),
                    )
                rows = await cur.fetchall()
            return [(_row(r)["table_schema"], _row(r)["table_name"], _row(r)["column_name"]) for r in rows]
        finally:
            conn.close()

    async def execute_sql_raw(self, connection_id: str, query: str) -> None:
        config = self._connections.get(connection_id)
        if not config:
            raise KeyError(f"Conexão não encontrada: {connection_id}")
        kwargs = self._connect_kwargs(config)
        conn = await aiomysql.connect(**kwargs)
        try:
            async with conn.cursor() as cur:
                await cur.execute(query.strip().rstrip(";"))
            conn.commit()
        finally:
            conn.close()

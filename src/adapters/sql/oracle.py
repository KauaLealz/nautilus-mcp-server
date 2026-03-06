"""
Adapter para Oracle Database (oracledb).
Implementa a mesma interface dos outros adapters SQL (async com oracledb.connect_async).
"""
from typing import Any
from urllib.parse import urlparse, unquote

import oracledb

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


def _parse_oracle_url(url: str) -> dict[str, str]:
    """Extrai user, password e dsn de uma URL oracle:// ou user/password@host:port/service."""
    url = url.strip()
    if url.startswith("oracle://"):
        parsed = urlparse(url)
        user = unquote(parsed.username or "")
        password = unquote(parsed.password or "")
        host = parsed.hostname or "localhost"
        port = parsed.port or 1521
        path = (parsed.path or "").lstrip("/")
        service = path or "ORCL"
        dsn = f"{host}:{port}/{service}"
        return {"user": user, "password": password, "dsn": dsn}
    # Formato legacy: user/password@host:port/service
    if "@" in url:
        part, dsn = url.rsplit("@", 1)
        if "/" in part:
            user, password = part.split("/", 1)
            return {"user": user.strip(), "password": password.strip(), "dsn": dsn.strip()}
    raise ValueError("URL Oracle inválida. Use oracle://user:password@host:port/service_name ou user/password@host:port/service")


class OracleAdapter:
    """Adapter para Oracle Database: conexão, execução de leitura e introspectação de schema."""

    def __init__(self, connections: dict[str, DatabaseConfig], timeout_seconds: int = 30):
        self._connections = {k: v for k, v in connections.items() if v.type == "oracle"}
        self._timeout = timeout_seconds
        self._params: dict[str, dict[str, str]] = {}
        for cid, cfg in self._connections.items():
            try:
                self._params[cid] = _parse_oracle_url(cfg.url)
            except Exception:
                self._params[cid] = {}

    def list_connections(self) -> list[ConnectionInfo]:
        return [connection_info_from_config(cid, c) for cid, c in self._connections.items()]

    def get_connection_info(self, connection_id: str) -> ConnectionInfo | None:
        config = self._connections.get(connection_id)
        if not config:
            return None
        return connection_info_from_config(connection_id, config)

    async def _connect(self, connection_id: str):
        params = self._params.get(connection_id)
        if not params:
            raise KeyError(f"Conexão não encontrada: {connection_id}")
        kwargs = {
            "user": params["user"],
            "password": params["password"],
            "dsn": params["dsn"],
        }
        try:
            # call_timeout (ms) - suportado em oracledb 2.0+
            kwargs["call_timeout"] = self._timeout * 1000
        except Exception:
            pass
        try:
            return await oracledb.connect_async(**kwargs)
        except TypeError:
            kwargs.pop("call_timeout", None)
            return await oracledb.connect_async(**kwargs)

    async def test_connection(self, connection_id: str) -> bool:
        try:
            conn = await self._connect(connection_id)
            try:
                cursor = conn.cursor()
                await cursor.execute("SELECT 1 FROM DUAL")
                await cursor.fetchone()
            finally:
                await conn.close()
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
        conn = await self._connect(connection_id)
        try:
            raw_q = query.strip().rstrip(";").rstrip("/")
            if "FETCH FIRST" not in raw_q.upper() and "ROWNUM" not in raw_q.upper() and "LIMIT" not in raw_q.upper():
                q = f"SELECT * FROM ({raw_q}) FETCH FIRST {max_rows} ROWS ONLY"
            else:
                q = raw_q
            cursor = conn.cursor()
            try:
                await cursor.execute(q)
            except Exception:
                if "FETCH FIRST" in q.upper():
                    q = f"SELECT * FROM ({raw_q}) WHERE ROWNUM <= {max_rows}"
                    await cursor.execute(q)
                else:
                    raise
            columns = [d[0] for d in cursor.description] if cursor.description else []
            try:
                rows = await cursor.fetchmany(max_rows)
            except (AttributeError, TypeError):
                rows = []
                count = 0
                async for r in cursor:
                    rows.append(r)
                    count += 1
                    if count >= max_rows:
                        break
            row_list = [list(r) for r in rows]
            await cursor.close()
            return QueryResult(columns=columns, rows=row_list, row_count=len(row_list))
        except Exception as e:
            raise ValueError(f"Oracle execute_read_only: {e!s}") from e
        finally:
            await conn.close()

    async def list_tables(
        self,
        connection_id: str,
        schema: str | None = None,
    ) -> list[tuple[str, str]]:
        conn = await self._connect(connection_id)
        try:
            cursor = conn.cursor()
            try:
                if schema:
                    await cursor.execute(
                        "SELECT OWNER, TABLE_NAME FROM ALL_TABLES WHERE OWNER = :1 ORDER BY OWNER, TABLE_NAME",
                        [schema.upper()],
                    )
                else:
                    await cursor.execute(
                        "SELECT USER AS OWNER, TABLE_NAME FROM USER_TABLES ORDER BY TABLE_NAME"
                    )
                # AsyncCursor: usar async iteration se fetchall não existir ou falhar
                try:
                    rows = await cursor.fetchall()
                except (AttributeError, TypeError):
                    rows = [r async for r in cursor]
            finally:
                try:
                    await cursor.close()
                except Exception:
                    pass
            return [(r[0], r[1]) for r in rows]
        except Exception as e:
            raise ValueError(f"Oracle list_tables: {e!s}") from e
        finally:
            await conn.close()

    async def describe_table(
        self,
        connection_id: str,
        table_name: str,
        schema: str | None = None,
    ) -> TableInfo | None:
        conn = await self._connect(connection_id)
        try:
            cursor = conn.cursor()
            if schema:
                await cursor.execute(
                    """SELECT OWNER, TABLE_NAME, COLUMN_NAME, DATA_TYPE, NULLABLE
                       FROM ALL_TAB_COLUMNS WHERE TABLE_NAME = :1 AND OWNER = :2 ORDER BY COLUMN_ID""",
                    [table_name.upper(), schema.upper()],
                )
            else:
                await cursor.execute(
                    """SELECT OWNER, TABLE_NAME, COLUMN_NAME, DATA_TYPE, NULLABLE
                       FROM ALL_TAB_COLUMNS WHERE TABLE_NAME = :1 ORDER BY OWNER, COLUMN_ID""",
                    [table_name.upper()],
                )
            rows = await cursor.fetchall()
            await cursor.close()
            if not rows:
                return None
            schema_name, tbl, _ = rows[0][0], rows[0][1], rows[0][2]
            columns = [
                ColumnInfo(
                    name=r[2],
                    data_type=r[3],
                    nullable=r[4] == "Y",
                )
                for r in rows
            ]
            return TableInfo(schema_name=schema_name, table_name=tbl, columns=columns)
        finally:
            await conn.close()

    async def list_databases(self, connection_id: str) -> list[str]:
        conn = await self._connect(connection_id)
        try:
            cursor = conn.cursor()
            try:
                await cursor.execute(
                    "SELECT USERNAME FROM ALL_USERS WHERE USERNAME NOT IN ('SYS','SYSTEM','OUTLN') ORDER BY USERNAME"
                )
                try:
                    rows = await cursor.fetchall()
                except (AttributeError, TypeError):
                    rows = [r async for r in cursor]
            finally:
                try:
                    await cursor.close()
                except Exception:
                    pass
            return [r[0] for r in rows]
        except Exception as e:
            raise ValueError(f"Oracle list_databases: {e!s}") from e
        finally:
            await conn.close()

    async def get_table_sample(
        self,
        connection_id: str,
        table_name: str,
        schema: str | None = None,
        limit: int = 5,
    ) -> QueryResult:
        schema = schema or self._params.get(connection_id, {}).get("user", "USER")
        conn = await self._connect(connection_id)
        try:
            limit = min(limit, 100)
            cursor = conn.cursor()
            await cursor.execute(
                f'SELECT * FROM "{schema}"."{table_name}" WHERE ROWNUM <= :1',
                [limit],
            )
            columns = [d[0] for d in cursor.description] if cursor.description else []
            rows = await cursor.fetchall()
            row_list = [list(r) for r in rows]
            await cursor.close()
            return QueryResult(columns=columns, rows=row_list, row_count=len(row_list))
        finally:
            await conn.close()

    async def get_schema_summary(
        self,
        connection_id: str,
        schema: str | None = None,
        include_row_count: bool = False,
    ) -> list[SchemaTableSummary]:
        conn = await self._connect(connection_id)
        try:
            cursor = conn.cursor()
            if schema:
                await cursor.execute(
                    """SELECT OWNER, TABLE_NAME, (SELECT COUNT(*) FROM ALL_TAB_COLUMNS c WHERE c.OWNER=t.OWNER AND c.TABLE_NAME=t.TABLE_NAME) AS col_count
                       FROM ALL_TABLES t WHERE t.OWNER = :1 ORDER BY OWNER, TABLE_NAME""",
                    [schema.upper()],
                )
            else:
                await cursor.execute(
                    """SELECT OWNER, TABLE_NAME, (SELECT COUNT(*) FROM ALL_TAB_COLUMNS c WHERE c.OWNER=t.OWNER AND c.TABLE_NAME=t.TABLE_NAME) AS col_count
                       FROM ALL_TABLES t WHERE t.OWNER NOT IN ('SYS','SYSTEM') ORDER BY OWNER, TABLE_NAME"""
                )
            rows = await cursor.fetchall()
            await cursor.close()
            result = []
            for r in rows:
                owner, tbl, col_count = r[0], r[1], r[2]
                row_count = None
                if include_row_count:
                    try:
                        c2 = conn.cursor()
                        await c2.execute(f'SELECT COUNT(*) FROM "{owner}"."{tbl}"')
                        row_count = (await c2.fetchone())[0]
                        await c2.close()
                    except Exception:
                        pass
                result.append(SchemaTableSummary(schema_name=owner, table_name=tbl, column_count=col_count, row_count=row_count))
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
        conn = await self._connect(connection_id)
        try:
            q = query.strip().rstrip(";").rstrip("/")
            cursor = conn.cursor()
            await cursor.execute("EXPLAIN PLAN FOR " + q)
            await cursor.execute("SELECT PLAN_TABLE_OUTPUT FROM TABLE(DBMS_XPLAN.DISPLAY)")
            rows = await cursor.fetchall()
            await cursor.close()
            return "\n".join(str(r[0]) for r in rows) if rows else ""
        finally:
            await conn.close()

    async def validate_query_sql(self, connection_id: str, query: str) -> bool:
        try:
            conn = await self._connect(connection_id)
            try:
                cursor = conn.cursor()
                await cursor.execute("EXPLAIN PLAN FOR " + query.strip().rstrip(";").rstrip("/"))
                await cursor.close()
            finally:
                await conn.close()
            return True
        except Exception:
            return False

    async def list_indexes(
        self,
        connection_id: str,
        table_name: str,
        schema: str | None = None,
    ) -> list[IndexInfo]:
        conn = await self._connect(connection_id)
        try:
            cursor = conn.cursor()
            if schema:
                await cursor.execute(
                    """SELECT i.INDEX_NAME, c.COLUMN_NAME, i.UNIQUENESS
                       FROM ALL_INDEXES i JOIN ALL_IND_COLUMNS c ON i.OWNER=c.INDEX_OWNER AND i.INDEX_NAME=c.INDEX_NAME AND i.TABLE_NAME=c.TABLE_NAME
                       WHERE i.TABLE_NAME = :1 AND i.OWNER = :2 ORDER BY i.INDEX_NAME, c.COLUMN_POSITION""",
                    [table_name.upper(), schema.upper()],
                )
            else:
                await cursor.execute(
                    """SELECT i.INDEX_NAME, c.COLUMN_NAME, i.UNIQUENESS
                       FROM ALL_INDEXES i JOIN ALL_IND_COLUMNS c ON i.OWNER=c.INDEX_OWNER AND i.INDEX_NAME=c.INDEX_NAME AND i.TABLE_NAME=c.TABLE_NAME
                       WHERE i.TABLE_NAME = :1 ORDER BY i.INDEX_NAME, c.COLUMN_POSITION""",
                    [table_name.upper()],
                )
            rows = await cursor.fetchall()
            await cursor.close()
            by_name: dict[str, IndexInfo] = {}
            for r in rows:
                iname, col, uniq = r[0], r[1], (r[2] == "UNIQUE")
                if iname not in by_name:
                    by_name[iname] = IndexInfo(index_name=iname, columns=[], is_unique=uniq)
                by_name[iname].columns.append(col)
            return list(by_name.values())
        finally:
            await conn.close()

    async def list_views(
        self,
        connection_id: str,
        schema: str | None = None,
    ) -> list[ViewInfo]:
        conn = await self._connect(connection_id)
        try:
            cursor = conn.cursor()
            if schema:
                await cursor.execute(
                    "SELECT OWNER, VIEW_NAME, TEXT FROM ALL_VIEWS WHERE OWNER = :1 ORDER BY OWNER, VIEW_NAME",
                    [schema.upper()],
                )
            else:
                await cursor.execute(
                    "SELECT OWNER, VIEW_NAME, TEXT FROM ALL_VIEWS WHERE OWNER NOT IN ('SYS','SYSTEM') ORDER BY OWNER, VIEW_NAME"
                )
            rows = await cursor.fetchall()
            await cursor.close()
            return [ViewInfo(schema_name=r[0], view_name=r[1], definition=r[2]) for r in rows]
        finally:
            await conn.close()

    async def get_foreign_keys(
        self,
        connection_id: str,
        table_name: str,
        schema: str | None = None,
    ) -> list[ForeignKeyInfo]:
        conn = await self._connect(connection_id)
        try:
            cursor = conn.cursor()
            if schema:
                await cursor.execute(
                    """SELECT c.CONSTRAINT_NAME, c.OWNER, c.TABLE_NAME, cc.COLUMN_NAME, r.OWNER AS R_OWNER, r.TABLE_NAME AS R_TABLE, rc.COLUMN_NAME AS R_COLUMN
                       FROM ALL_CONSTRAINTS c
                       JOIN ALL_CONS_COLUMNS cc ON c.OWNER=cc.OWNER AND c.CONSTRAINT_NAME=cc.CONSTRAINT_NAME
                       JOIN ALL_CONSTRAINTS r ON c.R_OWNER=r.OWNER AND c.R_CONSTRAINT_NAME=r.CONSTRAINT_NAME
                       JOIN ALL_CONS_COLUMNS rc ON r.OWNER=rc.OWNER AND r.CONSTRAINT_NAME=rc.CONSTRAINT_NAME AND cc.POSITION=rc.POSITION
                       WHERE c.CONSTRAINT_TYPE = 'R' AND c.TABLE_NAME = :1 AND c.OWNER = :2 ORDER BY cc.POSITION""",
                    [table_name.upper(), schema.upper()],
                )
            else:
                await cursor.execute(
                    """SELECT c.CONSTRAINT_NAME, c.OWNER, c.TABLE_NAME, cc.COLUMN_NAME, r.OWNER AS R_OWNER, r.TABLE_NAME AS R_TABLE, rc.COLUMN_NAME AS R_COLUMN
                       FROM ALL_CONSTRAINTS c
                       JOIN ALL_CONS_COLUMNS cc ON c.OWNER=cc.OWNER AND c.CONSTRAINT_NAME=cc.CONSTRAINT_NAME
                       JOIN ALL_CONSTRAINTS r ON c.R_OWNER=r.OWNER AND c.R_CONSTRAINT_NAME=r.CONSTRAINT_NAME
                       JOIN ALL_CONS_COLUMNS rc ON r.OWNER=rc.OWNER AND r.CONSTRAINT_NAME=rc.CONSTRAINT_NAME AND cc.POSITION=rc.POSITION
                       WHERE c.CONSTRAINT_TYPE = 'R' AND c.TABLE_NAME = :1 ORDER BY cc.POSITION""",
                    [table_name.upper()],
                )
            rows = await cursor.fetchall()
            await cursor.close()
            by_constraint: dict[str, list[tuple]] = {}
            for r in rows:
                cname = r[0]
                if cname not in by_constraint:
                    by_constraint[cname] = []
                by_constraint[cname].append(r)
            result = []
            for cname, grp in by_constraint.items():
                r0 = grp[0]
                result.append(ForeignKeyInfo(
                    constraint_name=cname,
                    from_schema=r0[1],
                    from_table=r0[2],
                    from_columns=[x[3] for x in grp],
                    to_schema=r0[4],
                    to_table=r0[5],
                    to_columns=[x[6] for x in grp],
                ))
            return result
        finally:
            await conn.close()

    async def get_table_relationships(
        self,
        connection_id: str,
        schema: str | None = None,
    ) -> list[TableRelationship]:
        conn = await self._connect(connection_id)
        try:
            cursor = conn.cursor()
            if schema:
                await cursor.execute(
                    "SELECT CONSTRAINT_NAME, TABLE_NAME, (SELECT TABLE_NAME FROM ALL_CONSTRAINTS r WHERE r.OWNER=c.R_OWNER AND r.CONSTRAINT_NAME=c.R_CONSTRAINT_NAME) AS R_TABLE FROM ALL_CONSTRAINTS c WHERE c.CONSTRAINT_TYPE = 'R' AND c.OWNER = :1",
                    [schema.upper()],
                )
            else:
                await cursor.execute(
                    "SELECT CONSTRAINT_NAME, TABLE_NAME, (SELECT TABLE_NAME FROM ALL_CONSTRAINTS r WHERE r.OWNER=c.R_OWNER AND r.CONSTRAINT_NAME=c.R_CONSTRAINT_NAME) AS R_TABLE FROM ALL_CONSTRAINTS c WHERE c.CONSTRAINT_TYPE = 'R' AND c.OWNER NOT IN ('SYS','SYSTEM')"
                )
            rows = await cursor.fetchall()
            await cursor.close()
            return [TableRelationship(from_table=r[1], to_table=r[2], constraint_name=r[0]) for r in rows if r[2]]
        finally:
            await conn.close()

    async def get_row_count(
        self,
        connection_id: str,
        table_name: str,
        schema: str | None = None,
        where_clause: str | None = None,
    ) -> int:
        schema = schema or self._params.get(connection_id, {}).get("user", "USER")
        conn = await self._connect(connection_id)
        try:
            cursor = conn.cursor()
            where = f" WHERE {where_clause}" if where_clause else ""
            await cursor.execute(f'SELECT COUNT(*) FROM "{schema}"."{table_name}"{where}')
            row = await cursor.fetchone()
            await cursor.close()
            return row[0] or 0
        finally:
            await conn.close()

    async def get_column_stats(
        self,
        connection_id: str,
        table_name: str,
        schema: str | None = None,
        column_names: list[str] | None = None,
    ) -> list[ColumnStat]:
        info = await self.describe_table(connection_id, table_name, schema=schema)
        if not info:
            return []
        schema_name = schema or self._params.get(connection_id, {}).get("user", "USER")
        conn = await self._connect(connection_id)
        result = []
        cols = [c for c in info.columns if column_names is None or c.name in column_names][:20]
        numeric_types = ("NUMBER", "FLOAT", "BINARY_FLOAT", "BINARY_DOUBLE", "INTEGER")
        try:
            for col in cols:
                try:
                    if col.data_type in numeric_types:
                        cursor = conn.cursor()
                        await cursor.execute(
                            f'SELECT COUNT(*), COUNT("{col.name}"), MIN("{col.name}"), MAX("{col.name}"), AVG("{col.name}") FROM "{schema_name}"."{table_name}"'
                        )
                        row = await cursor.fetchone()
                        await cursor.close()
                        if row and row[0]:
                            result.append(ColumnStat(column_name=col.name, stat_type="count", value=row[0]))
                            result.append(ColumnStat(column_name=col.name, stat_type="null_count", value=row[0] - (row[1] or 0)))
                            if row[2] is not None:
                                result.append(ColumnStat(column_name=col.name, stat_type="min", value=row[2]))
                                result.append(ColumnStat(column_name=col.name, stat_type="max", value=row[3]))
                                result.append(ColumnStat(column_name=col.name, stat_type="avg", value=float(row[4]) if row[4] is not None else None))
                    else:
                        cursor = conn.cursor()
                        await cursor.execute(
                            f'SELECT COUNT(*), COUNT(DISTINCT "{col.name}") FROM "{schema_name}"."{table_name}"'
                        )
                        row = await cursor.fetchone()
                        await cursor.close()
                        if row:
                            result.append(ColumnStat(column_name=col.name, stat_type="count", value=row[0]))
                            result.append(ColumnStat(column_name=col.name, stat_type="distinct_count", value=row[1]))
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
        conn = await self._connect(connection_id)
        try:
            cursor = conn.cursor()
            term = f"%{search_term.upper()}%"
            if schema:
                await cursor.execute(
                    """SELECT DISTINCT OWNER, TABLE_NAME, COLUMN_NAME FROM ALL_TAB_COLUMNS
                       WHERE (TABLE_NAME LIKE :1 OR COLUMN_NAME LIKE :1) AND OWNER = :2 ORDER BY OWNER, TABLE_NAME, COLUMN_NAME""",
                    [term, schema.upper()],
                )
            else:
                await cursor.execute(
                    """SELECT DISTINCT OWNER, TABLE_NAME, COLUMN_NAME FROM ALL_TAB_COLUMNS
                       WHERE TABLE_NAME LIKE :1 OR COLUMN_NAME LIKE :1
                       AND OWNER NOT IN ('SYS','SYSTEM') ORDER BY OWNER, TABLE_NAME, COLUMN_NAME""",
                    [term],
                )
            rows = await cursor.fetchall()
            await cursor.close()
            return [(r[0], r[1], r[2]) for r in rows]
        finally:
            await conn.close()

    async def execute_sql_raw(self, connection_id: str, query: str) -> None:
        conn = await self._connect(connection_id)
        try:
            cursor = conn.cursor()
            await cursor.execute(query.strip().rstrip(";").rstrip("/"))
            await conn.commit()
        finally:
            await conn.close()

import sqlite3
import psycopg
import pymysql
import pymssql
import oracledb
import pymongo
import redis
import time
import re
import os
import logging
from urllib.parse import urlparse, unquote

logger = logging.getLogger("nautilus")

def q_id(s: str) -> str:
    return f'"{str(s).replace(chr(34), chr(34) + chr(34))}"'

def parse_sql_url(url: str):
    parsed = urlparse(url)
    user = unquote(parsed.username) if parsed.username else None
    password = unquote(parsed.password) if parsed.password else None
    host = parsed.hostname
    port = parsed.port
    db = parsed.path.lstrip("/") if parsed.path else None
    return host, port, user, password, db

class SqliteAdapter:
    def __init__(self, configs):
        self.configs = configs
        self.conns = {}

    def get_conn(self, connection_id: str):
        if connection_id not in self.conns:
            cfg = self.configs.get(connection_id)
            if not cfg:
                raise ValueError(f"Conexao nao encontrada: {connection_id}")
            path = cfg["url"].replace("file:", "")
            conn = sqlite3.connect(path, check_same_thread=False)
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA foreign_keys = ON")
            self.conns[connection_id] = conn
        return self.conns[connection_id]

    def test_connection(self, connection_id: str) -> bool:
        try:
            conn = self.get_conn(connection_id)
            conn.execute("SELECT 1").fetchone()
            return True
        except Exception:
            return False

    def probe_connection(self, connection_id: str) -> dict:
        t0 = time.perf_counter()
        try:
            conn = self.get_conn(connection_id)
            row = conn.execute("SELECT sqlite_version() AS v").fetchone()
            latency = int((time.perf_counter() - t0) * 1000)
            return {"ok": True, "latencyMs": latency, "version": str(row["v"])}
        except Exception as e:
            return {"ok": False, "latencyMs": int((time.perf_counter() - t0) * 1000), "version": None, "error": str(e)}

    def execute_read_only(self, connection_id: str, query: str, max_rows: int, timeout_seconds: int) -> dict:
        conn = self.get_conn(connection_id)
        q = query.strip().rstrip(";")
        if not re.search(r"\bLIMIT\s+\d+", q, re.IGNORECASE):
            q = f"{q} LIMIT {max_rows}"
        cursor = conn.cursor()
        cursor.execute(q)
        rows = cursor.fetchall()
        if not rows:
            return {"columns": [d[0] for d in cursor.description] if cursor.description else [], "rows": [], "row_count": 0}
        columns = [d[0] for d in cursor.description]
        out_rows = [[row[col] for col in columns] for row in rows]
        return {"columns": columns, "rows": out_rows, "row_count": len(out_rows)}

    def list_tables(self, connection_id: str, schema: str = None) -> list:
        if schema and schema.strip() != "main":
            return []
        conn = self.get_conn(connection_id)
        cursor = conn.execute("SELECT 'main' AS s, name AS t FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name")
        return [[row["s"], row["t"]] for row in cursor.fetchall()]

    def describe_table(self, connection_id: str, table_name: str, schema: str = None) -> dict:
        conn = self.get_conn(connection_id)
        cursor = conn.execute(f"PRAGMA table_info({q_id(table_name)})")
        rows = cursor.fetchall()
        if not rows:
            return None
        columns = [{"name": row["name"], "data_type": row["type"] or "unknown", "nullable": row["notnull"] == 0} for row in rows]
        return {"schema_name": schema or "main", "table_name": table_name, "columns": columns}

    def list_databases(self, connection_id: str) -> list:
        return ["main"]

    def get_table_sample(self, connection_id: str, table_name: str, schema: str, limit: int, offset: int) -> dict:
        lim = max(1, min(limit, 50000))
        off = max(0, min(offset, 1000000))
        q = f"SELECT * FROM {q_id(table_name)} LIMIT {lim} OFFSET {off}"
        return self.execute_read_only(connection_id, q, lim, 10)

    def get_schema_summary(self, connection_id: str, schema: str, include_row_count: bool) -> list:
        tables = self.list_tables(connection_id, schema)
        out = []
        conn = self.get_conn(connection_id)
        for s, t in tables:
            row_count = None
            if include_row_count:
                try:
                    c = conn.execute(f"SELECT count(*) AS c FROM {q_id(t)}").fetchone()
                    row_count = c["c"]
                except Exception:
                    pass
            cols = conn.execute(f"PRAGMA table_info({q_id(t)})").fetchall()
            out.append({
                "schema_name": s,
                "table_name": t,
                "column_count": len(cols),
                "row_count": row_count
            })
        return out

    def export_schema_json(self, connection_id: str, schema: str) -> dict:
        tables_list = self.list_tables(connection_id, schema)
        tables = []
        for s, t in tables_list:
            info = self.describe_table(connection_id, t, s)
            if info:
                tables.append({
                    "schema": info["schema_name"],
                    "table": info["table_name"],
                    "columns": [{"name": c["name"], "data_type": c["data_type"], "nullable": c["nullable"]} for c in info["columns"]]
                })
        return {"tables": tables}

    def explain_query_sql(self, connection_id: str, query: str) -> str:
        conn = self.get_conn(connection_id)
        q = query.strip().rstrip(";")
        rows = conn.execute(f"EXPLAIN QUERY PLAN {q}").fetchall()
        return "\n".join(str(dict(r)) for r in rows)

    def validate_query_sql(self, connection_id: str, query: str) -> bool:
        try:
            conn = self.get_conn(connection_id)
            q = query.strip().rstrip(";")
            conn.execute(f"EXPLAIN QUERY PLAN {q}")
            return True
        except Exception:
            return False

    def list_indexes(self, connection_id: str, table_name: str, schema: str = None) -> list:
        conn = self.get_conn(connection_id)
        rows = conn.execute(f"PRAGMA index_list({q_id(table_name)})").fetchall()
        out = []
        for r in rows:
            cols = conn.execute(f"PRAGMA index_info({q_id(r['name'])})").fetchall()
            sorted_cols = sorted(cols, key=lambda x: x["seqno"])
            out.append({
                "index_name": r["name"],
                "columns": [c["name"] for c in sorted_cols if c["name"]],
                "is_unique": r["unique"] == 1
            })
        return out

    def list_views(self, connection_id: str, schema: str = None) -> list:
        conn = self.get_conn(connection_id)
        rows = conn.execute("SELECT name, sql FROM sqlite_master WHERE type='view' ORDER BY name").fetchall()
        return [{"schema_name": "main", "view_name": r["name"], "definition": r["sql"]} for r in rows]

    def get_foreign_keys(self, connection_id: str, table_name: str, schema: str = None) -> list:
        conn = self.get_conn(connection_id)
        rows = conn.execute(f"PRAGMA foreign_key_list({q_id(table_name)})").fetchall()
        by_id = {}
        for r in rows:
            fk_id = r["id"]
            if fk_id not in by_id:
                by_id[fk_id] = {
                    "constraint_name": f"fk_{fk_id}",
                    "from_schema": "main",
                    "from_table": table_name,
                    "from_columns": [],
                    "to_schema": "main",
                    "to_table": r["table"],
                    "to_columns": []
                }
            by_id[fk_id]["from_columns"].append(r["from"])
            by_id[fk_id]["to_columns"].append(r["to"] or r["from"])
        return list(by_id.values())

    def get_table_relationships(self, connection_id: str, schema: str = None) -> list:
        tables = self.list_tables(connection_id, schema)
        rels = []
        conn = self.get_conn(connection_id)
        for _, tbl in tables:
            fks = conn.execute(f"PRAGMA foreign_key_list({q_id(tbl)})").fetchall()
            seen = set()
            for fk in fks:
                k = f"{tbl}->{fk['table']}"
                if k in seen:
                    continue
                seen.add(k)
                rels.append({
                    "from_table": tbl,
                    "to_table": fk["table"],
                    "constraint_name": f"fk_{fk['id']}"
                })
        return rels

    def get_row_count(self, connection_id: str, table_name: str, schema: str = None, where_clause: str = None) -> int:
        conn = self.get_conn(connection_id)
        where = f" WHERE {where_clause.strip()}" if where_clause and where_clause.strip() else ""
        c = conn.execute(f"SELECT count(*) AS c FROM {q_id(table_name)}{where}").fetchone()
        return c["c"]

    def get_column_stats(self, connection_id: str, table_name: str, schema: str = None, column_names: list = None) -> list:
        conn = self.get_conn(connection_id)
        info = self.describe_table(connection_id, table_name, schema)
        if not info:
            return []
        cols = info["columns"]
        if column_names:
            col_set = set(column_names)
            cols = [c for c in cols if c["name"] in col_set]
        cols = cols[:20]
        result = []
        safe_table = q_id(table_name)
        for col in cols:
            cname = q_id(col["name"])
            try:
                row = conn.execute(f"SELECT count(*) AS cnt, count({cname}) AS nn FROM {safe_table}").fetchone()
                result.append({"column_name": col["name"], "stat_type": "count", "value": row["cnt"]})
                result.append({"column_name": col["name"], "stat_type": "non_null", "value": row["nn"]})
            except Exception:
                pass
        return result

    def suggest_tables(self, connection_id: str, search_term: str, schema: str = None) -> list:
        conn = self.get_conn(connection_id)
        term = f"%{search_term}%"
        rows = conn.execute("SELECT m.name AS t, p.name AS c FROM sqlite_master m JOIN pragma_table_info(m.name) p WHERE m.type='table' AND (m.name LIKE ? OR p.name LIKE ?)", (term, term)).fetchall()
        return [["main", r["t"], r["c"]] for r in rows]


class PostgresAdapter:
    def __init__(self, configs):
        self.configs = configs
        self.conns = {}

    def get_conn(self, connection_id: str):
        conn = self.conns.get(connection_id)
        if conn is not None:
            is_alive = True
            if conn.closed or getattr(conn, "broken", False):
                is_alive = False
            else:
                try:
                    with conn.cursor() as cur:
                        cur.execute("SELECT 1")
                except Exception:
                    is_alive = False
            if not is_alive:
                logger.info(f"Conexao Postgres {connection_id} inativa ou fechada. Removendo do cache.")
                try:
                    conn.close()
                except Exception:
                    pass
                self.conns.pop(connection_id, None)
                conn = None
        if conn is None:
            cfg = self.configs.get(connection_id)
            if not cfg:
                raise ValueError(f"Conexao nao encontrada: {connection_id}")
            logger.info(f"Criando nova conexao Postgres para {connection_id}")
            conn = psycopg.connect(cfg["url"], autocommit=True, connect_timeout=5)
            self.conns[connection_id] = conn
        return conn

    def test_connection(self, connection_id: str) -> bool:
        try:
            conn = self.get_conn(connection_id)
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                cur.fetchone()
            return True
        except Exception:
            return False

    def probe_connection(self, connection_id: str) -> dict:
        t0 = time.perf_counter()
        try:
            conn = self.get_conn(connection_id)
            with conn.cursor() as cur:
                cur.execute("SELECT version()")
                v = cur.fetchone()[0]
            latency = int((time.perf_counter() - t0) * 1000)
            return {"ok": True, "latencyMs": latency, "version": str(v).split("\n")[0]}
        except Exception as e:
            return {"ok": False, "latencyMs": int((time.perf_counter() - t0) * 1000), "version": None, "error": str(e)}

    def execute_read_only(self, connection_id: str, query: str, max_rows: int, timeout_seconds: int) -> dict:
        q = query.strip().rstrip(";")
        if not re.search(r"\bLIMIT\s+\d+", q, re.IGNORECASE):
            q = f"{q} LIMIT {max_rows}"
        try:
            conn = self.get_conn(connection_id)
            with conn.cursor() as cur:
                cur.execute(f"SET statement_timeout = {timeout_seconds * 1000}")
                cur.execute(q)
                if cur.description is None:
                    return {"columns": [], "rows": [], "row_count": 0}
                columns = [d.name for d in cur.description]
                rows = cur.fetchall()
                out_rows = [list(row) for row in rows]
                return {"columns": columns, "rows": out_rows, "row_count": len(out_rows)}
        except (psycopg.OperationalError, psycopg.InterfaceError) as e:
            logger.warning(f"Erro de conexao Postgres durante query para {connection_id}: {e}")
            self.conns.pop(connection_id, None)
            raise

    def list_tables(self, connection_id: str, schema: str = None) -> list:
        conn = self.get_conn(connection_id)
        schema_filter = "AND table_schema = %s" if schema else ""
        params = [schema] if schema else []
        q = f"""
            SELECT table_schema, table_name
            FROM information_schema.tables
            WHERE table_schema NOT IN ('pg_catalog', 'information_schema') {schema_filter}
            ORDER BY table_schema, table_name
        """
        with conn.cursor() as cur:
            cur.execute(q, params)
            return [list(row) for row in cur.fetchall()]

    def describe_table(self, connection_id: str, table_name: str, schema: str = None) -> dict:
        conn = self.get_conn(connection_id)
        schema_cond = "AND table_schema = %s" if schema else ""
        params = [table_name, schema] if schema else [table_name]
        q = f"""
            SELECT table_schema, table_name, column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_name = %s {schema_cond}
            ORDER BY ordinal_position
        """
        with conn.cursor() as cur:
            cur.execute(q, params)
            rows = cur.fetchall()
            if not rows:
                return None
            columns = [{"name": r[2], "data_type": r[3], "nullable": r[4].upper() == "YES"} for r in rows]
            return {"schema_name": rows[0][0], "table_name": rows[0][1], "columns": columns}

    def list_databases(self, connection_id: str) -> list:
        conn = self.get_conn(connection_id)
        with conn.cursor() as cur:
            cur.execute("SELECT datname FROM pg_database WHERE NOT datistemplate ORDER BY datname")
            return [r[0] for r in cur.fetchall()]

    def get_table_sample(self, connection_id: str, table_name: str, schema: str, limit: int, offset: int) -> dict:
        sch = schema or "public"
        lim = max(1, min(limit, 50000))
        off = max(0, min(offset, 1000000))
        q = f"SELECT * FROM {q_id(sch)}.{q_id(table_name)} LIMIT {lim} OFFSET {off}"
        return self.execute_read_only(connection_id, q, lim, 10)

    def get_schema_summary(self, connection_id: str, schema: str, include_row_count: bool) -> list:
        conn = self.get_conn(connection_id)
        schema_filter = "AND table_schema = %s" if schema else "AND table_schema NOT IN ('pg_catalog', 'information_schema')"
        params = [schema] if schema else []
        q = f"""
            SELECT table_schema, table_name,
              (SELECT count(*)::int FROM information_schema.columns c
               WHERE c.table_schema = t.table_schema AND c.table_name = t.table_name) AS col_count
            FROM information_schema.tables t
            WHERE table_type = 'BASE TABLE' {schema_filter}
            ORDER BY table_schema, table_name
        """
        with conn.cursor() as cur:
            cur.execute(q, params)
            rows = cur.fetchall()
            out = []
            for r in rows:
                table_schema = r[0]
                table_name = r[1]
                row_count = None
                if include_row_count:
                    try:
                        cur.execute(f"SELECT count(*)::bigint FROM {q_id(table_schema)}.{q_id(table_name)}")
                        row_count = cur.fetchone()[0]
                    except Exception:
                        pass
                out.append({
                    "schema_name": table_schema,
                    "table_name": table_name,
                    "column_count": r[2],
                    "row_count": row_count
                })
            return out

    def export_schema_json(self, connection_id: str, schema: str) -> dict:
        tables_list = self.list_tables(connection_id, schema)
        tables = []
        for s, t in tables_list:
            info = self.describe_table(connection_id, t, s)
            if info:
                tables.append({
                    "schema": info["schema_name"],
                    "table": info["table_name"],
                    "columns": [{"name": c["name"], "data_type": c["data_type"], "nullable": c["nullable"]} for c in info["columns"]]
                })
        return {"tables": tables}

    def explain_query_sql(self, connection_id: str, query: str) -> str:
        conn = self.get_conn(connection_id)
        q = query.strip().rstrip(";")
        with conn.cursor() as cur:
            cur.execute(f"EXPLAIN (FORMAT TEXT) {q}")
            return "\n".join(r[0] for r in cur.fetchall())

    def validate_query_sql(self, connection_id: str, query: str) -> bool:
        conn = self.get_conn(connection_id)
        q = query.strip().rstrip(";")
        try:
            with conn.cursor() as cur:
                cur.execute(f"PREPARE _nautilus_validate AS {q}")
                cur.execute("DEALLOCATE _nautilus_validate")
            return True
        except Exception:
            return False

    def list_indexes(self, connection_id: str, table_name: str, schema: str = None) -> list:
        conn = self.get_conn(connection_id)
        sch = schema or "public"
        q = """
          SELECT i.relname AS index_name, a.attname AS column_name,
                 ix.indisunique AS is_unique
          FROM pg_index ix
          JOIN pg_class t ON t.oid = ix.indrelid
          JOIN pg_class i ON i.oid = ix.indexrelid
          JOIN pg_namespace n ON n.oid = t.relnamespace
          JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(ix.indkey) AND a.attnum > 0 AND NOT a.attisdropped
          WHERE n.nspname = %s AND t.relname = %s
          ORDER BY i.relname, array_position(ix.indkey, a.attnum)
        """
        with conn.cursor() as cur:
            cur.execute(q, [sch, table_name])
            rows = cur.fetchall()
            by_idx = {}
            for r in rows:
                iname = r[0]
                if iname not in by_idx:
                    by_idx[iname] = {
                        "index_name": iname,
                        "columns": [],
                        "is_unique": bool(r[2])
                    }
                by_idx[iname]["columns"].append(r[1])
            return list(by_idx.values())

    def list_views(self, connection_id: str, schema: str = None) -> list:
        conn = self.get_conn(connection_id)
        schema_filter = "AND table_schema = %s" if schema else "AND table_schema NOT IN ('pg_catalog', 'information_schema')"
        params = [schema] if schema else []
        q = f"""
          SELECT table_schema, table_name, view_definition
          FROM information_schema.views
          WHERE 1=1 {schema_filter}
          ORDER BY table_schema, table_name
        """
        with conn.cursor() as cur:
            cur.execute(q, params)
            return [{"schema_name": r[0], "view_name": r[1], "definition": r[2]} for r in cur.fetchall()]

    def get_foreign_keys(self, connection_id: str, table_name: str, schema: str = None) -> list:
        conn = self.get_conn(connection_id)
        sch = schema or "public"
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
          WHERE c.contype = 'f' AND n1.nspname = %s AND t1.relname = %s
        """
        with conn.cursor() as cur:
            cur.execute(q, [sch, table_name])
            return [{
                "constraint_name": r[0],
                "from_schema": r[1],
                "from_table": r[2],
                "from_columns": list(r[3]) if r[3] else [],
                "to_schema": r[4],
                "to_table": r[5],
                "to_columns": list(r[6]) if r[6] else []
            } for r in cur.fetchall()]

    def get_table_relationships(self, connection_id: str, schema: str = None) -> list:
        conn = self.get_conn(connection_id)
        sch = schema or "public"
        q = """
          SELECT c.conname, t1.relname AS from_t, t2.relname AS to_t
          FROM pg_constraint c
          JOIN pg_class t1 ON t1.oid = c.conrelid
          JOIN pg_namespace n1 ON n1.oid = t1.relnamespace
          JOIN pg_class t2 ON t2.oid = c.confrelid
          WHERE c.contype = 'f' AND n1.nspname = %s
        """
        with conn.cursor() as cur:
            cur.execute(q, [sch])
            return [{
                "from_table": r[1],
                "to_table": r[2],
                "constraint_name": r[0]
            } for r in cur.fetchall()]

    def get_row_count(self, connection_id: str, table_name: str, schema: str = None, where_clause: str = None) -> int:
        conn = self.get_conn(connection_id)
        sch = schema or "public"
        where = f" WHERE {where_clause.strip()}" if where_clause and where_clause.strip() else ""
        q = f"SELECT count(*)::bigint FROM {q_id(sch)}.{q_id(table_name)}{where}"
        with conn.cursor() as cur:
            cur.execute(q)
            return cur.fetchone()[0]

    def get_column_stats(self, connection_id: str, table_name: str, schema: str = None, column_names: list = None) -> list:
        conn = self.get_conn(connection_id)
        sch = schema or "public"
        info = self.describe_table(connection_id, table_name, sch)
        if not info:
            return []
        cols = info["columns"]
        if column_names:
            col_set = set(column_names)
            cols = [c for c in cols if c["name"] in col_set]
        cols = cols[:20]
        result = []
        safe_table = f"{q_id(sch)}.{q_id(table_name)}"
        numeric_types = {
            "integer", "bigint", "smallint", "numeric", "real", "double precision", "numeric"
        }
        with conn.cursor() as cur:
            for col in cols:
                safe_col = q_id(col["name"])
                try:
                    col_type = col["data_type"].lower()
                    is_numeric = any(nt in col_type for nt in numeric_types)
                    if is_numeric:
                        cur.execute(f"SELECT count(*)::bigint, count({safe_col})::bigint, min({safe_col}), max({safe_col}), avg({safe_col})::numeric FROM {safe_table}")
                        r = cur.fetchone()
                        result.append({"column_name": col["name"], "stat_type": "count", "value": r[0]})
                        result.append({"column_name": col["name"], "stat_type": "null_count", "value": r[0] - r[1]})
                        if r[2] is not None:
                            result.append({"column_name": col["name"], "stat_type": "min", "value": r[2]})
                            result.append({"column_name": col["name"], "stat_type": "max", "value": r[3]})
                            result.append({"column_name": col["name"], "stat_type": "avg", "value": float(r[4]) if r[4] is not None else None})
                    else:
                        cur.execute(f"SELECT count(*)::bigint, count(DISTINCT {safe_col})::bigint FROM {safe_table}")
                        r = cur.fetchone()
                        result.append({"column_name": col["name"], "stat_type": "count", "value": r[0]})
                        result.append({"column_name": col["name"], "stat_type": "distinct_count", "value": r[1]})
                except Exception:
                    pass
        return result

    def suggest_tables(self, connection_id: str, search_term: str, schema: str = None) -> list:
        conn = self.get_conn(connection_id)
        term = f"%{search_term}%"
        schema_filter = "AND table_schema = %s" if schema else ""
        params = [term, schema] if schema else [term]
        q = f"""
          SELECT DISTINCT table_schema, table_name, column_name
          FROM information_schema.columns
          WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
            AND (table_name ILIKE %s OR column_name ILIKE %s) {schema_filter}
          ORDER BY table_schema, table_name, column_name
        """
        with conn.cursor() as cur:
            cur.execute(q, [term, term] + params[1:])
            return [list(r) for r in cur.fetchall()]


class MysqlAdapter:
    def __init__(self, configs):
        self.configs = configs
        self.conns = {}

    def get_conn(self, connection_id: str):
        conn = self.conns.get(connection_id)
        if conn is not None:
            try:
                conn.ping(reconnect=True)
            except Exception as e:
                logger.warning(f"Erro no ping da conexao MySQL {connection_id}: {e}")
                self.conns.pop(connection_id, None)
                conn = None
        if conn is None:
            cfg = self.configs.get(connection_id)
            if not cfg:
                raise ValueError(f"Conexao nao encontrada: {connection_id}")
            logger.info(f"Criando nova conexao MySQL para {connection_id}")
            host, port, user, password, db = parse_sql_url(cfg["url"])
            conn = pymysql.connect(
                host=host, port=port or 3306, user=user, password=password, database=db,
                autocommit=True, connect_timeout=5
            )
            self.conns[connection_id] = conn
        return conn

    def test_connection(self, connection_id: str) -> bool:
        try:
            conn = self.get_conn(connection_id)
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                cur.fetchone()
            return True
        except Exception:
            return False

    def probe_connection(self, connection_id: str) -> dict:
        t0 = time.perf_counter()
        try:
            conn = self.get_conn(connection_id)
            with conn.cursor() as cur:
                cur.execute("SELECT version()")
                v = cur.fetchone()[0]
            latency = int((time.perf_counter() - t0) * 1000)
            return {"ok": True, "latencyMs": latency, "version": str(v)}
        except Exception as e:
            return {"ok": False, "latencyMs": int((time.perf_counter() - t0) * 1000), "version": None, "error": str(e)}

    def execute_read_only(self, connection_id: str, query: str, max_rows: int, timeout_seconds: int) -> dict:
        q = query.strip().rstrip(";")
        if not re.search(r"\bLIMIT\s+\d+", q, re.IGNORECASE):
            q = f"{q} LIMIT {max_rows}"
        try:
            conn = self.get_conn(connection_id)
            with conn.cursor() as cur:
                cur.execute(q)
                if cur.description is None:
                    return {"columns": [], "rows": [], "row_count": 0}
                columns = [d[0] for d in cur.description]
                rows = cur.fetchall()
                out_rows = [list(row) for row in rows]
                return {"columns": columns, "rows": out_rows, "row_count": len(out_rows)}
        except (pymysql.err.OperationalError, pymysql.err.InterfaceError) as e:
            logger.warning(f"Erro de conexao MySQL durante query para {connection_id}: {e}")
            self.conns.pop(connection_id, None)
            raise

    def list_tables(self, connection_id: str, schema: str = None) -> list:
        conn = self.get_conn(connection_id)
        schema_filter = "AND table_schema = %s" if schema else "AND table_schema = DATABASE()"
        params = [schema] if schema else []
        q = f"""
            SELECT table_schema, table_name
            FROM information_schema.tables
            WHERE 1=1 {schema_filter}
            ORDER BY table_schema, table_name
        """
        with conn.cursor() as cur:
            cur.execute(q, params)
            return [list(row) for row in cur.fetchall()]

    def describe_table(self, connection_id: str, table_name: str, schema: str = None) -> dict:
        conn = self.get_conn(connection_id)
        schema_cond = "AND table_schema = %s" if schema else "AND table_schema = DATABASE()"
        params = [table_name, schema] if schema else [table_name]
        q = f"""
            SELECT table_schema, table_name, column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_name = %s {schema_cond}
            ORDER BY ordinal_position
        """
        with conn.cursor() as cur:
            cur.execute(q, params)
            rows = cur.fetchall()
            if not rows:
                return None
            columns = [{"name": r[2], "data_type": r[3], "nullable": r[4].upper() == "YES"} for r in rows]
            return {"schema_name": rows[0][0], "table_name": rows[0][1], "columns": columns}

    def list_databases(self, connection_id: str) -> list:
        conn = self.get_conn(connection_id)
        with conn.cursor() as cur:
            cur.execute("SHOW DATABASES")
            return [r[0] for r in cur.fetchall()]

    def get_table_sample(self, connection_id: str, table_name: str, schema: str, limit: int, offset: int) -> dict:
        sch = schema or ""
        prefix = f"{q_id(sch)}." if sch else ""
        lim = max(1, min(limit, 50000))
        off = max(0, min(offset, 1000000))
        q = f"SELECT * FROM {prefix}{q_id(table_name)} LIMIT {lim} OFFSET {off}"
        return self.execute_read_only(connection_id, q, lim, 10)

    def get_schema_summary(self, connection_id: str, schema: str, include_row_count: bool) -> list:
        conn = self.get_conn(connection_id)
        schema_filter = "AND table_schema = %s" if schema else "AND table_schema = DATABASE()"
        params = [schema] if schema else []
        q = f"""
            SELECT table_schema, table_name,
              (SELECT count(*) FROM information_schema.columns c
               WHERE c.table_schema = t.table_schema AND c.table_name = t.table_name) AS col_count
            FROM information_schema.tables t
            WHERE table_type = 'BASE TABLE' {schema_filter}
            ORDER BY table_schema, table_name
        """
        with conn.cursor() as cur:
            cur.execute(q, params)
            rows = cur.fetchall()
            out = []
            for r in rows:
                table_schema = r[0]
                table_name = r[1]
                row_count = None
                if include_row_count:
                    try:
                        cur.execute(f"SELECT count(*) FROM {q_id(table_schema)}.{q_id(table_name)}")
                        row_count = cur.fetchone()[0]
                    except Exception:
                        pass
                out.append({
                    "schema_name": table_schema,
                    "table_name": table_name,
                    "column_count": r[2],
                    "row_count": row_count
                })
            return out

    def export_schema_json(self, connection_id: str, schema: str) -> dict:
        tables_list = self.list_tables(connection_id, schema)
        tables = []
        for s, t in tables_list:
            info = self.describe_table(connection_id, t, s)
            if info:
                tables.append({
                    "schema": info["schema_name"],
                    "table": info["table_name"],
                    "columns": [{"name": c["name"], "data_type": c["data_type"], "nullable": c["nullable"]} for c in info["columns"]]
                })
        return {"tables": tables}

    def explain_query_sql(self, connection_id: str, query: str) -> str:
        conn = self.get_conn(connection_id)
        q = query.strip().rstrip(";")
        with conn.cursor() as cur:
            cur.execute(f"EXPLAIN {q}")
            return "\n".join(str(r) for r in cur.fetchall())

    def validate_query_sql(self, connection_id: str, query: str) -> bool:
        conn = self.get_conn(connection_id)
        q = query.strip().rstrip(";")
        try:
            with conn.cursor() as cur:
                cur.execute(f"EXPLAIN {q}")
            return True
        except Exception:
            return False

    def list_indexes(self, connection_id: str, table_name: str, schema: str = None) -> list:
        conn = self.get_conn(connection_id)
        sch = schema or ""
        prefix = f"{q_id(sch)}." if sch else ""
        q = f"SHOW INDEX FROM {prefix}{q_id(table_name)}"
        with conn.cursor() as cur:
            cur.execute(q)
            rows = cur.fetchall()
            by_idx = {}
            for r in rows:
                iname = r[2]
                col_name = r[4]
                non_unique = r[1]
                if iname not in by_idx:
                    by_idx[iname] = {
                        "index_name": iname,
                        "columns": [],
                        "is_unique": non_unique == 0
                    }
                by_idx[iname]["columns"].append(col_name)
            return list(by_idx.values())

    def list_views(self, connection_id: str, schema: str = None) -> list:
        conn = self.get_conn(connection_id)
        schema_filter = "AND table_schema = %s" if schema else "AND table_schema = DATABASE()"
        params = [schema] if schema else []
        q = f"""
          SELECT table_schema, table_name, view_definition
          FROM information_schema.views
          WHERE 1=1 {schema_filter}
          ORDER BY table_schema, table_name
        """
        with conn.cursor() as cur:
            cur.execute(q, params)
            return [{"schema_name": r[0], "view_name": r[1], "definition": r[2]} for r in cur.fetchall()]

    def get_foreign_keys(self, connection_id: str, table_name: str, schema: str = None) -> list:
        conn = self.get_conn(connection_id)
        sch = schema or ""
        q = """
            SELECT 
              constraint_name,
              table_schema,
              table_name,
              column_name,
              referenced_table_schema,
              referenced_table_name,
              referenced_column_name
            FROM information_schema.key_column_usage
            WHERE table_name = %s AND referenced_table_name IS NOT NULL
              AND (table_schema = %s OR (%s IS NULL AND table_schema = DATABASE()))
            ORDER BY constraint_name, ordinal_position
        """
        with conn.cursor() as cur:
            cur.execute(q, [table_name, sch, sch])
            rows = cur.fetchall()
            by_name = {}
            for r in rows:
                cname = r[0]
                if cname not in by_name:
                    by_name[cname] = {
                        "constraint_name": cname,
                        "from_schema": r[1],
                        "from_table": r[2],
                        "from_columns": [],
                        "to_schema": r[4],
                        "to_table": r[5],
                        "to_columns": []
                    }
                by_name[cname]["from_columns"].append(r[3])
                by_name[cname]["to_columns"].append(r[6])
            return list(by_name.values())

    def get_table_relationships(self, connection_id: str, schema: str = None) -> list:
        conn = self.get_conn(connection_id)
        sch = schema or ""
        q = """
            SELECT constraint_name, table_name, referenced_table_name
            FROM information_schema.key_column_usage
            WHERE referenced_table_name IS NOT NULL
              AND (table_schema = %s OR (%s IS NULL AND table_schema = DATABASE()))
            GROUP BY constraint_name, table_name, referenced_table_name
        """
        with conn.cursor() as cur:
            cur.execute(q, [sch, sch])
            return [{
                "from_table": r[1],
                "to_table": r[2],
                "constraint_name": r[0]
            } for r in cur.fetchall()]

    def get_row_count(self, connection_id: str, table_name: str, schema: str = None, where_clause: str = None) -> int:
        conn = self.get_conn(connection_id)
        sch = schema or ""
        prefix = f"{q_id(sch)}." if sch else ""
        where = f" WHERE {where_clause.strip()}" if where_clause and where_clause.strip() else ""
        q = f"SELECT count(*) FROM {prefix}{q_id(table_name)}{where}"
        with conn.cursor() as cur:
            cur.execute(q)
            return cur.fetchone()[0]

    def get_column_stats(self, connection_id: str, table_name: str, schema: str = None, column_names: list = None) -> list:
        conn = self.get_conn(connection_id)
        sch = schema or ""
        info = self.describe_table(connection_id, table_name, sch)
        if not info:
            return []
        cols = info["columns"]
        if column_names:
            col_set = set(column_names)
            cols = [c for c in cols if c["name"] in col_set]
        cols = cols[:20]
        result = []
        prefix = f"{q_id(sch)}." if sch else ""
        safe_table = f"{prefix}{q_id(table_name)}"
        numeric_types = {
            "int", "decimal", "numeric", "float", "double", "real"
        }
        with conn.cursor() as cur:
            for col in cols:
                safe_col = q_id(col["name"])
                try:
                    col_type = col["data_type"].lower()
                    is_numeric = any(nt in col_type for nt in numeric_types)
                    if is_numeric:
                        cur.execute(f"SELECT count(*), count({safe_col}), min({safe_col}), max({safe_col}), avg({safe_col}) FROM {safe_table}")
                        r = cur.fetchone()
                        result.append({"column_name": col["name"], "stat_type": "count", "value": r[0]})
                        result.append({"column_name": col["name"], "stat_type": "null_count", "value": r[0] - r[1]})
                        if r[2] is not None:
                            result.append({"column_name": col["name"], "stat_type": "min", "value": r[2]})
                            result.append({"column_name": col["name"], "stat_type": "max", "value": r[3]})
                            result.append({"column_name": col["name"], "stat_type": "avg", "value": float(r[4]) if r[4] is not None else None})
                    else:
                        cur.execute(f"SELECT count(*), count(DISTINCT {safe_col}) FROM {safe_table}")
                        r = cur.fetchone()
                        result.append({"column_name": col["name"], "stat_type": "count", "value": r[0]})
                        result.append({"column_name": col["name"], "stat_type": "distinct_count", "value": r[1]})
                except Exception:
                    pass
        return result

    def suggest_tables(self, connection_id: str, search_term: str, schema: str = None) -> list:
        conn = self.get_conn(connection_id)
        term = f"%{search_term}%"
        schema_filter = "AND table_schema = %s" if schema else "AND table_schema = DATABASE()"
        params = [term, schema] if schema else [term]
        q = f"""
          SELECT DISTINCT table_schema, table_name, column_name
          FROM information_schema.columns
          WHERE (table_name LIKE %s OR column_name LIKE %s) {schema_filter}
          ORDER BY table_schema, table_name, column_name
        """
        with conn.cursor() as cur:
            cur.execute(q, [term, term] + params[1:])
            return [list(r) for r in cur.fetchall()]


class MssqlAdapter:
    def __init__(self, configs):
        self.configs = configs
        self.conns = {}

    def get_conn(self, connection_id: str):
        conn = self.conns.get(connection_id)
        if conn is not None:
            is_alive = True
            try:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1")
                    cur.fetchone()
            except Exception:
                is_alive = False
            if not is_alive:
                logger.info(f"Conexao MSSQL {connection_id} inativa. Removendo do cache.")
                try:
                    conn.close()
                except Exception:
                    pass
                self.conns.pop(connection_id, None)
                conn = None
        if conn is None:
            cfg = self.configs.get(connection_id)
            if not cfg:
                raise ValueError(f"Conexao nao encontrada: {connection_id}")
            url_val = cfg["url"]
            params = {}
            for p in url_val.split(";"):
                if "=" in p:
                    k, v = p.split("=", 1)
                    k_lower = k.strip().lower()
                    if k_lower == "server":
                        if "," in v:
                            host_part, port_part = v.split(",", 1)
                            params["host"] = host_part.strip()
                            params["port"] = int(port_part.strip())
                        else:
                            params["host"] = v.strip()
                    elif k_lower == "database":
                        params["database"] = v.strip()
                    elif k_lower == "user id" or k_lower == "uid":
                        params["user"] = v.strip()
                    elif k_lower == "password" or k_lower == "pwd":
                        params["password"] = v.strip()
            logger.info(f"Criando nova conexao MSSQL para {connection_id}")
            conn = pymssql.connect(
                server=params.get("host"), port=params.get("port", 1433),
                user=params.get("user"), password=params.get("password"),
                database=params.get("database"), autocommit=True,
                login_timeout=5
            )
            self.conns[connection_id] = conn
        return conn

    def test_connection(self, connection_id: str) -> bool:
        try:
            conn = self.get_conn(connection_id)
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                cur.fetchone()
            return True
        except Exception:
            return False

    def probe_connection(self, connection_id: str) -> dict:
        t0 = time.perf_counter()
        try:
            conn = self.get_conn(connection_id)
            with conn.cursor() as cur:
                cur.execute("SELECT @@VERSION")
                v = cur.fetchone()[0]
            latency = int((time.perf_counter() - t0) * 1000)
            return {"ok": True, "latencyMs": latency, "version": str(v).split("\n")[0]}
        except Exception as e:
            return {"ok": False, "latencyMs": int((time.perf_counter() - t0) * 1000), "version": None, "error": str(e)}

    def execute_read_only(self, connection_id: str, query: str, max_rows: int, timeout_seconds: int) -> dict:
        q = query.strip().rstrip(";")
        if not re.search(r"\bTOP\s+\d+", q, re.IGNORECASE) and not re.search(r"\bOFFSET\s+\d+", q, re.IGNORECASE):
            if q.lower().startswith("select"):
                q = f"SELECT TOP {max_rows} " + q[6:]
        try:
            conn = self.get_conn(connection_id)
            with conn.cursor() as cur:
                cur.execute(q)
                if cur.description is None:
                    return {"columns": [], "rows": [], "row_count": 0}
                columns = [d[0] for d in cur.description]
                rows = cur.fetchall()
                out_rows = [list(row) for row in rows]
                return {"columns": columns, "rows": out_rows, "row_count": len(out_rows)}
        except (pymssql.OperationalError, pymssql.InterfaceError) as e:
            logger.warning(f"Erro de conexao MSSQL durante query para {connection_id}: {e}")
            self.conns.pop(connection_id, None)
            raise

    def list_tables(self, connection_id: str, schema: str = None) -> list:
        conn = self.get_conn(connection_id)
        schema_filter = "AND TABLE_SCHEMA = %s" if schema else ""
        params = [schema] if schema else []
        q = f"""
            SELECT TABLE_SCHEMA, TABLE_NAME
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_TYPE = 'BASE TABLE' {schema_filter}
            ORDER BY TABLE_SCHEMA, TABLE_NAME
        """
        with conn.cursor() as cur:
            cur.execute(q, params)
            return [list(row) for row in cur.fetchall()]

    def describe_table(self, connection_id: str, table_name: str, schema: str = None) -> dict:
        conn = self.get_conn(connection_id)
        schema_cond = "AND TABLE_SCHEMA = %s" if schema else ""
        params = [table_name, schema] if schema else [table_name]
        q = f"""
            SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, DATA_TYPE, IS_NULLABLE
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_NAME = %s {schema_cond}
            ORDER BY ORDINAL_POSITION
        """
        with conn.cursor() as cur:
            cur.execute(q, params)
            rows = cur.fetchall()
            if not rows:
                return None
            columns = [{"name": r[2], "data_type": r[3], "nullable": r[4].upper() == "YES"} for r in rows]
            return {"schema_name": rows[0][0], "table_name": rows[0][1], "columns": columns}

    def list_databases(self, connection_id: str) -> list:
        conn = self.get_conn(connection_id)
        with conn.cursor() as cur:
            cur.execute("SELECT name FROM sys.databases WHERE name NOT IN ('master', 'tempdb', 'model', 'msdb') ORDER BY name")
            return [r[0] for r in cur.fetchall()]

    def get_table_sample(self, connection_id: str, table_name: str, schema: str, limit: int, offset: int) -> dict:
        sch = schema or "dbo"
        lim = max(1, min(limit, 50000))
        q = f"SELECT TOP {lim} * FROM {q_id(sch)}.{q_id(table_name)}"
        return self.execute_read_only(connection_id, q, lim, 10)

    def get_schema_summary(self, connection_id: str, schema: str, include_row_count: bool) -> list:
        conn = self.get_conn(connection_id)
        schema_filter = "AND TABLE_SCHEMA = %s" if schema else ""
        params = [schema] if schema else []
        q = f"""
            SELECT TABLE_SCHEMA, TABLE_NAME,
              (SELECT count(*) FROM INFORMATION_SCHEMA.COLUMNS c
               WHERE c.TABLE_SCHEMA = t.TABLE_SCHEMA AND c.TABLE_NAME = t.TABLE_NAME) AS col_count
            FROM INFORMATION_SCHEMA.TABLES t
            WHERE TABLE_TYPE = 'BASE TABLE' {schema_filter}
            ORDER BY TABLE_SCHEMA, TABLE_NAME
        """
        with conn.cursor() as cur:
            cur.execute(q, params)
            rows = cur.fetchall()
            out = []
            for r in rows:
                table_schema = r[0]
                table_name = r[1]
                row_count = None
                if include_row_count:
                    try:
                        cur.execute(f"SELECT count(*) FROM {q_id(table_schema)}.{q_id(table_name)}")
                        row_count = cur.fetchone()[0]
                    except Exception:
                        pass
                out.append({
                    "schema_name": table_schema,
                    "table_name": table_name,
                    "column_count": r[2],
                    "row_count": row_count
                })
            return out

    def export_schema_json(self, connection_id: str, schema: str) -> dict:
        tables_list = self.list_tables(connection_id, schema)
        tables = []
        for s, t in tables_list:
            info = self.describe_table(connection_id, t, s)
            if info:
                tables.append({
                    "schema": info["schema_name"],
                    "table": info["table_name"],
                    "columns": [{"name": c["name"], "data_type": c["data_type"], "nullable": c["nullable"]} for c in info["columns"]]
                })
        return {"tables": tables}

    def explain_query_sql(self, connection_id: str, query: str) -> str:
        return "EXPLAIN nao suportado nativamente para MSSQL neste driver."

    def validate_query_sql(self, connection_id: str, query: str) -> bool:
        conn = self.get_conn(connection_id)
        try:
            with conn.cursor() as cur:
                cur.execute("SET NOEXEC ON")
                cur.execute(query)
                cur.execute("SET NOEXEC OFF")
            return True
        except Exception:
            return False

    def list_indexes(self, connection_id: str, table_name: str, schema: str = None) -> list:
        conn = self.get_conn(connection_id)
        sch = schema or "dbo"
        q = """
            SELECT 
                i.name AS IndexName,
                c.name AS ColumnName,
                i.is_unique AS IsUnique
            FROM sys.indexes i
            INNER JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
            INNER JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
            INNER JOIN sys.tables t ON i.object_id = t.object_id
            INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
            WHERE t.name = %s AND s.name = %s
            ORDER BY i.name, ic.key_ordinal
        """
        with conn.cursor() as cur:
            cur.execute(q, [table_name, sch])
            rows = cur.fetchall()
            by_idx = {}
            for r in rows:
                iname = r[0]
                if not iname:
                    continue
                if iname not in by_idx:
                    by_idx[iname] = {
                        "index_name": iname,
                        "columns": [],
                        "is_unique": bool(r[2])
                    }
                by_idx[iname]["columns"].append(r[1])
            return list(by_idx.values())

    def list_views(self, connection_id: str, schema: str = None) -> list:
        conn = self.get_conn(connection_id)
        schema_filter = "AND TABLE_SCHEMA = %s" if schema else ""
        params = [schema] if schema else []
        q = f"""
          SELECT TABLE_SCHEMA, TABLE_NAME, VIEW_DEFINITION
          FROM INFORMATION_SCHEMA.VIEWS
          WHERE 1=1 {schema_filter}
          ORDER BY TABLE_SCHEMA, TABLE_NAME
        """
        with conn.cursor() as cur:
            cur.execute(q, params)
            return [{"schema_name": r[0], "view_name": r[1], "definition": r[2]} for r in cur.fetchall()]

    def get_foreign_keys(self, connection_id: str, table_name: str, schema: str = None) -> list:
        conn = self.get_conn(connection_id)
        sch = schema or "dbo"
        q = """
            SELECT 
                obj.name AS FK_NAME,
                sch.name AS [SCHEMA],
                tab1.name AS [TABLE],
                col1.name AS [COLUMN],
                sch2.name AS [REF_SCHEMA],
                tab2.name AS [REF_TABLE],
                col2.name AS [REF_COLUMN]
            FROM sys.foreign_key_columns fkc
            INNER JOIN sys.objects obj ON obj.object_id = fkc.constraint_object_id
            INNER JOIN sys.tables tab1 ON tab1.object_id = fkc.parent_object_id
            INNER JOIN sys.schemas sch ON tab1.schema_id = sch.schema_id
            INNER JOIN sys.columns col1 ON col1.column_id = parent_column_id AND col1.object_id = tab1.object_id
            INNER JOIN sys.tables tab2 ON tab2.object_id = fkc.referenced_object_id
            INNER JOIN sys.schemas sch2 ON tab2.schema_id = sch2.schema_id
            INNER JOIN sys.columns col2 ON col2.column_id = referenced_column_id AND col2.object_id = tab2.object_id
            WHERE tab1.name = %s AND sch.name = %s
        """
        with conn.cursor() as cur:
            cur.execute(q, [table_name, sch])
            rows = cur.fetchall()
            by_name = {}
            for r in rows:
                cname = r[0]
                if cname not in by_name:
                    by_name[cname] = {
                        "constraint_name": cname,
                        "from_schema": r[1],
                        "from_table": r[2],
                        "from_columns": [],
                        "to_schema": r[4],
                        "to_table": r[5],
                        "to_columns": []
                    }
                by_name[cname]["from_columns"].append(r[3])
                by_name[cname]["to_columns"].append(r[6])
            return list(by_name.values())

    def get_table_relationships(self, connection_id: str, schema: str = None) -> list:
        conn = self.get_conn(connection_id)
        sch = schema or "dbo"
        q = """
            SELECT 
                obj.name AS FK_NAME,
                tab1.name AS [TABLE],
                tab2.name AS [REF_TABLE]
            FROM sys.foreign_key_columns fkc
            INNER JOIN sys.objects obj ON obj.object_id = fkc.constraint_object_id
            INNER JOIN sys.tables tab1 ON tab1.object_id = fkc.parent_object_id
            INNER JOIN sys.schemas sch ON tab1.schema_id = sch.schema_id
            INNER JOIN sys.tables tab2 ON tab2.object_id = fkc.referenced_object_id
            WHERE sch.name = %s
            GROUP BY obj.name, tab1.name, tab2.name
        """
        with conn.cursor() as cur:
            cur.execute(q, [sch])
            return [{
                "from_table": r[1],
                "to_table": r[2],
                "constraint_name": r[0]
            } for r in cur.fetchall()]

    def get_row_count(self, connection_id: str, table_name: str, schema: str = None, where_clause: str = None) -> int:
        conn = self.get_conn(connection_id)
        sch = schema or "dbo"
        where = f" WHERE {where_clause.strip()}" if where_clause and where_clause.strip() else ""
        q = f"SELECT count(*) FROM {q_id(sch)}.{q_id(table_name)}{where}"
        with conn.cursor() as cur:
            cur.execute(q)
            return cur.fetchone()[0]

    def get_column_stats(self, connection_id: str, table_name: str, schema: str = None, column_names: list = None) -> list:
        conn = self.get_conn(connection_id)
        sch = schema or "dbo"
        info = self.describe_table(connection_id, table_name, sch)
        if not info:
            return []
        cols = info["columns"]
        if column_names:
            col_set = set(column_names)
            cols = [c for c in cols if c["name"] in col_set]
        cols = cols[:20]
        result = []
        safe_table = f"{q_id(sch)}.{q_id(table_name)}"
        numeric_types = {
            "int", "decimal", "numeric", "float", "double", "real"
        }
        with conn.cursor() as cur:
            for col in cols:
                safe_col = q_id(col["name"])
                try:
                    col_type = col["data_type"].lower()
                    is_numeric = any(nt in col_type for nt in numeric_types)
                    if is_numeric:
                        cur.execute(f"SELECT count(*), count({safe_col}), min({safe_col}), max({safe_col}), avg({safe_col}) FROM {safe_table}")
                        r = cur.fetchone()
                        result.append({"column_name": col["name"], "stat_type": "count", "value": r[0]})
                        result.append({"column_name": col["name"], "stat_type": "null_count", "value": r[0] - r[1]})
                        if r[2] is not None:
                            result.append({"column_name": col["name"], "stat_type": "min", "value": r[2]})
                            result.append({"column_name": col["name"], "stat_type": "max", "value": r[3]})
                            result.append({"column_name": col["name"], "stat_type": "avg", "value": float(r[4]) if r[4] is not None else None})
                    else:
                        cur.execute(f"SELECT count(*), count(DISTINCT {safe_col}) FROM {safe_table}")
                        r = cur.fetchone()
                        result.append({"column_name": col["name"], "stat_type": "count", "value": r[0]})
                        result.append({"column_name": col["name"], "stat_type": "distinct_count", "value": r[1]})
                except Exception:
                    pass
        return result

    def suggest_tables(self, connection_id: str, search_term: str, schema: str = None) -> list:
        conn = self.get_conn(connection_id)
        term = f"%{search_term}%"
        schema_filter = "AND TABLE_SCHEMA = %s" if schema else ""
        params = [term, schema] if schema else [term]
        q = f"""
          SELECT DISTINCT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME
          FROM INFORMATION_SCHEMA.COLUMNS
          WHERE (TABLE_NAME LIKE %s OR COLUMN_NAME LIKE %s) {schema_filter}
          ORDER BY TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME
        """
        with conn.cursor() as cur:
            cur.execute(q, [term, term] + params[1:])
            return [list(r) for r in cur.fetchall()]


class OracleAdapter:
    def __init__(self, configs):
        self.configs = configs
        self.conns = {}

    def get_conn(self, connection_id: str):
        conn = self.conns.get(connection_id)
        if conn is not None:
            is_alive = True
            try:
                conn.ping()
            except Exception:
                is_alive = False
            if not is_alive:
                logger.info(f"Conexao Oracle {connection_id} inativa. Removendo do cache.")
                try:
                    conn.close()
                except Exception:
                    pass
                self.conns.pop(connection_id, None)
                conn = None
        if conn is None:
            cfg = self.configs.get(connection_id)
            if not cfg:
                raise ValueError(f"Conexao nao encontrada: {connection_id}")
            url_val = cfg["url"]
            parsed = urlparse(url_val)
            user = unquote(parsed.username) if parsed.username else None
            password = unquote(parsed.password) if parsed.password else None
            dsn = f"{parsed.hostname}:{parsed.port or 1521}/{parsed.path.lstrip('/')}"
            logger.info(f"Criando nova conexao Oracle para {connection_id}")
            conn = oracledb.connect(user=user, password=password, dsn=dsn)
            self.conns[connection_id] = conn
        return conn

    def test_connection(self, connection_id: str) -> bool:
        try:
            conn = self.get_conn(connection_id)
            with conn.cursor() as cur:
                cur.execute("SELECT 1 FROM DUAL")
                cur.fetchone()
            return True
        except Exception:
            return False

    def probe_connection(self, connection_id: str) -> dict:
        t0 = time.perf_counter()
        try:
            conn = self.get_conn(connection_id)
            with conn.cursor() as cur:
                cur.execute("SELECT version FROM product_component_version WHERE rownum = 1")
                v = cur.fetchone()[0]
            latency = int((time.perf_counter() - t0) * 1000)
            return {"ok": True, "latencyMs": latency, "version": str(v)}
        except Exception as e:
            return {"ok": False, "latencyMs": int((time.perf_counter() - t0) * 1000), "version": None, "error": str(e)}

    def execute_read_only(self, connection_id: str, query: str, max_rows: int, timeout_seconds: int) -> dict:
        q = query.strip().rstrip(";")
        if not re.search(r"\bFETCH\s+FIRST\s+\d+", q, re.IGNORECASE) and not re.search(r"\bROWNUM\b", q, re.IGNORECASE):
            q = f"SELECT * FROM ({q}) WHERE ROWNUM <= {max_rows}"
        try:
            conn = self.get_conn(connection_id)
            with conn.cursor() as cur:
                cur.execute(q)
                if cur.description is None:
                    return {"columns": [], "rows": [], "row_count": 0}
                columns = [d[0] for d in cur.description]
                rows = cur.fetchall()
                out_rows = [list(row) for row in rows]
                return {"columns": columns, "rows": out_rows, "row_count": len(out_rows)}
        except (oracledb.DatabaseError, oracledb.InterfaceError, oracledb.OperationalError) as e:
            logger.warning(f"Erro de conexao Oracle durante query para {connection_id}: {e}")
            self.conns.pop(connection_id, None)
            raise

    def list_tables(self, connection_id: str, schema: str = None) -> list:
        conn = self.get_conn(connection_id)
        schema_filter = "AND owner = :sch" if schema else "AND owner = USER"
        params = {"sch": schema.upper()} if schema else {}
        q = f"""
            SELECT owner, table_name
            FROM all_tables
            WHERE 1=1 {schema_filter}
            ORDER BY owner, table_name
        """
        with conn.cursor() as cur:
            cur.execute(q, params)
            return [list(row) for row in cur.fetchall()]

    def describe_table(self, connection_id: str, table_name: str, schema: str = None) -> dict:
        conn = self.get_conn(connection_id)
        schema_cond = "AND owner = :sch" if schema else "AND owner = USER"
        params = {"tbl": table_name.upper()}
        if schema:
            params["sch"] = schema.upper()
        q = f"""
            SELECT owner, table_name, column_name, data_type, nullable
            FROM all_tab_columns
            WHERE table_name = :tbl {schema_cond}
            ORDER BY column_id
        """
        with conn.cursor() as cur:
            cur.execute(q, params)
            rows = cur.fetchall()
            if not rows:
                return None
            columns = [{"name": r[2], "data_type": r[3], "nullable": r[4] == "Y"} for r in rows]
            return {"schema_name": rows[0][0], "table_name": rows[0][1], "columns": columns}

    def list_databases(self, connection_id: str) -> list:
        conn = self.get_conn(connection_id)
        with conn.cursor() as cur:
            cur.execute("SELECT name FROM v$database")
            return [r[0] for r in cur.fetchall()]

    def get_table_sample(self, connection_id: str, table_name: str, schema: str, limit: int, offset: int) -> dict:
        sch = schema or "USER"
        lim = max(1, min(limit, 50000))
        q = f"SELECT * FROM {q_id(sch)}.{q_id(table_name)} WHERE ROWNUM <= {lim}"
        return self.execute_read_only(connection_id, q, lim, 10)

    def get_schema_summary(self, connection_id: str, schema: str, include_row_count: bool) -> list:
        conn = self.get_conn(connection_id)
        schema_filter = "AND owner = :sch" if schema else "AND owner = USER"
        params = {"sch": schema.upper()} if schema else {}
        q = f"""
            SELECT owner, table_name,
              (SELECT count(*) FROM all_tab_columns c
               WHERE c.owner = t.owner AND c.table_name = t.table_name) AS col_count
            FROM all_tables t
            WHERE 1=1 {schema_filter}
            ORDER BY owner, table_name
        """
        with conn.cursor() as cur:
            cur.execute(q, params)
            rows = cur.fetchall()
            out = []
            for r in rows:
                table_schema = r[0]
                table_name = r[1]
                row_count = None
                if include_row_count:
                    try:
                        cur.execute(f"SELECT num_rows FROM all_tables WHERE owner = :o AND table_name = :t", {"o": table_schema, "t": table_name})
                        row_count = cur.fetchone()[0]
                    except Exception:
                        pass
                out.append({
                    "schema_name": table_schema,
                    "table_name": table_name,
                    "column_count": r[2],
                    "row_count": row_count
                })
            return out

    def export_schema_json(self, connection_id: str, schema: str) -> dict:
        tables_list = self.list_tables(connection_id, schema)
        tables = []
        for s, t in tables_list:
            info = self.describe_table(connection_id, t, s)
            if info:
                tables.append({
                    "schema": info["schema_name"],
                    "table": info["table_name"],
                    "columns": [{"name": c["name"], "data_type": c["data_type"], "nullable": c["nullable"]} for c in info["columns"]]
                })
        return {"tables": tables}

    def explain_query_sql(self, connection_id: str, query: str) -> str:
        return "EXPLAIN nao implementado via driver Python nesta versao."

    def validate_query_sql(self, connection_id: str, query: str) -> bool:
        return True


class MongodbAdapter:
    def __init__(self, configs, timeout_ms):
        self.configs = configs
        self.timeout_ms = timeout_ms
        self.clients = {}

    def get_client(self, connection_id: str):
        client = self.clients.get(connection_id)
        if client is not None:
            try:
                client.admin.command("ping")
            except Exception:
                logger.info(f"Conexao MongoDB {connection_id} inativa. Removendo do cache.")
                try:
                    client.close()
                except Exception:
                    pass
                self.clients.pop(connection_id, None)
                client = None
        if client is None:
            cfg = self.configs.get(connection_id)
            if not cfg:
                raise ValueError(f"Conexao nao encontrada: {connection_id}")
            logger.info(f"Criando nova conexao MongoDB para {connection_id}")
            client = pymongo.MongoClient(
                cfg["url"], serverSelectionTimeoutMS=self.timeout_ms, connectTimeoutMS=self.timeout_ms
            )
            self.clients[connection_id] = client
        return client

    def test_connection(self, connection_id: str) -> bool:
        try:
            client = self.get_client(connection_id)
            client.admin.command("ping")
            return True
        except Exception:
            return False

    def probe_connection(self, connection_id: str) -> dict:
        t0 = time.perf_counter()
        try:
            client = self.get_client(connection_id)
            bi = client.admin.command("buildInfo")
            latency = int((time.perf_counter() - t0) * 1000)
            return {"ok": True, "latencyMs": latency, "version": str(bi.get("version"))}
        except Exception as e:
            return {"ok": False, "latencyMs": int((time.perf_counter() - t0) * 1000), "version": None, "error": str(e)}

    def list_collections(self, connection_id: str, database: str) -> list:
        client = self.get_client(connection_id)
        db = client[database]
        return sorted(db.list_collection_names())

    def find_documents(self, connection_id: str, database: str, collection: str, filter_json: str, limit: int, skip: int) -> list:
        import json
        client = self.get_client(connection_id)
        filt = {}
        if filter_json and filter_json.strip():
            try:
                filt = json.loads(filter_json)
            except Exception:
                pass
        db = client[database]
        coll = db[collection]
        cap = max(1, limit)
        sk = max(0, min(skip, 50000))
        cursor = coll.find(filt).skip(sk).limit(cap)
        docs = list(cursor)
        return docs

    def aggregate(self, connection_id: str, database: str, collection: str, pipeline_json: str, limit: int, skip: int) -> list:
        import json
        client = self.get_client(connection_id)
        try:
            pipeline = json.loads(pipeline_json) if pipeline_json.strip() else []
        except Exception:
            raise ValueError("Pipeline invalido: JSON malformado")
        if not isinstance(pipeline, list):
            raise ValueError("Pipeline deve ser uma lista de stages")
            
        forbidden = {"$out", "$merge", "$currentOp", "$listSessions", "$collStats", "$indexStats", "$planCacheStats"}
        allowed = {"$match", "$project", "$group", "$sort", "$limit", "$skip", "$unwind", "$lookup", "$count", "$addFields"}
        
        for i, stage in enumerate(pipeline):
            if not isinstance(stage, dict) or len(stage) != 1:
                raise ValueError(f"Stage {i} invalido")
            op = list(stage.keys())[0]
            if op in forbidden:
                raise ValueError(f"Stage '{op}' nao e permitido (somente leitura)")
            if op not in allowed and not op.startswith("$"):
                raise ValueError(f"Stage '{op}' desconhecido")
                
        copy_pipeline = [dict(s) for s in pipeline]
        sk = max(0, min(skip, 50000))
        if sk > 0:
            copy_pipeline.insert(0, {"$skip": sk})
            
        lim = max(1, limit)
        if not copy_pipeline or list(copy_pipeline[-1].keys())[0] != "$limit":
            copy_pipeline.append({"$limit": lim})
        else:
            last_stage = copy_pipeline[-1]
            last_stage["$limit"] = min(last_stage.get("$limit", lim), lim)
            
        db = client[database]
        coll = db[collection]
        return list(coll.aggregate(copy_pipeline))

    def list_collection_indexes(self, connection_id: str, database: str, collection: str) -> list:
        client = self.get_client(connection_id)
        db = client[database]
        coll = db[collection]
        return list(coll.list_indexes())


class RedisAdapter:
    def __init__(self, configs, timeout_ms):
        self.configs = configs
        self.timeout_ms = timeout_ms
        self.clients = {}

    def get_client(self, connection_id: str):
        client = self.clients.get(connection_id)
        if client is not None:
            try:
                client.ping()
            except Exception:
                logger.info(f"Conexao Redis {connection_id} inativa. Removendo do cache.")
                self.clients.pop(connection_id, None)
                client = None
        if client is None:
            cfg = self.configs.get(connection_id)
            if not cfg:
                raise ValueError(f"Conexao nao encontrada: {connection_id}")
            logger.info(f"Criando nova conexao Redis para {connection_id}")
            client = redis.from_url(cfg["url"], socket_connect_timeout=self.timeout_ms / 1000.0)
            self.clients[connection_id] = client
        return client

    def test_connection(self, connection_id: str) -> bool:
        try:
            client = self.get_client(connection_id)
            return client.ping()
        except Exception:
            return False

    def probe_connection(self, connection_id: str) -> dict:
        t0 = time.perf_counter()
        try:
            client = self.get_client(connection_id)
            client.ping()
            info = client.info("server")
            version = info.get("redis_version")
            latency = int((time.perf_counter() - t0) * 1000)
            return {"ok": True, "latencyMs": latency, "version": str(version)}
        except Exception as e:
            return {"ok": False, "latencyMs": int((time.perf_counter() - t0) * 1000), "version": None, "error": str(e)}

    def get_key(self, connection_id: str, key: str) -> str:
        client = self.get_client(connection_id)
        val = client.get(key)
        return val.decode("utf-8") if isinstance(val, bytes) else val

    def scan_keys(self, connection_id: str, pattern: str, max_keys: int, start_cursor: str = None) -> dict:
        client = self.get_client(connection_id)
        cap = max(1, min(max_keys, 2000))
        pat = pattern.strip() if pattern else "*"
        cursor = int(start_cursor) if start_cursor and start_cursor != "0" else 0
        out = []
        truncated = False
        
        while True:
            cursor, keys = client.scan(cursor=cursor, match=pat, count=min(100, cap - len(out) + 20))
            for k in keys:
                out.append(k.decode("utf-8") if isinstance(k, bytes) else k)
                if len(out) >= cap:
                    truncated = cursor != 0
                    break
            if cursor == 0 or len(out) >= cap:
                break
                
        return {
            "keys": out,
            "nextCursor": str(cursor) if truncated else None,
            "truncated": truncated
        }

    def key_type(self, connection_id: str, key: str) -> str:
        client = self.get_client(connection_id)
        t = client.type(key)
        return t.decode("utf-8") if isinstance(t, bytes) else t

    def key_ttl(self, connection_id: str, key: str) -> int:
        client = self.get_client(connection_id)
        return client.ttl(key)

    def mget(self, connection_id: str, keys_list: list) -> list:
        client = self.get_client(connection_id)
        slice_keys = keys_list[:50]
        if not slice_keys:
            return []
        vals = client.mget(slice_keys)
        return [v.decode("utf-8") if isinstance(v, bytes) else v for v in vals]

    def read_structured_sample(self, connection_id: str, key: str, max_elements: int) -> dict:
        client = self.get_client(connection_id)
        raw_t = client.type(key)
        t = raw_t.decode("utf-8") if isinstance(raw_t, bytes) else raw_t
        cap = max(1, min(max_elements, 200))
        
        if t == "string":
            v = client.get(key)
            val = v.decode("utf-8") if isinstance(v, bytes) else v
            return {"redis_type": "string", "value": val}
            
        if t == "hash":
            h = client.hgetall(key)
            decoded_h = {}
            for k, v in h.items():
                dk = k.decode("utf-8") if isinstance(k, bytes) else k
                dv = v.decode("utf-8") if isinstance(v, bytes) else v
                decoded_h[dk] = dv
            entries = list(decoded_h.items())[:cap]
            return {
                "redis_type": "hash",
                "fields": dict(entries),
                "truncated": len(decoded_h) > cap,
                "field_count": len(decoded_h)
            }
            
        if t == "list":
            length = client.llen(key)
            slice_list = client.lrange(key, 0, cap - 1)
            decoded_list = [v.decode("utf-8") if isinstance(v, bytes) else v for v in slice_list]
            return {"redis_type": "list", "length": length, "elements": decoded_list, "truncated": length > cap}
            
        if t == "set":
            members = client.smembers(key)
            decoded_members = [v.decode("utf-8") if isinstance(v, bytes) else v for v in members]
            return {
                "redis_type": "set",
                "cardinality": len(decoded_members),
                "sample": decoded_members[:cap],
                "truncated": len(decoded_members) > cap
            }
            
        if t == "zset":
            length = client.zcard(key)
            slice_zset = client.zrange(key, 0, cap - 1)
            decoded_zset = [v.decode("utf-8") if isinstance(v, bytes) else v for v in slice_zset]
            return {"redis_type": "zset", "cardinality": length, "members": decoded_zset, "truncated": length > cap}
            
        return {"redis_type": t}

import pymysql
import re
import time
import logging

from nautilus.core.sql_utils import q_id, parse_sql_url
from nautilus.adapters.base import query_timeout_ms, query_timeout_s, connect_timeout_s

logger = logging.getLogger("nautilus")


class MysqlAdapter:
    def __init__(self, configs):
        self.configs = configs
        self.conns = {}

    def close_all(self):
        """Fecha todas as conexões abertas — usado ao recarregar a config."""
        for conn in self.conns.values():
            try:
                conn.close()
            except Exception:
                pass
        self.conns.clear()

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
            read_tmo = query_timeout_s(self.configs, connection_id)
            conn = pymysql.connect(
                host=host, port=port or 3306, user=user, password=password, database=db,
                autocommit=True,
                connect_timeout=connect_timeout_s(self.configs, connection_id),
                read_timeout=read_tmo, write_timeout=read_tmo
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

    def execute_read_only(self, connection_id: str, query: str, max_rows: int, timeout_ms: int = None) -> dict:
        q = query.strip().rstrip(";")
        if not re.search(r"\bLIMIT\s+\d+", q, re.IGNORECASE):
            q = f"{q} LIMIT {max_rows}"
        tmo_ms = timeout_ms or query_timeout_ms(self.configs, connection_id)
        try:
            conn = self.get_conn(connection_id)
            with conn.cursor() as cur:
                try:
                    # MySQL 5.7.8+; MariaDB não tem — o read_timeout do socket cobre ambos.
                    cur.execute(f"SET SESSION MAX_EXECUTION_TIME={int(tmo_ms)}")
                except Exception:
                    pass
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
        return self.execute_read_only(connection_id, q, lim)

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

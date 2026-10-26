import sqlite3
import re
import time
import logging

from nautilus.core.sql_utils import q_id
from nautilus.adapters.base import query_timeout_ms

logger = logging.getLogger("nautilus")


class SqliteAdapter:
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

    def execute_read_only(self, connection_id: str, query: str, max_rows: int, timeout_ms: int = None) -> dict:
        conn = self.get_conn(connection_id)
        q = query.strip().rstrip(";")
        if not re.search(r"\bLIMIT\s+\d+", q, re.IGNORECASE):
            q = f"{q} LIMIT {max_rows}"
        tmo_ms = timeout_ms or query_timeout_ms(self.configs, connection_id)
        deadline = time.monotonic() + (tmo_ms / 1000.0)

        def _guard():
            # Retornar != 0 aborta a execução; o SQLite levanta OperationalError.
            return 1 if time.monotonic() > deadline else 0

        conn.set_progress_handler(_guard, 10000)
        try:
            cursor = conn.cursor()
            cursor.execute(q)
            rows = cursor.fetchall()
        finally:
            conn.set_progress_handler(None, 10000)
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
        return self.execute_read_only(connection_id, q, lim)

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

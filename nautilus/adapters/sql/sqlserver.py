import pymssql
import re
import time
import logging

from nautilus.core.sql_utils import q_id

logger = logging.getLogger("nautilus")


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

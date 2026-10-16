import oracledb
import re
import time
import logging

from urllib.parse import urlparse, unquote

from nautilus.core.sql_utils import q_id
from nautilus.adapters.base import query_timeout_ms, connect_timeout_s

logger = logging.getLogger("nautilus")


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
            conn = oracledb.connect(
                user=user, password=password, dsn=dsn,
                tcp_connect_timeout=connect_timeout_s(self.configs, connection_id)
            )
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

    def execute_read_only(self, connection_id: str, query: str, max_rows: int, timeout_ms: int = None) -> dict:
        q = query.strip().rstrip(";")
        if not re.search(r"\bFETCH\s+FIRST\s+\d+", q, re.IGNORECASE) and not re.search(r"\bROWNUM\b", q, re.IGNORECASE):
            q = f"SELECT * FROM ({q}) WHERE ROWNUM <= {max_rows}"
        tmo_ms = timeout_ms or query_timeout_ms(self.configs, connection_id)
        try:
            conn = self.get_conn(connection_id)
            conn.call_timeout = int(tmo_ms)  # oracledb: timeout por round-trip, em ms
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
        return self.execute_read_only(connection_id, q, lim)

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

import sqlite3

import pytest

from nautilus.adapters import base
from nautilus.adapters.sql.sqlite import SqliteAdapter
from nautilus.adapters.sql.postgres import PostgresAdapter
from nautilus.adapters.sql.mysql import MysqlAdapter
from nautilus.adapters.sql.oracle import OracleAdapter


class FakeCursor:
    def __init__(self, recorder):
        self.recorder = recorder
        self.description = None

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def execute(self, sql, params=None):
        self.recorder.append(sql)

    def fetchall(self):
        return []


class FakeConn:
    def __init__(self):
        self.statements = []
        self.call_timeout = None
        self.closed = False
        self.broken = False

    def cursor(self):
        return FakeCursor(self.statements)


# --- helpers de base ---

def test_base_query_timeout_ms_fallback():
    assert base.query_timeout_ms({}, "x") == base.DEFAULT_QUERY_TIMEOUT_MS


def test_base_connect_timeout_seconds_rounding():
    cfg = {"c": {"connect_timeout_ms": 7000}}
    assert base.connect_timeout_s(cfg, "c") == 7


def test_base_query_timeout_seconds():
    cfg = {"c": {"query_timeout_ms": 45000}}
    assert base.query_timeout_s(cfg, "c") == 45


# --- SQLite: timeout REAL aborta uma query longa ---

def test_sqlite_timeout_aborts_long_query():
    a = SqliteAdapter({"lite": {"url": ":memory:", "query_timeout_ms": 50}})
    long_q = (
        "WITH RECURSIVE r(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM r WHERE i < 100000000) "
        "SELECT count(*) FROM r"
    )
    with pytest.raises(sqlite3.OperationalError):
        a.execute_read_only("lite", long_q, 10)


def test_sqlite_fast_query_ok():
    a = SqliteAdapter({"lite": {"url": ":memory:", "query_timeout_ms": 30000}})
    res = a.execute_read_only("lite", "SELECT 1 AS n", 10)
    assert res["rows"] == [[1]]


def test_sqlite_get_table_sample_no_spurious_timeout():
    # Regressão: get_table_sample não deve passar 10 (ms) como timeout.
    a = SqliteAdapter({"lite": {"url": ":memory:", "query_timeout_ms": 30000}})
    conn = a.get_conn("lite")
    conn.execute("CREATE TABLE t (id INTEGER)")
    conn.executemany("INSERT INTO t (id) VALUES (?)", [(i,) for i in range(2000)])
    res = a.get_table_sample("lite", "t", None, limit=100, offset=500)
    assert res["row_count"] == 100


# --- valor de timeout chega ao driver, sem truncar ---

def test_postgres_sets_statement_timeout_in_ms():
    a = PostgresAdapter({"pg": {"url": "x", "query_timeout_ms": 45000}})
    fake = FakeConn()
    a.get_conn = lambda cid: fake
    a.execute_read_only("pg", "SELECT 1", 10)
    assert any("SET statement_timeout = 45000" in s for s in fake.statements)


def test_mysql_sets_max_execution_time_in_ms():
    a = MysqlAdapter({"my": {"url": "mysql://u:p@h:3306/d", "query_timeout_ms": 45000}})
    fake = FakeConn()
    a.get_conn = lambda cid: fake
    a.execute_read_only("my", "SELECT 1", 10)
    assert any("MAX_EXECUTION_TIME=45000" in s for s in fake.statements)


def test_oracle_sets_call_timeout_in_ms():
    a = OracleAdapter({"ora": {"url": "oracle://u:p@h:1521/svc", "query_timeout_ms": 45000}})
    fake = FakeConn()
    a.get_conn = lambda cid: fake
    a.execute_read_only("ora", "SELECT 1 FROM DUAL", 10)
    assert fake.call_timeout == 45000


def test_mssql_connect_uses_timeouts(monkeypatch):
    from nautilus.adapters.sql import sqlserver as mssql_mod
    from nautilus.config.url_builder import build_url_from_components

    captured = {}

    def fake_connect(**kwargs):
        captured.update(kwargs)
        return FakeConn()

    monkeypatch.setattr(mssql_mod.pymssql, "connect", fake_connect)
    url = build_url_from_components("sqlserver", "u", "p", "h", "1433", "db")
    a = mssql_mod.MssqlAdapter({"ms": {"url": url, "query_timeout_ms": 45000, "connect_timeout_ms": 7000}})
    a.get_conn("ms")
    assert captured["login_timeout"] == 7
    assert captured["timeout"] == 45

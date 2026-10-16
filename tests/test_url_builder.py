from nautilus.config.url_builder import build_url_from_components
from nautilus.core.sql_utils import parse_sql_url
from nautilus.adapters.sql.sqlserver import _parse_odbc


def test_sqlserver_semicolon_and_equals_and_space_in_password_roundtrips():
    url = build_url_from_components("sqlserver", "sa", "pa;ss=w rd", "h", "1433", "db")
    p = _parse_odbc(url)
    assert p["uid"] == "sa"
    assert p["pwd"] == "pa;ss=w rd"
    assert p["server"] == "h,1433"
    assert p["database"] == "db"


def test_sqlserver_brace_in_password_roundtrips():
    url = build_url_from_components("sqlserver", "sa", "pa}ss", "h", "1433", "db")
    p = _parse_odbc(url)
    assert p["pwd"] == "pa}ss"


def test_postgres_space_password_is_percent20_and_decodes():
    url = build_url_from_components("postgresql", "user", "p ss", "h", "5432", "db")
    assert "%20" in url and "+" not in url
    host, port, u, pw, db = parse_sql_url(url)
    assert u == "user"
    assert pw == "p ss"


def test_mysql_special_password_decodes():
    url = build_url_from_components("mysql", "user", "p@ss w/d", "h", "3306", "db")
    host, port, u, pw, db = parse_sql_url(url)
    assert pw == "p@ss w/d"


def test_oracle_decodes_space_in_password(monkeypatch):
    from nautilus.adapters.sql import oracle as ora_mod

    captured = {}

    def fake_connect(**kwargs):
        captured.update(kwargs)
        return object()

    monkeypatch.setattr(ora_mod.oracledb, "connect", fake_connect)
    url = build_url_from_components("oracle", "sys", "p ss", "h", "1521", "svc")
    a = ora_mod.OracleAdapter({"o": {"url": url, "connect_timeout_ms": 5000}})
    a.get_conn("o")
    assert captured["user"] == "sys"
    assert captured["password"] == "p ss"

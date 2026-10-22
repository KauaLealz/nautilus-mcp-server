import pytest

from nautilus.config.loader import load_config, ConfigError
from nautilus.config import crypto


def _write(tmp_path, text):
    p = tmp_path / "nautilus.yaml"
    p.write_text(text, encoding="utf-8")
    return str(p)


@pytest.fixture(autouse=True)
def _isolate_key(tmp_path, monkeypatch):
    monkeypatch.setenv(crypto.KEY_FILE_ENV, str(tmp_path / "keyfile"))


@pytest.fixture
def key(monkeypatch):
    k = crypto.generate_key()
    monkeypatch.setenv(crypto.KEY_ENV, k)
    return k


def test_url_direct(tmp_path):
    path = _write(tmp_path, """
connections:
  pg:
    type: postgresql
    url: "postgresql://u:p@localhost:5432/db"
""")
    cfg = load_config(path)
    pg = cfg["databases"]["pg"]
    assert pg["type"] == "postgresql"
    assert pg["url"] == "postgresql://u:p@localhost:5432/db"
    assert pg["read_only"] is True
    assert pg["query_timeout_ms"] == 30000
    assert pg["connect_timeout_ms"] == 5000


def test_components_build(tmp_path, key):
    enc = crypto.encrypt("s3cret", key)
    path = _write(tmp_path, f"""
connections:
  app:
    type: mysql
    host: db.example.com
    port: 3306
    user: app
    password: "{enc}"
    database: appdb
""")
    cfg = load_config(path)
    url = cfg["databases"]["app"]["url"]
    assert url.startswith("mysql+pymysql://app:s3cret@db.example.com:3306/appdb")


def test_env_var_not_interpolated_is_literal(tmp_path):
    # ${VAR} não é mais interpolado: fica literal no valor (sem ler o ambiente).
    path = _write(tmp_path, """
connections:
  pg:
    type: postgresql
    url: "postgresql://u:p@${HOST}:5432/d"
""")
    cfg = load_config(path)
    assert cfg["databases"]["pg"]["url"] == "postgresql://u:p@${HOST}:5432/d"


def test_invalid_type_raises(tmp_path):
    path = _write(tmp_path, """
connections:
  x:
    type: cassandra
    url: "whatever"
""")
    with pytest.raises(ConfigError):
        load_config(path)


def test_missing_connections_raises(tmp_path):
    path = _write(tmp_path, """
defaults:
  read_only: true
""")
    with pytest.raises(ConfigError):
        load_config(path)


def test_read_only_override(tmp_path):
    path = _write(tmp_path, """
defaults:
  read_only: true
connections:
  rw:
    type: postgresql
    url: "postgresql://u:p@h:5432/d"
    read_only: false
  ro:
    type: postgresql
    url: "postgresql://u:p@h:5432/d2"
""")
    cfg = load_config(path)
    assert cfg["databases"]["rw"]["read_only"] is False
    assert cfg["databases"]["ro"]["read_only"] is True


def test_per_conn_timeout_override(tmp_path):
    path = _write(tmp_path, """
defaults:
  query_timeout_ms: 30000
connections:
  slow:
    type: postgresql
    url: "postgresql://u:p@h:5432/d"
    query_timeout_ms: 120000
""")
    cfg = load_config(path)
    assert cfg["databases"]["slow"]["query_timeout_ms"] == 120000


def test_mariadb_normalized(tmp_path, key):
    enc = crypto.encrypt("p", key)
    path = _write(tmp_path, f"""
connections:
  m:
    type: mariadb
    host: h
    user: u
    password: "{enc}"
    database: d
""")
    cfg = load_config(path)
    assert cfg["databases"]["m"]["type"] == "mysql"


def test_encrypted_password_is_decrypted(tmp_path, key):
    enc = crypto.encrypt("sênha com espaço", key)
    path = _write(tmp_path, f"""
connections:
  app:
    type: postgresql
    host: h
    user: u
    password: "{enc}"
    database: d
""")
    cfg = load_config(path)
    # senha decifrada entra na URL já percent-encoded
    assert "s%C3%AAnha%20com%20espa%C3%A7o" in cfg["databases"]["app"]["url"]


def test_plaintext_password_rejected(tmp_path, key):
    path = _write(tmp_path, """
connections:
  app:
    type: postgresql
    host: h
    user: u
    password: "texto-puro"
    database: d
""")
    with pytest.raises(ConfigError) as exc:
        load_config(path)
    assert "cifrada" in str(exc.value)


def test_envvar_password_rejected(tmp_path, key, monkeypatch):
    monkeypatch.setenv("PG_PASS", "fromenv")
    path = _write(tmp_path, """
connections:
  app:
    type: postgresql
    host: h
    user: u
    password: "${PG_PASS}"
    database: d
""")
    with pytest.raises(ConfigError) as exc:
        load_config(path)
    assert "cifrada" in str(exc.value)


def test_encrypted_password_without_key_raises(tmp_path, monkeypatch):
    k = crypto.generate_key()
    enc = crypto.encrypt("s3cret", k)
    monkeypatch.delenv(crypto.KEY_ENV, raising=False)
    path = _write(tmp_path, f"""
connections:
  app:
    type: postgresql
    host: h
    user: u
    password: "{enc}"
    database: d
""")
    with pytest.raises(ConfigError) as exc:
        load_config(path)
    assert crypto.KEY_ENV in str(exc.value)


def test_wrong_key_raises(tmp_path, monkeypatch):
    enc = crypto.encrypt("s3cret", crypto.generate_key())
    monkeypatch.setenv(crypto.KEY_ENV, crypto.generate_key())  # chave diferente
    path = _write(tmp_path, f"""
connections:
  app:
    type: postgresql
    host: h
    user: u
    password: "{enc}"
    database: d
""")
    with pytest.raises(ConfigError):
        load_config(path)


def test_sqlite_path(tmp_path):
    path = _write(tmp_path, """
connections:
  lite:
    type: sqlite
    path: "C:\\\\data\\\\app.db"
""")
    cfg = load_config(path)
    assert cfg["databases"]["lite"]["url"].startswith("file:")


def test_redis_socket_timeout_defaults_to_query_timeout(tmp_path):
    path = _write(tmp_path, """
defaults:
  query_timeout_ms: 20000
connections:
  cache:
    type: redis
    url: "${REDIS_URL}"
""".replace("${REDIS_URL}", "redis://localhost:6379/0"))
    cfg = load_config(path)
    assert cfg["databases"]["cache"]["socket_timeout_ms"] == 20000

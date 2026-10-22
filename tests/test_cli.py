import pytest

from nautilus import cli
from nautilus.config import crypto
from nautilus.config.loader import load_config


@pytest.fixture(autouse=True)
def _isolate_key(tmp_path, monkeypatch):
    monkeypatch.setenv(crypto.KEY_FILE_ENV, str(tmp_path / "keyfile"))


@pytest.fixture
def key(monkeypatch):
    k = crypto.generate_key()
    monkeypatch.setenv(crypto.KEY_ENV, k)
    return k


def test_add_encrypts_password(key):
    data = {}
    cli.add_connection(
        data, "pg",
        {"type": "postgresql", "host": "h", "user": "u", "database": "d"},
        password_plain="s3cret", key=key,
    )
    stored = data["connections"]["pg"]["password"]
    assert crypto.is_encrypted(stored)
    assert "s3cret" not in stored
    assert crypto.decrypt(stored, key) == "s3cret"


def test_add_duplicate_raises(key):
    data = {"connections": {"pg": {"type": "postgresql"}}}
    with pytest.raises(cli.CliError):
        cli.add_connection(data, "pg", {"type": "postgresql"}, key=key)


def test_add_invalid_type_raises(key):
    with pytest.raises(cli.CliError):
        cli.add_connection({}, "x", {"type": "cassandra"}, key=key)


def test_add_sqlite_without_password(key):
    data = {}
    cli.add_connection(data, "lite", {"type": "sqlite", "path": "./a.db"}, key=key)
    entry = data["connections"]["lite"]
    assert entry["type"] == "sqlite"
    assert "password" not in entry


def test_edit_changes_password(key):
    data = {"connections": {"pg": {"type": "postgresql", "host": "h",
                                   "password": crypto.encrypt("old", key)}}}
    cli.edit_connection(data, "pg", {}, password_plain="new", key=key)
    assert crypto.decrypt(data["connections"]["pg"]["password"], key) == "new"


def test_edit_missing_raises(key):
    with pytest.raises(cli.CliError):
        cli.edit_connection({}, "nope", {}, key=key)


def test_remove(key):
    data = {"connections": {"pg": {"type": "postgresql"}, "my": {"type": "mysql"}}}
    cli.remove_connection(data, "pg")
    assert "pg" not in data["connections"]
    assert "my" in data["connections"]


def test_remove_missing_raises():
    with pytest.raises(cli.CliError):
        cli.remove_connection({"connections": {}}, "nope")


def test_list_never_exposes_password(key):
    data = {"connections": {"pg": {"type": "postgresql", "host": "h",
                                   "password": crypto.encrypt("s3cret", key)}}}
    rows = cli.list_connections(data)
    assert rows[0]["id"] == "pg"
    assert rows[0]["has_password"] is True
    assert "s3cret" not in str(rows)


def test_write_then_load_roundtrip(tmp_path, key):
    """add via CLI -> grava YAML -> loader decifra e monta a URL."""
    path = str(tmp_path / "nautilus.yaml")
    data = cli.read_yaml(path)
    cli.add_connection(
        data, "pg",
        {"type": "postgresql", "host": "db", "port": 5432, "user": "u", "database": "app"},
        password_plain="s3cret", key=key,
    )
    cli.write_yaml(path, data)

    cfg = load_config(path)
    assert cfg["databases"]["pg"]["url"].startswith("postgresql://u:s3cret@db:5432/app")
    # texto puro nunca aparece no arquivo gravado
    assert "s3cret" not in (tmp_path / "nautilus.yaml").read_text(encoding="utf-8")


def test_read_yaml_missing_file_returns_empty(tmp_path):
    assert cli.read_yaml(str(tmp_path / "nao-existe.yaml")) == {}

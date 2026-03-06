"""
Configuração central do Nautilus MCP Server.
Carrega conexões e limites de segurança a partir de variáveis de ambiente.
Suporta URL única ou variáveis separadas (user, password, host, etc.) para evitar
quebra com caracteres especiais (@, :, /) em senha ou usuário.
"""
import os
from typing import Any
from urllib.parse import quote

from pydantic import BaseModel, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class DatabaseConfig(BaseModel):
    """Configuração de uma conexão de banco de dados."""

    type: str  # postgresql | mysql | sqlserver | oracle | mongodb | redis
    url: str
    read_only: bool = True

    @field_validator("type")
    @classmethod
    def type_must_be_supported(cls, v: str) -> str:
        supported = {"postgresql", "mysql", "sqlserver", "oracle", "mongodb", "redis"}
        if v.lower() not in supported:
            raise ValueError(f"Tipo de banco não suportado: {v}. Use: {supported}")
        return v.lower()


class Settings(BaseSettings):
    """Configurações do servidor lidas de variáveis de ambiente."""

    model_config = SettingsConfigDict(
        env_prefix="NAUTILUS_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    query_max_length: int = 2000
    query_timeout_seconds: int = 30
    max_rows: int = 500
    allow_write: bool = False
    confirm_write_token: str = ""  # Se definido, execute_confirmed_write exige este token

    # Conexões são carregadas via _load_databases (chave dinâmica não mapeável direto no Pydantic)
    databases: dict[str, DatabaseConfig] = {}

    @classmethod
    def from_env(cls, **kwargs: Any) -> "Settings":
        """Constrói Settings e carrega databases a partir de variáveis DATABASES__<id>__<key>."""
        # Primeiro carrega o que o Pydantic consegue mapear
        instance = cls(**kwargs)
        # Depois preenche databases a partir do env
        instance.databases = _load_databases_from_env()
        return instance


def _default_port(db_type: str) -> str:
    """Porta padrão por tipo de banco."""
    ports = {
        "postgresql": "5432",
        "mysql": "3306",
        "sqlserver": "1433",
        "oracle": "1521",
        "mongodb": "27017",
        "redis": "6379",
    }
    return ports.get(db_type, "")


def _build_url_from_components(
    db_type: str,
    user: str,
    password: str,
    host: str,
    port: str,
    database: str,
) -> str:
    """
    Monta URL de conexão com user/password codificados (evita quebra com @, :, /).
    """
    safe_user = quote(user, safe="")
    safe_password = quote(password, safe="")
    if db_type == "postgresql":
        path = (database or "").strip().lstrip("/") or "postgres"
        return f"postgresql://{safe_user}:{safe_password}@{host}:{port}/{path}"
    if db_type == "mysql":
        path = (database or "").strip().lstrip("/")
        return f"mysql://{safe_user}:{safe_password}@{host}:{port}/{path}" if path else f"mysql://{safe_user}:{safe_password}@{host}:{port}"
    if db_type == "oracle":
        service = (database or "").strip().lstrip("/") or "ORCL"
        return f"oracle://{safe_user}:{safe_password}@{host}:{port}/{service}"
    if db_type == "mongodb":
        path = (database or "").strip().lstrip("/")
        if safe_user and safe_password:
            base = f"mongodb://{safe_user}:{safe_password}@{host}:{port}"
        elif safe_user:
            base = f"mongodb://{safe_user}@{host}:{port}"
        elif safe_password:
            base = f"mongodb://:{safe_password}@{host}:{port}"
        else:
            base = f"mongodb://{host}:{port}"
        return f"{base}/{path}" if path else base
    if db_type == "redis":
        db_num = (database or "0").strip().lstrip("/") or "0"
        if safe_user and safe_password:
            return f"redis://{safe_user}:{safe_password}@{host}:{port}/{db_num}"
        if safe_password:
            return f"redis://:{safe_password}@{host}:{port}/{db_num}"
        return f"redis://{host}:{port}/{db_num}"
    if db_type == "sqlserver":
        # ODBC: caracteres especiais no PWD podem exigir URL encodada; preferir url quando senha tiver ;
        driver = "ODBC Driver 17 for SQL Server"
        return f"odbc://DRIVER={{{driver}}};SERVER={host},{port};DATABASE={database or 'master'};UID={user};PWD={password};TrustServerCertificate=yes"
    return ""


def _load_databases_from_env() -> dict[str, DatabaseConfig]:
    """
    Lê variáveis DATABASES__<connection_id>__<key> e monta conexões.
    Se houver user, password e host (em vez de url), monta a URL com quote() para
    suportar caracteres especiais (@, :, /) em usuário e senha.
    """
    prefix = "DATABASES__"
    raw: dict[str, dict[str, str]] = {}
    for key, value in os.environ.items():
        if not key.startswith(prefix) or not value.strip():
            continue
        rest = key[len(prefix) :].lower()
        parts = rest.split("__")
        if len(parts) != 2:
            continue
        conn_id, attr = parts[0], parts[1]
        if conn_id not in raw:
            raw[conn_id] = {}
        raw[conn_id][attr] = value.strip()
    result: dict[str, DatabaseConfig] = {}
    for conn_id, data in raw.items():
        type_val = data.get("type")
        if not type_val:
            continue
        url_val = data.get("url")
        user = data.get("user", "").strip()
        password = data.get("password", "").strip()
        host = data.get("host", "").strip()
        port = (data.get("port") or _default_port(type_val)).strip()
        database = (data.get("database") or data.get("db") or "").strip()
        if not url_val and host:
            url_val = _build_url_from_components(type_val, user, password, host, port, database)
        if not url_val:
            continue
        read_only = data.get("read_only", "true").lower() in ("true", "1", "yes")
        try:
            result[conn_id] = DatabaseConfig(
                type=type_val,
                url=url_val,
                read_only=read_only,
            )
        except Exception:
            continue
    return result


_settings: Settings | None = None


def get_settings() -> Settings:
    """Retorna instância singleton das configurações (carrega .env na primeira chamada)."""
    global _settings
    if _settings is None:
        _settings = Settings.from_env()
    return _settings

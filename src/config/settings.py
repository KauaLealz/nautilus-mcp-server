"""
Configuração central do Nautilus MCP Server.
Carrega conexões e limites de segurança a partir de variáveis de ambiente.
"""
import os
from typing import Any

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


def _load_databases_from_env() -> dict[str, DatabaseConfig]:
    """Lê todas as variáveis DATABASES__<connection_id>__<key> e monta o dicionário de conexões."""
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
        url_val = data.get("url")
        if not type_val or not url_val:
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

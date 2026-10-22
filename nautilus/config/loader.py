import os
import re
import logging

import yaml

from nautilus.config.url_builder import (
    SUPPORTED_ENGINES,
    parse_host_and_port,
    default_port,
    build_url_from_components,
)
from nautilus.config.crypto import is_encrypted, decrypt, CryptoError

logger = logging.getLogger("nautilus")

DEFAULT_CONFIG_PATH = "nautilus.yaml"

_ENV_PATTERN = re.compile(r"\$\{([^}]+)\}")

# Defaults de fábrica, usados quando o bloco `defaults` do YAML não os define.
FACTORY_DEFAULTS = {
    "read_only": True,
    "query_timeout_ms": 30000,
    "connect_timeout_ms": 5000,
    "max_rows": 200,
    "default_rows": 50,
    "query_max_length": 2000,
}


class ConfigError(Exception):
    """Configuração inválida — falha cedo, na subida do servidor."""


def _as_bool(value, fallback=False):
    if isinstance(value, bool):
        return value
    if value is None:
        return fallback
    return str(value).strip().lower() not in ("false", "0", "no", "")


def _coerce_timeout(value, name, ctx):
    if value is None:
        return None
    try:
        n = int(value)
    except (TypeError, ValueError):
        raise ConfigError(f"{name} em {ctx} deve ser um inteiro em milissegundos.")
    if n <= 0:
        raise ConfigError(f"{name} em {ctx} deve ser positivo.")
    return n


def interpolate(value, ctx):
    """Troca ${VAR} pelo valor do ambiente. Variável ausente é erro de config."""
    if not isinstance(value, str):
        return value

    def repl(match):
        var = match.group(1).strip()
        if var not in os.environ:
            raise ConfigError(
                f"Variável de ambiente '{var}' referenciada em {ctx} não está definida."
            )
        return os.environ[var]

    return _ENV_PATTERN.sub(repl, value)


def _resolve_defaults(defaults_raw):
    d = defaults_raw or {}
    if not isinstance(d, dict):
        raise ConfigError("Bloco 'defaults' deve ser um mapeamento.")
    ctx = "defaults"
    return {
        "read_only": _as_bool(d.get("read_only", FACTORY_DEFAULTS["read_only"]), FACTORY_DEFAULTS["read_only"]),
        "query_timeout_ms": _coerce_timeout(d.get("query_timeout_ms"), "query_timeout_ms", ctx) or FACTORY_DEFAULTS["query_timeout_ms"],
        "connect_timeout_ms": _coerce_timeout(d.get("connect_timeout_ms"), "connect_timeout_ms", ctx) or FACTORY_DEFAULTS["connect_timeout_ms"],
        "max_rows": int(d.get("max_rows", FACTORY_DEFAULTS["max_rows"])),
        "default_rows": int(d.get("default_rows", FACTORY_DEFAULTS["default_rows"])),
        "query_max_length": int(d.get("query_max_length", FACTORY_DEFAULTS["query_max_length"])),
    }


def _resolve_password(password_raw, ctx):
    """Senha só é aceita cifrada (`enc:...`) e é decifrada com NAUTILUS_KEY.

    Texto puro e ${VAR} não são aceitos — a senha deve ser gravada pelo CLI
    (`nautilus add` / `nautilus edit`), que a cifra na hora.
    """
    if password_raw is None:
        return ""
    pw = str(password_raw)
    if not is_encrypted(pw):
        raise ConfigError(
            f"{ctx}: a senha deve estar cifrada (formato 'enc:...'). "
            f"Texto puro e ${{VAR}} não são aceitos — grave-a com "
            f"`nautilus add` ou `nautilus edit`."
        )
    try:
        return decrypt(pw)
    except CryptoError as e:
        raise ConfigError(f"{ctx}: {e}")


def _build_connection(conn_id, data, defaults):
    ctx = f"conexão '{conn_id}'"
    if not isinstance(data, dict):
        raise ConfigError(f"{ctx} deve ser um mapeamento de atributos.")

    type_raw = data.get("type")
    if not type_raw:
        raise ConfigError(f"{ctx} não tem 'type'.")
    raw_t = str(type_raw).strip().lower()
    if raw_t not in SUPPORTED_ENGINES:
        raise ConfigError(
            f"{ctx} com type inválido: '{type_raw}'. Suportados: {', '.join(sorted(SUPPORTED_ENGINES))}."
        )
    t = "mysql" if raw_t == "mariadb" else raw_t

    url_val = interpolate(data.get("url") or "", ctx).strip()
    user = interpolate(str(data.get("user", "")), ctx).strip()
    password = _resolve_password(data.get("password"), ctx)
    host_raw = interpolate(str(data.get("host", "")), ctx).strip()
    port_fallback = str(data.get("port", "")).strip() or default_port(t)
    host, port = parse_host_and_port(host_raw, port_fallback)
    database = interpolate(str(data.get("database", data.get("db", ""))), ctx).strip()

    if t == "sqlite":
        if not url_val:
            p = interpolate(str(data.get("path", data.get("file", ""))), ctx).strip()
            if p:
                if p.startswith("file:") or p == ":memory:":
                    url_val = p
                else:
                    url_val = f"file:{p.replace(chr(92), '/')}"
        if not url_val:
            raise ConfigError(f"{ctx} (sqlite) precisa de 'url' ou 'path'.")
    elif not url_val:
        if not host:
            raise ConfigError(f"{ctx} precisa de 'url' ou de 'host' (+ user/password/database).")
        url_val = build_url_from_components(t, user, password, host, port, database)
        if not url_val:
            raise ConfigError(f"{ctx}: não foi possível montar a URL para o type '{t}'.")

    query_timeout_ms = _coerce_timeout(data.get("query_timeout_ms"), "query_timeout_ms", ctx) or defaults["query_timeout_ms"]
    connect_timeout_ms = _coerce_timeout(data.get("connect_timeout_ms"), "connect_timeout_ms", ctx) or defaults["connect_timeout_ms"]

    cfg = {
        "type": t,
        "url": url_val,
        "read_only": _as_bool(data.get("read_only", defaults["read_only"]), defaults["read_only"]),
        "query_timeout_ms": query_timeout_ms,
        "connect_timeout_ms": connect_timeout_ms,
    }
    if t == "redis":
        cfg["socket_timeout_ms"] = _coerce_timeout(data.get("socket_timeout_ms"), "socket_timeout_ms", ctx) or query_timeout_ms
    return cfg


def load_config(path):
    """Lê e valida o YAML. Devolve {'defaults': {...}, 'databases': {...}}.

    Levanta ConfigError (falha cedo) em qualquer inconsistência.
    """
    try:
        with open(path, "r", encoding="utf-8") as f:
            raw = yaml.safe_load(f)
    except FileNotFoundError:
        raise ConfigError(f"Arquivo de configuração não encontrado: {path}")
    except yaml.YAMLError as e:
        raise ConfigError(f"YAML inválido em {path}: {e}")

    if raw is None:
        raise ConfigError(f"Arquivo de configuração vazio: {path}")
    if not isinstance(raw, dict):
        raise ConfigError(f"A raiz de {path} deve ser um mapeamento (defaults/connections).")

    defaults = _resolve_defaults(raw.get("defaults"))

    conns_raw = raw.get("connections")
    if not conns_raw or not isinstance(conns_raw, dict):
        raise ConfigError("Bloco 'connections' ausente ou vazio.")

    databases = {}
    for conn_id, data in conns_raw.items():
        cid = str(conn_id)
        cfg = _build_connection(cid, data, defaults)
        databases[cid] = cfg
        logger.info(
            f"Conexão '{cid}' do tipo '{cfg['type']}' carregada do YAML "
            f"(somente leitura: {cfg['read_only']}, timeout: {cfg['query_timeout_ms']}ms)."
        )

    logger.info(f"Total de conexões carregadas do YAML: {len(databases)}")
    return {"defaults": defaults, "databases": databases}

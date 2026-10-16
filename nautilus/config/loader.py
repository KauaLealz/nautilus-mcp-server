import os
import logging

from nautilus.config.url_builder import (
    SUPPORTED_ENGINES,
    parse_host_and_port,
    default_port,
    build_url_from_components,
)

logger = logging.getLogger("nautilus")


def load_databases_from_env():
    prefix = "DATABASES__"
    raw = {}
    for key, value in os.environ.items():
        if not key.startswith(prefix) or not value.strip():
            continue
        rest = key[len(prefix):].lower()
        parts = rest.split("__")
        if len(parts) != 2:
            continue
        conn_id, attr = parts[0], parts[1]
        if conn_id not in raw:
            raw[conn_id] = {}
        raw[conn_id][attr] = value.strip()

    result = {}
    for conn_id, data in raw.items():
        type_val = data.get("type")
        if not type_val:
            continue
        raw_t = type_val.lower()
        if raw_t not in SUPPORTED_ENGINES:
            continue
        t = "mysql" if raw_t == "mariadb" else raw_t
        url_val = data.get("url", "").strip()
        user = data.get("user", "").strip()
        password = data.get("password", "").strip()
        host_raw = data.get("host", "").strip()
        port_fallback = data.get("port", "").strip() or default_port(t)
        host, port = parse_host_and_port(host_raw, port_fallback)
        database = data.get("database", data.get("db", "")).strip()

        if t == "sqlite":
            if not url_val:
                p = data.get("path", data.get("file", "")).strip()
                if p:
                    if p.startswith("file:") or p == ":memory:":
                        url_val = p
                    else:
                        normalized_p = p.replace('\\', '/')
                        url_val = f"file:{normalized_p}"
            if not url_val:
                continue

        elif host:
            url_val = build_url_from_components(t, user, password, host, port, database)

        if not url_val:
            continue

        read_only = data.get("read_only", "true").lower() not in ("false", "0", "no")
        result[conn_id] = {
            "type": t,
            "url": url_val,
            "read_only": read_only
        }
        logger.info(f"Conexão '{conn_id}' do tipo '{t}' carregada com sucesso do ambiente (Somente Leitura: {read_only}).")
    logger.info(f"Total de conexões de banco de dados carregadas: {len(result)}")
    return result

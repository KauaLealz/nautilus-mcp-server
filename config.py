import os
import logging
from urllib.parse import urlparse, quote_plus
from dotenv import load_dotenv

logger = logging.getLogger("nautilus")

load_dotenv()

SUPPORTED_ENGINES = {
    "postgresql", "mysql", "mariadb", "sqlite", "sqlserver", "oracle", "mongodb", "redis"
}

def parse_host_and_port(host_raw: str, port_fallback: str):
    h = host_raw.strip()
    if not h:
        return "", port_fallback.strip()
    if h.startswith("["):
        idx = h.find("]")
        if idx != -1:
            port_part = h[idx+1:]
            if port_part.startswith(":"):
                return h[:idx+1], port_part[1:].strip()
            return h[:idx+1], port_fallback.strip()
        return h, port_fallback.strip()
    idx = h.rfind(":")
    if idx > 0:
        tail = h[idx+1:]
        if tail.isdigit() and len(tail) <= 5:
            return h[:idx], tail
    return h, port_fallback.strip()

def default_port(db_type: str) -> str:
    ports = {
        "postgresql": "5432",
        "mysql": "3306",
        "sqlserver": "1433",
        "oracle": "1521",
        "mongodb": "27017",
        "redis": "6379",
    }
    return ports.get(db_type, "")

def odbc_escape(value: str) -> str:
    return value.replace("}", "}}")

def build_url_from_components(db_type: str, user: str, password: str, host: str, port: str, database: str) -> str:
    safe_user = quote_plus(user) if user else ""
    safe_password = quote_plus(password) if password else ""
    
    if db_type == "postgresql":
        path = database.strip().lstrip("/") or "postgres"
        return f"postgresql://{safe_user}:{safe_password}@{host}:{port}/{path}"
    
    if db_type == "mysql":
        path = database.strip().lstrip("/")
        if path:
            return f"mysql+pymysql://{safe_user}:{safe_password}@{host}:{port}/{path}"
        return f"mysql+pymysql://{safe_user}:{safe_password}@{host}:{port}"
        
    if db_type == "oracle":
        service = database.strip().lstrip("/") or "ORCL"
        return f"oracle+oracledb://{safe_user}:{safe_password}@{host}:{port}/{service}"
        
    if db_type == "mongodb":
        path = database.strip().lstrip("/")
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
        db_num = database.strip().lstrip("/") or "0"
        if safe_user and safe_password:
            return f"redis://{safe_user}:{safe_password}@{host}:{port}/{db_num}"
        if safe_password:
            return f"redis://:{safe_password}@{host}:{port}/{db_num}"
        return f"redis://{host}:{port}/{db_num}"
        
    if db_type == "sqlserver":
        u = odbc_escape(user)
        p = odbc_escape(password)
        db_name = database or "master"
        return f"Driver={{ODBC Driver 17 for SQL Server}};Server={host},{port};Database={db_name};Uid={u};Pwd={p};Encrypt=yes;TrustServerCertificate=yes;"
        
    return ""

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

def get_settings():
    max_row_limit = int(os.environ.get("NAUTILUS_MAX_ROW_LIMIT", "0"))
    if max_row_limit <= 0:
        max_row_limit = int(os.environ.get("NAUTILUS_MAX_ROWS", "200"))
    if max_row_limit <= 0:
        max_row_limit = 200
    max_row_limit = min(max(1, max_row_limit), 10000)
    
    default_row_limit = int(os.environ.get("NAUTILUS_DEFAULT_ROW_LIMIT", "50"))
    if default_row_limit <= 0:
        default_row_limit = 50
    default_row_limit = min(max(1, default_row_limit), max_row_limit)
    
    timeout_ms = int(os.environ.get("NAUTILUS_QUERY_TIMEOUT_MS", "0"))
    if timeout_ms <= 0:
        sec = int(os.environ.get("NAUTILUS_QUERY_TIMEOUT_SECONDS", "0"))
        timeout_ms = int(sec * 1000) if sec > 0 else 5000
    timeout_ms = min(max(1000, timeout_ms), 600000)
    
    read_only_mode = os.environ.get("NAUTILUS_READ_ONLY_MODE", "true").lower() not in ("false", "0", "no")
    
    return {
        "query_max_length": int(os.environ.get("NAUTILUS_QUERY_MAX_LENGTH", "2000")),
        "default_row_limit": default_row_limit,
        "max_row_limit": max_row_limit,
        "query_timeout_ms": timeout_ms,
        "read_only_mode": read_only_mode,
        "databases": load_databases_from_env()
    }

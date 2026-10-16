from urllib.parse import quote_plus

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

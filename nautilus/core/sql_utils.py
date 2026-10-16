from urllib.parse import urlparse, unquote


def q_id(s: str) -> str:
    return f'"{str(s).replace(chr(34), chr(34) + chr(34))}"'


def parse_sql_url(url: str):
    parsed = urlparse(url)
    user = unquote(parsed.username) if parsed.username else None
    password = unquote(parsed.password) if parsed.password else None
    host = parsed.hostname
    port = parsed.port
    db = parsed.path.lstrip("/") if parsed.path else None
    return host, port, user, password, db

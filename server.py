from mcp.server.fastmcp import FastMCP
from pydantic import Field
from typing import Optional, List, Dict, Any
import json
import time
import os
import sys
import logging
from bson import ObjectId
from datetime import datetime
from decimal import Decimal

from config import get_settings
from validator import SqlQueryValidator, QuerySafetyError
from formatter import FormatterService
from adapters import (
    SqliteAdapter, PostgresAdapter, MysqlAdapter, MssqlAdapter, OracleAdapter,
    MongodbAdapter, RedisAdapter
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    stream=sys.stderr
)
logger = logging.getLogger("nautilus")


mcp = FastMCP(
    "Nautilus",
    instructions="Nautilus MCP multicloud: PostgreSQL, MySQL/MariaDB, SQLite, SQL Server, Oracle, MongoDB, Redis. Somente leitura. Primeiro use db_list_connections para obter os connection_id. Tools: db_list_connections, db_list_resources, db_get_metadata, db_describe_indexes, db_query_sql, db_fetch_documents, db_read_cache, db_peek_sample. Limites: padrão 50 linhas/documentos, máximo 200; timeout padrão 5000 ms (NAUTILUS_QUERY_TIMEOUT_MS). Conexões via env DATABASES__<id>__*. Recurso nautilus://connections: JSON com a mesma lista de conexões."
)
settings = get_settings()
databases = settings["databases"]

@mcp.resource("nautilus://connections", description="Conexões configuradas (connection_id, type, read_only).", mime_type="application/json")
def get_connections() -> str:
    conns = [{"connection_id": cid, "type": cfg["type"], "read_only": cfg["read_only"]} for cid, cfg in databases.items()]
    return json.dumps({"connections": conns})

sqlite_conns = {cid: cfg for cid, cfg in databases.items() if cfg["type"] == "sqlite"}
postgres_conns = {cid: cfg for cid, cfg in databases.items() if cfg["type"] == "postgresql"}
mysql_conns = {cid: cfg for cid, cfg in databases.items() if cfg["type"] == "mysql"}
mssql_conns = {cid: cfg for cid, cfg in databases.items() if cfg["type"] == "sqlserver"}
oracle_conns = {cid: cfg for cid, cfg in databases.items() if cfg["type"] == "oracle"}
mongodb_conns = {cid: cfg for cid, cfg in databases.items() if cfg["type"] == "mongodb"}
redis_conns = {cid: cfg for cid, cfg in databases.items() if cfg["type"] == "redis"}

sqlite_adapter = SqliteAdapter(sqlite_conns) if sqlite_conns else None
postgres_adapter = PostgresAdapter(postgres_conns) if postgres_conns else None
mysql_adapter = MysqlAdapter(mysql_conns) if mysql_conns else None
mssql_adapter = MssqlAdapter(mssql_conns) if mssql_conns else None
oracle_adapter = OracleAdapter(oracle_conns) if oracle_conns else None
mongodb_adapter = MongodbAdapter(mongodb_conns, settings["query_timeout_ms"]) if mongodb_conns else None
redis_adapter = RedisAdapter(redis_conns, settings["query_timeout_ms"]) if redis_conns else None

sql_validator = SqlQueryValidator(
    max_length=settings["query_max_length"],
    max_rows_cap=settings["max_row_limit"]
)

def resolve_connection_id(connection_id: str) -> str:
    target = connection_id.strip().lower()
    for cid in databases:
        if cid.lower() == target:
            return cid
    raise ValueError(f"Conexao nao encontrada: {connection_id}")

def get_db_adapter(cid: str):
    cfg = databases[cid]
    t = cfg["type"]
    if t == "sqlite" and sqlite_adapter:
        return sqlite_adapter, t
    if t == "postgresql" and postgres_adapter:
        return postgres_adapter, t
    if t == "mysql" and mysql_adapter:
        return mysql_adapter, t
    if t == "sqlserver" and mssql_adapter:
        return mssql_adapter, t
    if t == "oracle" and oracle_adapter:
        return oracle_adapter, t
    if t == "mongodb" and mongodb_adapter:
        return mongodb_adapter, t
    if t == "redis" and redis_adapter:
        return redis_adapter, t
    raise ValueError(f"Tipo de conexao nao suportado ou nao configurado: {t}")

def deep_sanitize(value):
    if value is None:
        return None
    if isinstance(value, ObjectId):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, bytes):
        try:
            return value.decode("utf-8")
        except Exception:
            return value.hex()
    if isinstance(value, list):
        return [deep_sanitize(v) for v in value]
    if isinstance(value, dict):
        return {k: deep_sanitize(v) for k, v in value.items()}
    return value

@mcp.tool(name="db_list_connections")
def db_list_connections() -> str:
    logger.info("Listando conexões de banco de dados disponíveis.")
    connections = [{"connection_id": cid, "type": cfg["type"], "read_only": cfg["read_only"]} for cid, cfg in databases.items()]
    if not connections:
        logger.warning("Nenhuma conexão configurada encontrada.")
        return "Nenhuma conexao configurada. Defina variaveis DATABASES__<connection_id>__*."
    lines = [f"- {c['connection_id']}: engine {c['type']}{' (read_only)' if c['read_only'] else ''}" for c in connections]
    logger.info(f"Retornando {len(connections)} conexões configuradas.")
    return f"Conexoes disponiveis ({len(connections)}):\n" + "\n".join(lines)

@mcp.tool(name="db_list_resources")
def db_list_resources(
    connection_id: str,
    schema: Optional[str] = None,
    database: Optional[str] = None,
    key_pattern: Optional[str] = None,
    cursor: Optional[str] = None
) -> str:
    logger.info(f"Listando recursos para connection_id: {connection_id}, schema: {schema}, database: {database}")
    cid = resolve_connection_id(connection_id)
    adapter, engine = get_db_adapter(cid)
    
    if engine in {"sqlite", "postgresql", "mysql", "sqlserver", "oracle"}:
        tables = adapter.list_tables(cid, schema)
        if not tables:
            logger.info(f"Nenhuma tabela encontrada no schema '{schema or 'default'}' para {cid}")
            return f"Nenhuma tabela encontrada no schema '{schema or 'default'}'."
        logger.info(f"Retornando {len(tables)} tabelas encontradas no banco {cid}")
        return "Tabelas:\n" + "\n".join(f"  - {t[0]}.{t[1]}" for t in tables)
        
    elif engine == "mongodb":
        if not database:
            logger.warning("Database não informado para listar coleções MongoDB.")
            return "Erro: database e obrigatorio para MongoDB."
        cols = adapter.list_collections(cid, database)
        if not cols:
            logger.info(f"Nenhuma coleção no database '{database}' para {cid}")
            return f"Nenhuma colecao no database '{database}'."
        logger.info(f"Retornando {len(cols)} coleções para {cid}")
        return f"Colecoes no database '{database}':\n" + "\n".join(f"  - {c}" for c in cols)
        
    elif engine == "redis":
        pat = key_pattern or "*"
        res = adapter.scan_keys(cid, pat, settings["max_row_limit"], cursor)
        lines = [f"  - {k}" for k in res["keys"]]
        out = f"Chaves encontradas (padrao '{pat}'):\n" + "\n".join(lines)
        if res.get("nextCursor"):
            out += f"\nProximo cursor: {res['nextCursor']}"
        logger.info(f"Retornando {len(res['keys'])} chaves Redis para {cid}")
        return out
        
    logger.error(f"Engine de banco não suportado para listagem de recursos: {engine}")
    return "Engine nao suportado."

@mcp.tool(name="db_get_metadata")
def db_get_metadata(
    connection_id: str,
    resource_name: str,
    schema: Optional[str] = None,
    database: Optional[str] = None
) -> str:
    cid = resolve_connection_id(connection_id)
    adapter, engine = get_db_adapter(cid)
    
    if engine in {"sqlite", "postgresql", "mysql", "sqlserver", "oracle"}:
        info = adapter.describe_table(cid, resource_name, schema)
        if not info:
            return f"Tabela '{resource_name}' nao encontrada."
        lines = [f"  - {c['name']}: {c['data_type']}{' (NULL)' if c['nullable'] else ' (NOT NULL)'}" for c in info["columns"]]
        return f"Colunas da tabela {info['schema_name']}.{info['table_name']}:\n" + "\n".join(lines)
        
    elif engine == "mongodb":
        if not database:
            return "Erro: database e obrigatorio para MongoDB."
        docs = adapter.find_documents(cid, database, resource_name, "{}", 5, 0)
        docs = deep_sanitize(docs)
        return FormatterService.format_mongo_documents(docs, 5)
        
    elif engine == "redis":
        t = adapter.key_type(cid, resource_name)
        ttl = adapter.key_ttl(cid, resource_name)
        sample = adapter.read_structured_sample(cid, resource_name, 5)
        return f"Chave: {resource_name}\nTipo: {t}\nTTL: {ttl}s\nAmostra:\n{json.dumps(sample, indent=2, default=str)}"
        
    return "Engine nao suportado."

@mcp.tool(name="db_describe_indexes")
def db_describe_indexes(
    connection_id: str,
    table_name: str,
    schema: Optional[str] = None,
    database: Optional[str] = None
) -> str:
    cid = resolve_connection_id(connection_id)
    adapter, engine = get_db_adapter(cid)
    
    if engine in {"sqlite", "postgresql", "mysql", "sqlserver", "oracle"}:
        idx = adapter.list_indexes(cid, table_name, schema)
        if not idx:
            return f"Nenhum indice listado para a tabela '{table_name}'."
        lines = [f"  - {i['index_name']}: colunas [{', '.join(i['columns'])}]{' UNIQUE' if i['is_unique'] else ''}" for i in idx]
        return f"Indices de '{table_name}':\n" + "\n".join(lines)
        
    elif engine == "mongodb":
        if not database:
            return "Erro: database e obrigatorio para MongoDB."
        idx = adapter.list_collection_indexes(cid, database, table_name)
        return json.dumps(deep_sanitize(idx), indent=2)
        
    return "Engine nao suportado para descrever indices."

@mcp.tool(name="db_query_sql")
def db_query_sql(
    connection_id: str,
    query: str,
    limit: Optional[int] = None,
    output_format: Optional[str] = "json"
) -> str:
    logger.info(f"Executando query SQL na conexão {connection_id}. Query original: {query.strip()}")
    cid = resolve_connection_id(connection_id)
    adapter, engine = get_db_adapter(cid)
    
    if engine not in {"sqlite", "postgresql", "mysql", "sqlserver", "oracle"}:
        logger.warning(f"Tentativa de executar query SQL em banco não SQL. Engine: {engine}")
        return "db_query_sql so se aplica a conexoes SQL."
        
    try:
        validated_query = sql_validator.sanitize_or_raise(query)
    except QuerySafetyError as e:
        logger.error(f"Erro de segurança ao validar query SQL: {e}")
        return f"Erro de seguranca: {str(e)}"
        
    cap = settings["max_row_limit"]
    lim = limit if (limit is not None and limit > 0) else settings["default_row_limit"]
    lim = min(max(1, lim), cap)
    
    logger.info(f"Query higienizada para execução no {engine}: {validated_query} (limite: {lim})")
    try:
        res = adapter.execute_read_only(cid, validated_query, lim, settings["query_timeout_ms"] // 1000)
    except Exception as e:
        logger.error(f"Falha na execução da query no banco {cid}: {e}")
        raise
    
    fmt = output_format or "json"
    logger.info(f"Query executada com sucesso. Retornando {res['row_count']} linhas no formato {fmt}")
    if fmt == "csv":
        return FormatterService.query_result_to_csv(res["columns"], res["rows"])
    elif fmt == "table":
        return FormatterService.format_query_result(res["columns"], res["rows"], res["row_count"], lim)
    else:
        return FormatterService.query_result_to_json(res["columns"], res["rows"])

@mcp.tool(name="db_fetch_documents")
def db_fetch_documents(
    connection_id: str,
    database: str,
    collection: str,
    filter_json: Optional[str] = None,
    limit: Optional[int] = None,
    skip: Optional[int] = None
) -> str:
    cid = resolve_connection_id(connection_id)
    adapter, engine = get_db_adapter(cid)
    
    if engine != "mongodb":
        return "db_fetch_documents so se aplica a conexao MongoDB."
        
    cap = settings["max_row_limit"]
    lim = limit if (limit is not None and limit > 0) else settings["default_row_limit"]
    lim = min(max(1, lim), cap)
    
    sk = skip if (skip is not None and skip >= 0) else 0
    filt = filter_json or "{}"
    
    docs = adapter.find_documents(cid, database, collection, filt, lim, sk)
    docs = deep_sanitize(docs)
    return FormatterService.format_mongo_documents(docs, lim)

@mcp.tool(name="db_read_cache")
def db_read_cache(
    connection_id: str,
    key: str,
    max_elements: Optional[int] = None
) -> str:
    cid = resolve_connection_id(connection_id)
    adapter, engine = get_db_adapter(cid)
    
    if engine != "redis":
        return "db_read_cache so se aplica a conexao Redis."
        
    cap = max_elements if (max_elements is not None and max_elements > 0) else settings["default_row_limit"]
    cap = min(cap, settings["max_row_limit"])
    
    res = adapter.read_structured_sample(cid, key, cap)
    return json.dumps(res, indent=2, default=str)

@mcp.tool(name="db_peek_sample")
def db_peek_sample(
    connection_id: str,
    resource_name: str,
    schema: Optional[str] = None,
    database: Optional[str] = None
) -> str:
    cid = resolve_connection_id(connection_id)
    adapter, engine = get_db_adapter(cid)
    
    if engine in {"sqlite", "postgresql", "mysql", "sqlserver", "oracle"}:
        res = adapter.get_table_sample(cid, resource_name, schema, 3, 0)
        return FormatterService.format_query_result(res["columns"], res["rows"], res["row_count"], 3)
        
    elif engine == "mongodb":
        if not database:
            return "Erro: database e obrigatorio para MongoDB."
        docs = adapter.find_documents(cid, database, resource_name, "{}", 3, 0)
        docs = deep_sanitize(docs)
        return FormatterService.format_mongo_documents(docs, 3)
        
    elif engine == "redis":
        sample = adapter.read_structured_sample(cid, resource_name, 3)
        return json.dumps(sample, indent=2, default=str)
        
    return "Engine nao suportado."

def main():
    mcp.run()

if __name__ == "__main__":
    main()

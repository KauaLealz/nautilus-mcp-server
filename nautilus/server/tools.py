import json
import logging
from typing import Optional

from nautilus.server.app import (
    mcp,
    settings,
    databases,
    sql_validator,
    resolve_connection_id,
    get_db_adapter,
    deep_sanitize,
)
from nautilus.core.validator import QuerySafetyError
from nautilus.core.formatter import FormatterService

logger = logging.getLogger("nautilus")


@mcp.tool(name="db_list_connections")
def db_list_connections() -> str:
    logger.info("Listando conexões de banco de dados disponíveis.")
    connections = [{"connection_id": cid, "type": cfg["type"], "read_only": cfg["read_only"]} for cid, cfg in databases.items()]
    if not connections:
        logger.warning("Nenhuma conexão configurada encontrada.")
        return "Nenhuma conexao configurada. Defina as conexoes no arquivo YAML (env NAUTILUS_CONFIG, padrao nautilus.yaml)."
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
        res = adapter.execute_read_only(cid, validated_query, lim)
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

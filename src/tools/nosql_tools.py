"""
Tools MCP para bancos NoSQL: MongoDB e Redis.
"""
from src.bootstrap import get_adapter
from src.config.settings import get_settings
from src.utils.error_handler import ErrorHandler
from src.utils.formatter import format_mongo_documents, format_redis_keys


def register_nosql_tools(mcp):
    """Registra as tools NoSQL no FastMCP."""

    @mcp.tool()
    async def list_collections(connection_id: str, database: str):
        """
        Lista as coleções de um database MongoDB.
        Args:
            connection_id: ID da conexão (de list_connections), deve ser tipo mongodb.
            database: Nome do database.
        Returns:
            Lista de nomes das coleções.
        """
        try:
            if not connection_id or not connection_id.strip():
                return "Erro: connection_id não pode ser vazio."
            if not database or not database.strip():
                return "Erro: database não pode ser vazio."
            adapter = get_adapter(connection_id.strip())
            if adapter is None:
                return f"Conexão '{connection_id}' não encontrada."
            if not hasattr(adapter, "list_collections"):
                return f"Conexão '{connection_id}' não é MongoDB. Use list_connections para ver os tipos."
            names = await adapter.list_collections(connection_id.strip(), database.strip())
            if not names:
                return f"Nenhuma coleção no database '{database}'."
            return "Coleções:\n" + "\n".join(f"  - {n}" for n in names)
        except Exception as e:
            info = ErrorHandler.handle(e, "list_collections")
            return ErrorHandler.format_for_agent(info)

    @mcp.tool()
    async def find_documents(
        connection_id: str,
        database: str,
        collection: str,
        filter_json: str = "{}",
        limit: int = 100,
    ):
        """
        Busca documentos em uma coleção MongoDB com filtro (JSON).
        Apenas leitura; limite máximo de 500 documentos.
        Args:
            connection_id: ID da conexão (de list_connections), tipo mongodb.
            database: Nome do database.
            collection: Nome da coleção.
            filter_json: Filtro em JSON (ex: {"status": "active"}). Use "{}" para todos.
            limit: Máximo de documentos a retornar (até 500).
        Returns:
            Documentos em formato JSON.
        """
        try:
            if not connection_id or not connection_id.strip():
                return "Erro: connection_id não pode ser vazio."
            if not database or not database.strip():
                return "Erro: database não pode ser vazio."
            if not collection or not collection.strip():
                return "Erro: collection não pode ser vazio."
            adapter = get_adapter(connection_id.strip())
            if adapter is None:
                return f"Conexão '{connection_id}' não encontrada."
            if not hasattr(adapter, "find_documents"):
                return f"Conexão '{connection_id}' não é MongoDB."
            cap = get_settings().max_rows
            limit = min(max(1, limit), cap)
            docs = await adapter.find_documents(
                connection_id.strip(),
                database.strip(),
                collection.strip(),
                filter_json=filter_json.strip() or "{}",
                limit=limit,
            )
            return format_mongo_documents(docs, max_display=50)
        except Exception as e:
            info = ErrorHandler.handle(e, "find_documents")
            return ErrorHandler.format_for_agent(info)

    @mcp.tool()
    async def redis_get(connection_id: str, key: str):
        """
        Obtém o valor de uma chave Redis (leitura).
        Args:
            connection_id: ID da conexão (de list_connections), tipo redis.
            key: Nome da chave.
        Returns:
            Valor da chave ou mensagem se não existir.
        """
        try:
            if not connection_id or not connection_id.strip():
                return "Erro: connection_id não pode ser vazio."
            if not key or not key.strip():
                return "Erro: key não pode ser vazia."
            adapter = get_adapter(connection_id.strip())
            if adapter is None:
                return f"Conexão '{connection_id}' não encontrada."
            if not hasattr(adapter, "get_key"):
                return f"Conexão '{connection_id}' não é Redis."
            value = await adapter.get_key(connection_id.strip(), key.strip())
            if value is None:
                return f"Chave '{key}' não encontrada ou sem valor."
            return value
        except Exception as e:
            info = ErrorHandler.handle(e, "redis_get")
            return ErrorHandler.format_for_agent(info)

    @mcp.tool()
    async def redis_keys(connection_id: str, pattern: str = "*"):
        """
        Lista chaves Redis que batem com o pattern (ex: "user:*").
        Limite de 200 chaves para evitar sobrecarga.
        Args:
            connection_id: ID da conexão (de list_connections), tipo redis.
            pattern: Pattern glob (padrão: *).
        Returns:
            Lista de chaves.
        """
        try:
            if not connection_id or not connection_id.strip():
                return "Erro: connection_id não pode ser vazio."
            adapter = get_adapter(connection_id.strip())
            if adapter is None:
                return f"Conexão '{connection_id}' não encontrada."
            if not hasattr(adapter, "keys"):
                return f"Conexão '{connection_id}' não é Redis."
            keys = await adapter.keys(
                connection_id.strip(),
                pattern=(pattern.strip() if pattern else "*"),
            )
            if not keys:
                return f"Nenhuma chave encontrada para o pattern '{pattern or '*'}'."
            return "Chaves:\n" + format_redis_keys(keys)
        except Exception as e:
            info = ErrorHandler.handle(e, "redis_keys")
            return ErrorHandler.format_for_agent(info)

    @mcp.tool()
    async def mongodb_aggregate(
        connection_id: str,
        database: str,
        collection: str,
        pipeline_json: str,
        limit: int = 500,
    ):
        """
        Executa pipeline de agregação MongoDB (read-only). Stages permitidos: $match, $project, $group, $sort, $limit, $skip, $unwind, $lookup, $count, $addFields. Proibidos: $out, $merge.
        Args:
            connection_id: ID da conexão mongodb.
            database: Nome do database.
            collection: Nome da coleção.
            pipeline_json: Pipeline em JSON (lista de stages).
            limit: Limite de documentos no resultado.
        """
        try:
            adapter = get_adapter(connection_id.strip())
            if not adapter or not hasattr(adapter, "aggregate"):
                return f"Conexão '{connection_id}' não é MongoDB ou não suporta aggregate."
            docs = await adapter.aggregate(
                connection_id.strip(),
                database.strip(),
                collection.strip(),
                pipeline_json.strip() or "[]",
                limit=min(limit, 500),
            )
            return format_mongo_documents(docs, max_display=50)
        except Exception as e:
            return ErrorHandler.format_for_agent(ErrorHandler.handle(e, "mongodb_aggregate"))

    @mcp.tool()
    async def redis_key_type(connection_id: str, key: str):
        """Retorna o tipo da chave Redis (string, list, set, hash, zset, etc.)."""
        try:
            adapter = get_adapter(connection_id.strip())
            if not adapter or not hasattr(adapter, "key_type"):
                return f"Conexão '{connection_id}' não é Redis."
            t = await adapter.key_type(connection_id.strip(), key.strip())
            return f"Tipo da chave '{key}': {t}"
        except Exception as e:
            return ErrorHandler.format_for_agent(ErrorHandler.handle(e, "redis_key_type"))

    @mcp.tool()
    async def redis_key_ttl(connection_id: str, key: str):
        """Retorna o TTL da chave Redis em segundos (-1 = sem expiração, -2 = não existe)."""
        try:
            adapter = get_adapter(connection_id.strip())
            if not adapter or not hasattr(adapter, "key_ttl"):
                return f"Conexão '{connection_id}' não é Redis."
            ttl = await adapter.key_ttl(connection_id.strip(), key.strip())
            if ttl == -2:
                return f"Chave '{key}' não existe."
            if ttl == -1:
                return f"Chave '{key}' não tem expiração."
            return f"TTL da chave '{key}': {ttl} segundos."
        except Exception as e:
            return ErrorHandler.format_for_agent(ErrorHandler.handle(e, "redis_key_ttl"))

    @mcp.tool()
    async def redis_mget(connection_id: str, keys: str):
        """Retorna os valores de várias chaves Redis de uma vez. keys: lista separada por vírgula (máx. 50)."""
        try:
            adapter = get_adapter(connection_id.strip())
            if not adapter or not hasattr(adapter, "mget"):
                return f"Conexão '{connection_id}' não é Redis."
            key_list = [k.strip() for k in keys.split(",") if k.strip()][:50]
            if not key_list:
                return "Nenhuma chave fornecida."
            values = await adapter.mget(connection_id.strip(), key_list)
            lines = []
            for k, v in zip(key_list, values):
                lines.append(f"  {k}: {v if v is not None else '(não existe)'}")
            return "Valores:\n" + "\n".join(lines)
        except Exception as e:
            return ErrorHandler.format_for_agent(ErrorHandler.handle(e, "redis_mget"))

"""
Adapter para MongoDB (pymongo async).
Operações de leitura: listar coleções e buscar documentos com limite.
"""
import json
from datetime import date, datetime
from typing import Any

from pymongo.asynchronous.mongo_client import AsyncMongoClient

try:
    from bson import ObjectId
except ImportError:
    ObjectId = None


def _sanitize_mongo_docs(docs: list[dict]) -> list[dict]:
    """Converte ObjectId, datetime, etc. para tipos JSON-serializáveis."""
    out = []
    for doc in docs:
        if not isinstance(doc, dict):
            out.append(doc)
            continue
        clean = {}
        for k, v in doc.items():
            if ObjectId is not None and isinstance(v, ObjectId):
                clean[k] = str(v)
            elif isinstance(v, datetime):
                clean[k] = v.isoformat()
            elif isinstance(v, date):
                clean[k] = v.isoformat()
            elif isinstance(v, dict):
                clean[k] = _sanitize_mongo_docs([v])[0] if v else {}
            elif isinstance(v, list):
                clean[k] = [_sanitize_mongo_docs([x])[0] if isinstance(x, dict) else (str(x) if ObjectId is not None and type(x).__name__ == "ObjectId" else x) for x in v]
            elif isinstance(v, (bytes, bytearray)):
                clean[k] = v.decode("utf-8", errors="replace")
            else:
                try:
                    json.dumps(v, default=str)
                    clean[k] = v
                except (TypeError, ValueError):
                    clean[k] = str(v)
        out.append(clean)
    return out

from src.config.settings import DatabaseConfig
from src.domain.models import ConnectionInfo
from src.adapters.base import connection_info_from_config


class MongodbAdapter:
    """Adapter para MongoDB: listar coleções e find com limite (somente leitura)."""

    def __init__(self, connections: dict[str, DatabaseConfig], timeout_seconds: int = 30):
        self._connections = {k: v for k, v in connections.items() if v.type == "mongodb"}
        self._timeout_ms = timeout_seconds * 1000
        self._clients: dict[str, AsyncMongoClient] = {}

    def _get_client(self, connection_id: str) -> AsyncMongoClient:
        config = self._connections.get(connection_id)
        if not config:
            raise KeyError(f"Conexão não encontrada: {connection_id}")
        if connection_id not in self._clients:
            self._clients[connection_id] = AsyncMongoClient(config.url)
        return self._clients[connection_id]

    def list_connections(self) -> list[ConnectionInfo]:
        return [connection_info_from_config(cid, c) for cid, c in self._connections.items()]

    def get_connection_info(self, connection_id: str) -> ConnectionInfo | None:
        config = self._connections.get(connection_id)
        if not config:
            return None
        return connection_info_from_config(connection_id, config)

    async def test_connection(self, connection_id: str) -> bool:
        try:
            client = self._get_client(connection_id)
            await client.admin.command("ping")
            return True
        except Exception:
            return False

    async def list_collections(self, connection_id: str, database: str) -> list[str]:
        """Lista nomes das coleções do database."""
        client = self._get_client(connection_id)
        db = client[database]
        names = await db.list_collection_names()
        return sorted(names)

    async def find_documents(
        self,
        connection_id: str,
        database: str,
        collection: str,
        filter_json: str = "{}",
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        """
        Busca documentos com filtro (JSON). Apenas leitura; limite máximo aplicado.
        """
        client = self._get_client(connection_id)
        db = client[database]
        coll = db[collection]
        try:
            filt = json.loads(filter_json) if filter_json.strip() else {}
        except json.JSONDecodeError:
            filt = {}
        cap = min(limit, 500)  # cap interno do adapter; a tool já limita por get_settings().max_rows
        cursor = coll.find(filt, max_time_ms=self._timeout_ms).limit(cap)
        return await cursor.to_list(length=cap)

    _ALLOWED_AGGREGATE_STAGES = frozenset({"$match", "$project", "$group", "$sort", "$limit", "$skip", "$unwind", "$lookup", "$count", "$addFields"})
    _FORBIDDEN_AGGREGATE_STAGES = frozenset({"$out", "$merge", "$currentOp", "$listSessions", "$collStats", "$indexStats", "$planCacheStats"})

    async def aggregate(
        self,
        connection_id: str,
        database: str,
        collection: str,
        pipeline_json: str,
        limit: int = 500,
    ) -> list[dict[str, Any]]:
        """Executa pipeline de agregação (read-only). Stages permitidos: match, project, group, sort, limit, skip, unwind, look up, count. Proibidos: $out, $merge."""
        client = self._get_client(connection_id)
        db = client[database]
        coll = db[collection]
        try:
            pipeline = json.loads(pipeline_json) if pipeline_json.strip() else []
        except json.JSONDecodeError:
            raise ValueError("Pipeline inválido: JSON malformado")
        if not isinstance(pipeline, list):
            raise ValueError("Pipeline deve ser uma lista de stages")
        for i, stage in enumerate(pipeline):
            if not isinstance(stage, dict) or len(stage) != 1:
                raise ValueError(f"Stage {i} inválido")
            op = list(stage.keys())[0]
            if op in self._FORBIDDEN_AGGREGATE_STAGES:
                raise ValueError(f"Stage '{op}' não é permitido (somente leitura)")
            if op not in self._ALLOWED_AGGREGATE_STAGES and not op.startswith("$"):
                raise ValueError(f"Stage '{op}' desconhecido")
        # Cópia para não mutar o original; adiciona $limit se necessário
        pipeline = [dict(s) for s in pipeline]
        if pipeline and list(pipeline[-1].keys())[0] != "$limit":
            pipeline = pipeline + [{"$limit": min(limit, 500)}]
        elif pipeline and list(pipeline[-1].keys())[0] == "$limit":
            pipeline[-1]["$limit"] = min(int(pipeline[-1]["$limit"]), 500)
        cursor = coll.aggregate(pipeline, maxTimeMS=self._timeout_ms)
        raw: list[dict[str, Any]] = []
        try:
            # AsyncCommandCursor: async for (to_list existe a partir do pymongo 4.9)
            async for doc in cursor:
                raw.append(doc)
                if len(raw) >= 500:
                    break
        except (AttributeError, TypeError):
            try:
                raw = await cursor.to_list(length=500)
            except Exception:
                raw = []
        except Exception as e:
            raise ValueError(f"MongoDB aggregate: {e!s}") from e
        return _sanitize_mongo_docs(raw)

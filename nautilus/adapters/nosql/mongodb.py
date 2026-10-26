import pymongo
import time
import logging

logger = logging.getLogger("nautilus")


class MongodbAdapter:
    def __init__(self, configs, timeout_ms):
        self.configs = configs
        self.timeout_ms = timeout_ms
        self.clients = {}

    def close_all(self):
        """Fecha todos os clients abertos — usado ao recarregar a config."""
        for client in self.clients.values():
            try:
                client.close()
            except Exception:
                pass
        self.clients.clear()

    def get_client(self, connection_id: str):
        client = self.clients.get(connection_id)
        if client is not None:
            try:
                client.admin.command("ping")
            except Exception:
                logger.info(f"Conexao MongoDB {connection_id} inativa. Removendo do cache.")
                try:
                    client.close()
                except Exception:
                    pass
                self.clients.pop(connection_id, None)
                client = None
        if client is None:
            cfg = self.configs.get(connection_id)
            if not cfg:
                raise ValueError(f"Conexao nao encontrada: {connection_id}")
            logger.info(f"Criando nova conexao MongoDB para {connection_id}")
            client = pymongo.MongoClient(
                cfg["url"], serverSelectionTimeoutMS=self.timeout_ms, connectTimeoutMS=self.timeout_ms
            )
            self.clients[connection_id] = client
        return client

    def test_connection(self, connection_id: str) -> bool:
        try:
            client = self.get_client(connection_id)
            client.admin.command("ping")
            return True
        except Exception:
            return False

    def probe_connection(self, connection_id: str) -> dict:
        t0 = time.perf_counter()
        try:
            client = self.get_client(connection_id)
            bi = client.admin.command("buildInfo")
            latency = int((time.perf_counter() - t0) * 1000)
            return {"ok": True, "latencyMs": latency, "version": str(bi.get("version"))}
        except Exception as e:
            return {"ok": False, "latencyMs": int((time.perf_counter() - t0) * 1000), "version": None, "error": str(e)}

    def list_collections(self, connection_id: str, database: str) -> list:
        client = self.get_client(connection_id)
        db = client[database]
        return sorted(db.list_collection_names())

    def find_documents(self, connection_id: str, database: str, collection: str, filter_json: str, limit: int, skip: int) -> list:
        import json
        client = self.get_client(connection_id)
        filt = {}
        if filter_json and filter_json.strip():
            try:
                filt = json.loads(filter_json)
            except Exception:
                pass
        db = client[database]
        coll = db[collection]
        cap = max(1, limit)
        sk = max(0, min(skip, 50000))
        cursor = coll.find(filt).skip(sk).limit(cap)
        docs = list(cursor)
        return docs

    def aggregate(self, connection_id: str, database: str, collection: str, pipeline_json: str, limit: int, skip: int) -> list:
        import json
        client = self.get_client(connection_id)
        try:
            pipeline = json.loads(pipeline_json) if pipeline_json.strip() else []
        except Exception:
            raise ValueError("Pipeline invalido: JSON malformado")
        if not isinstance(pipeline, list):
            raise ValueError("Pipeline deve ser uma lista de stages")
            
        forbidden = {"$out", "$merge", "$currentOp", "$listSessions", "$collStats", "$indexStats", "$planCacheStats"}
        allowed = {"$match", "$project", "$group", "$sort", "$limit", "$skip", "$unwind", "$lookup", "$count", "$addFields"}
        
        for i, stage in enumerate(pipeline):
            if not isinstance(stage, dict) or len(stage) != 1:
                raise ValueError(f"Stage {i} invalido")
            op = list(stage.keys())[0]
            if op in forbidden:
                raise ValueError(f"Stage '{op}' nao e permitido (somente leitura)")
            if op not in allowed and not op.startswith("$"):
                raise ValueError(f"Stage '{op}' desconhecido")
                
        copy_pipeline = [dict(s) for s in pipeline]
        sk = max(0, min(skip, 50000))
        if sk > 0:
            copy_pipeline.insert(0, {"$skip": sk})
            
        lim = max(1, limit)
        if not copy_pipeline or list(copy_pipeline[-1].keys())[0] != "$limit":
            copy_pipeline.append({"$limit": lim})
        else:
            last_stage = copy_pipeline[-1]
            last_stage["$limit"] = min(last_stage.get("$limit", lim), lim)
            
        db = client[database]
        coll = db[collection]
        return list(coll.aggregate(copy_pipeline))

    def list_collection_indexes(self, connection_id: str, database: str, collection: str) -> list:
        client = self.get_client(connection_id)
        db = client[database]
        coll = db[collection]
        return list(coll.list_indexes())

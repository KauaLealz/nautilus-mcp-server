import redis
import time
import logging

logger = logging.getLogger("nautilus")


class RedisAdapter:
    def __init__(self, configs, timeout_ms):
        self.configs = configs
        self.timeout_ms = timeout_ms
        self.clients = {}

    def get_client(self, connection_id: str):
        client = self.clients.get(connection_id)
        if client is not None:
            try:
                client.ping()
            except Exception:
                logger.info(f"Conexao Redis {connection_id} inativa. Removendo do cache.")
                self.clients.pop(connection_id, None)
                client = None
        if client is None:
            cfg = self.configs.get(connection_id)
            if not cfg:
                raise ValueError(f"Conexao nao encontrada: {connection_id}")
            logger.info(f"Criando nova conexao Redis para {connection_id}")
            client = redis.from_url(cfg["url"], socket_connect_timeout=self.timeout_ms / 1000.0)
            self.clients[connection_id] = client
        return client

    def test_connection(self, connection_id: str) -> bool:
        try:
            client = self.get_client(connection_id)
            return client.ping()
        except Exception:
            return False

    def probe_connection(self, connection_id: str) -> dict:
        t0 = time.perf_counter()
        try:
            client = self.get_client(connection_id)
            client.ping()
            info = client.info("server")
            version = info.get("redis_version")
            latency = int((time.perf_counter() - t0) * 1000)
            return {"ok": True, "latencyMs": latency, "version": str(version)}
        except Exception as e:
            return {"ok": False, "latencyMs": int((time.perf_counter() - t0) * 1000), "version": None, "error": str(e)}

    def get_key(self, connection_id: str, key: str) -> str:
        client = self.get_client(connection_id)
        val = client.get(key)
        return val.decode("utf-8") if isinstance(val, bytes) else val

    def scan_keys(self, connection_id: str, pattern: str, max_keys: int, start_cursor: str = None) -> dict:
        client = self.get_client(connection_id)
        cap = max(1, min(max_keys, 2000))
        pat = pattern.strip() if pattern else "*"
        cursor = int(start_cursor) if start_cursor and start_cursor != "0" else 0
        out = []
        truncated = False
        
        while True:
            cursor, keys = client.scan(cursor=cursor, match=pat, count=min(100, cap - len(out) + 20))
            for k in keys:
                out.append(k.decode("utf-8") if isinstance(k, bytes) else k)
                if len(out) >= cap:
                    truncated = cursor != 0
                    break
            if cursor == 0 or len(out) >= cap:
                break
                
        return {
            "keys": out,
            "nextCursor": str(cursor) if truncated else None,
            "truncated": truncated
        }

    def key_type(self, connection_id: str, key: str) -> str:
        client = self.get_client(connection_id)
        t = client.type(key)
        return t.decode("utf-8") if isinstance(t, bytes) else t

    def key_ttl(self, connection_id: str, key: str) -> int:
        client = self.get_client(connection_id)
        return client.ttl(key)

    def mget(self, connection_id: str, keys_list: list) -> list:
        client = self.get_client(connection_id)
        slice_keys = keys_list[:50]
        if not slice_keys:
            return []
        vals = client.mget(slice_keys)
        return [v.decode("utf-8") if isinstance(v, bytes) else v for v in vals]

    def read_structured_sample(self, connection_id: str, key: str, max_elements: int) -> dict:
        client = self.get_client(connection_id)
        raw_t = client.type(key)
        t = raw_t.decode("utf-8") if isinstance(raw_t, bytes) else raw_t
        cap = max(1, min(max_elements, 200))
        
        if t == "string":
            v = client.get(key)
            val = v.decode("utf-8") if isinstance(v, bytes) else v
            return {"redis_type": "string", "value": val}
            
        if t == "hash":
            h = client.hgetall(key)
            decoded_h = {}
            for k, v in h.items():
                dk = k.decode("utf-8") if isinstance(k, bytes) else k
                dv = v.decode("utf-8") if isinstance(v, bytes) else v
                decoded_h[dk] = dv
            entries = list(decoded_h.items())[:cap]
            return {
                "redis_type": "hash",
                "fields": dict(entries),
                "truncated": len(decoded_h) > cap,
                "field_count": len(decoded_h)
            }
            
        if t == "list":
            length = client.llen(key)
            slice_list = client.lrange(key, 0, cap - 1)
            decoded_list = [v.decode("utf-8") if isinstance(v, bytes) else v for v in slice_list]
            return {"redis_type": "list", "length": length, "elements": decoded_list, "truncated": length > cap}
            
        if t == "set":
            members = client.smembers(key)
            decoded_members = [v.decode("utf-8") if isinstance(v, bytes) else v for v in members]
            return {
                "redis_type": "set",
                "cardinality": len(decoded_members),
                "sample": decoded_members[:cap],
                "truncated": len(decoded_members) > cap
            }
            
        if t == "zset":
            length = client.zcard(key)
            slice_zset = client.zrange(key, 0, cap - 1)
            decoded_zset = [v.decode("utf-8") if isinstance(v, bytes) else v for v in slice_zset]
            return {"redis_type": "zset", "cardinality": length, "members": decoded_zset, "truncated": length > cap}
            
        return {"redis_type": t}

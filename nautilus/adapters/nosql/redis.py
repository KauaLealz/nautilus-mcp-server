import base64
import time
import logging

import redis
from redis.exceptions import (
    ConnectionError as RedisConnectionError,
    TimeoutError as RedisTimeoutError,
    ResponseError,
)

logger = logging.getLogger("nautilus")

MAX_VALUE_BYTES = 8192        # teto de bytes por elemento retornado
MAX_SAMPLE_ELEMENTS = 200     # teto duro de elementos amostrados
HEALTH_CHECK_INTERVAL = 30    # segundos


def _decode(v):
    """Decodifica bytes do Redis com segurança. Binário vira marcador base64;
    strings grandes são truncadas por bytes. Ints (XINFO) passam direto."""
    if v is None:
        return None
    if isinstance(v, (int, float, bool)):
        return v
    if isinstance(v, bytes):
        try:
            s = v.decode("utf-8")
        except UnicodeDecodeError:
            head = base64.b64encode(v[:MAX_VALUE_BYTES]).decode("ascii")
            return f"<binary:{len(v)}B:base64:{head}>"
        return _truncate(s, len(v))
    if isinstance(v, str):
        return _truncate(v, len(v.encode("utf-8")))
    return v


def _truncate(s, byte_len):
    if byte_len > MAX_VALUE_BYTES:
        cut = s.encode("utf-8")[:MAX_VALUE_BYTES].decode("utf-8", errors="ignore")
        return f"{cut}...<truncado, {byte_len} bytes no total>"
    return s


class RedisAdapter:
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

    def _build_client(self, connection_id):
        cfg = self.configs.get(connection_id)
        if not cfg:
            raise ValueError(f"Conexao nao encontrada: {connection_id}")
        socket_timeout = (cfg.get("socket_timeout_ms") or cfg.get("query_timeout_ms") or self.timeout_ms) / 1000.0
        connect_timeout = (cfg.get("connect_timeout_ms") or self.timeout_ms) / 1000.0
        logger.info(f"Criando nova conexao Redis para {connection_id} (socket_timeout={socket_timeout}s)")
        return redis.from_url(
            cfg["url"],
            socket_timeout=socket_timeout,
            socket_connect_timeout=connect_timeout,
            health_check_interval=HEALTH_CHECK_INTERVAL,
            retry_on_timeout=True,
            decode_responses=False,
        )

    def get_client(self, connection_id: str):
        client = self.clients.get(connection_id)
        if client is None:
            client = self._build_client(connection_id)
            self.clients[connection_id] = client
        return client

    def _run(self, connection_id, fn):
        """Executa fn(client) e reconecta UMA vez se a conexão cair."""
        try:
            return fn(self.get_client(connection_id))
        except (RedisConnectionError, RedisTimeoutError) as e:
            logger.warning(f"Conexao Redis {connection_id} caiu ({e}). Reconectando uma vez.")
            self.clients.pop(connection_id, None)
            return fn(self.get_client(connection_id))

    def test_connection(self, connection_id: str) -> bool:
        try:
            return bool(self._run(connection_id, lambda c: c.ping()))
        except Exception:
            return False

    def probe_connection(self, connection_id: str) -> dict:
        t0 = time.perf_counter()
        try:
            def op(c):
                c.ping()
                return c.info("server")
            info = self._run(connection_id, op)
            version = info.get("redis_version")
            latency = int((time.perf_counter() - t0) * 1000)
            return {"ok": True, "latencyMs": latency, "version": str(version)}
        except Exception as e:
            return {"ok": False, "latencyMs": int((time.perf_counter() - t0) * 1000), "version": None, "error": str(e)}

    def _safe_string(self, c, key):
        """Lê só os primeiros MAX_VALUE_BYTES do valor (via GETRANGE) para não
        materializar strings gigantes; devolve (valor_decodificado, tamanho_total)."""
        length = c.strlen(key)
        raw = c.getrange(key, 0, MAX_VALUE_BYTES)
        return _decode(raw), length

    def get_key(self, connection_id: str, key: str):
        def op(c):
            raw_t = c.type(key)
            t = raw_t.decode("utf-8") if isinstance(raw_t, bytes) else raw_t
            if t == "none":
                return None
            if t != "string":
                return {"error": f"Chave '{key}' e do tipo '{t}', nao string. Use db_read_cache para inspecionar."}
            val, _ = self._safe_string(c, key)
            return val
        return self._run(connection_id, op)

    def scan_keys(self, connection_id: str, pattern: str, max_keys: int, start_cursor: str = None) -> dict:
        cap = max(1, min(max_keys, 2000))
        pat = pattern.strip() if pattern else "*"
        try:
            start = int(start_cursor) if (start_cursor and str(start_cursor) != "0") else 0
        except (TypeError, ValueError):
            start = 0

        def op(c):
            out = []
            cursor = start
            while True:
                cursor, keys = c.scan(cursor=cursor, match=pat, count=200)
                for k in keys:
                    out.append(_decode(k))
                    if len(out) >= cap:
                        break
                if cursor == 0 or len(out) >= cap:
                    break
            truncated = cursor != 0
            return {
                "keys": out,
                "nextCursor": str(cursor) if truncated else None,
                "truncated": truncated,
            }
        return self._run(connection_id, op)

    def key_type(self, connection_id: str, key: str) -> str:
        def op(c):
            t = c.type(key)
            return t.decode("utf-8") if isinstance(t, bytes) else t
        return self._run(connection_id, op)

    def key_ttl(self, connection_id: str, key: str) -> int:
        return self._run(connection_id, lambda c: c.ttl(key))

    def mget(self, connection_id: str, keys_list: list) -> list:
        slice_keys = keys_list[:50]
        if not slice_keys:
            return []
        return self._run(connection_id, lambda c: [_decode(v) for v in c.mget(slice_keys)])

    def read_structured_sample(self, connection_id: str, key: str, max_elements: int) -> dict:
        cap = max(1, min(max_elements, MAX_SAMPLE_ELEMENTS))

        def op(c):
            raw_t = c.type(key)
            t = raw_t.decode("utf-8") if isinstance(raw_t, bytes) else raw_t

            if t == "none":
                # Pode ser RedisJSON (alguns builds nao reportam em TYPE) ou chave ausente.
                js = self._try_json(c, key)
                if js is not None:
                    return js
                return {"redis_type": "none", "exists": False}

            if t == "string":
                val, length = self._safe_string(c, key)
                return {"redis_type": "string", "value": val, "length": length, "truncated": length > MAX_VALUE_BYTES}

            if t == "hash":
                h = c.hgetall(key)
                items = list(h.items())[:cap]
                fields = {_decode(k): _decode(v) for k, v in items}
                return {"redis_type": "hash", "fields": fields, "field_count": len(h), "truncated": len(h) > cap}

            if t == "list":
                length = c.llen(key)
                elements = [_decode(v) for v in c.lrange(key, 0, cap - 1)]
                return {"redis_type": "list", "length": length, "elements": elements, "truncated": length > cap}

            if t == "set":
                length = c.scard(key)
                members = [_decode(v) for v in c.sscan(key, count=cap)[1][:cap]]
                return {"redis_type": "set", "cardinality": length, "sample": members, "truncated": length > cap}

            if t == "zset":
                length = c.zcard(key)
                members = [_decode(v) for v in c.zrange(key, 0, cap - 1)]
                return {"redis_type": "zset", "cardinality": length, "members": members, "truncated": length > cap}

            if t == "stream":
                return self._read_stream(c, key, cap)

            # Tipos de modulo (ReJSON-RL etc.)
            js = self._try_json(c, key)
            if js is not None:
                return js
            return {"redis_type": t}

        return self._run(connection_id, op)

    def _read_stream(self, c, key, cap):
        length = c.xlen(key)
        entries = []
        for sid, fields in c.xrange(key, count=cap):
            entries.append({
                "id": _decode(sid),
                "fields": {_decode(k): _decode(v) for k, v in fields.items()},
            })
        groups = []
        try:
            for g in c.xinfo_groups(key):
                groups.append({_decode(k): _decode(v) for k, v in g.items()})
        except ResponseError:
            pass
        return {
            "redis_type": "stream",
            "length": length,
            "entries": entries,
            "truncated": length > cap,
            "groups": groups,
        }

    def _try_json(self, c, key):
        try:
            raw = c.execute_command("JSON.GET", key)
        except ResponseError:
            return None
        if raw is None:
            return None
        return {"redis_type": "json", "value": _decode(raw)}

"""
Adapter para Redis (redis.asyncio).
Operações de leitura: GET e KEYS com limite para evitar sobrecarga.
"""
from redis.asyncio import Redis
from redis.asyncio.connection import ConnectionPool

from src.config.settings import DatabaseConfig
from src.domain.models import ConnectionInfo
from src.adapters.base import connection_info_from_config


class RedisAdapter:
    """Adapter para Redis: get e keys (com limite) em modo leitura."""

    MAX_KEYS = 200

    def __init__(self, connections: dict[str, DatabaseConfig], timeout_seconds: int = 30):
        self._connections = {k: v for k, v in connections.items() if v.type == "redis"}
        self._timeout = timeout_seconds
        self._pools: dict[str, ConnectionPool] = {}

    def _get_client(self, connection_id: str) -> Redis:
        config = self._connections.get(connection_id)
        if not config:
            raise KeyError(f"Conexão não encontrada: {connection_id}")
        if connection_id not in self._pools:
            self._pools[connection_id] = ConnectionPool.from_url(
                config.url,
                socket_connect_timeout=self._timeout,
                socket_timeout=self._timeout,
            )
        return Redis(connection_pool=self._pools[connection_id])

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
            await client.ping()
            return True
        except Exception:
            return False

    async def get_key(self, connection_id: str, key: str) -> str | None:
        """Retorna o valor da chave (string) ou None."""
        client = self._get_client(connection_id)
        value = await client.get(key)
        if value is None:
            return None
        if isinstance(value, bytes):
            return value.decode("utf-8", errors="replace")
        return str(value)

    async def keys(self, connection_id: str, pattern: str = "*") -> list[str]:
        """Lista chaves que batem com o pattern. Limite de MAX_KEYS."""
        client = self._get_client(connection_id)
        keys_list = await client.keys(pattern)
        result = [k.decode("utf-8") if isinstance(k, bytes) else str(k) for k in keys_list]
        return result[: self.MAX_KEYS]

    async def key_type(self, connection_id: str, key: str) -> str:
        """Retorna o tipo da chave (string, list, set, hash, zset, etc.)."""
        client = self._get_client(connection_id)
        t = await client.type(key)
        return t if isinstance(t, str) else (t.decode("utf-8") if isinstance(t, bytes) else str(t))

    async def key_ttl(self, connection_id: str, key: str) -> int | None:
        """Retorna TTL da chave em segundos (-1 sem expiração, -2 não existe)."""
        client = self._get_client(connection_id)
        return await client.ttl(key)

    MAX_MGET = 50

    async def mget(self, connection_id: str, keys: list[str]) -> list[str | None]:
        """Retorna valores de várias chaves (máximo MAX_MGET)."""
        client = self._get_client(connection_id)
        keys = keys[: self.MAX_MGET]
        values = await client.mget(keys)
        result = []
        for v in values:
            if v is None:
                result.append(None)
            elif isinstance(v, bytes):
                result.append(v.decode("utf-8", errors="replace"))
            else:
                result.append(str(v))
        return result

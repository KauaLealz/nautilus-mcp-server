import json

from nautilus.server.app import mcp, get_state


@mcp.resource("nautilus://connections", description="Conexões configuradas (connection_id, type, read_only).", mime_type="application/json")
def get_connections() -> str:
    databases = get_state()["databases"]
    conns = [{"connection_id": cid, "type": cfg["type"], "read_only": cfg["read_only"]} for cid, cfg in databases.items()]
    return json.dumps({"connections": conns})

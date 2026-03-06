"""
Nautilus MCP Server - Entry point.
Expõe tools para acesso seguro a bancos de dados SQL e NoSQL.
"""
from mcp.server.fastmcp import FastMCP

from src.tools.common_tools import register_common_tools
from src.tools.sql_tools import register_sql_tools
from src.tools.nosql_tools import register_nosql_tools
from src.tools.discovery_tools import register_discovery_tools
from src.tools.query_extra_tools import register_query_extra_tools
from src.tools.audit_tools import register_audit_tools
from src.tools.comparison_tools import register_comparison_tools
from src.tools.saved_queries_tools import register_saved_queries_tools
from src.tools.confirm_write_tools import register_confirm_write_tools

mcp = FastMCP(
    name="nautilus",
    instructions="Acesso seguro a bancos de dados SQL e NoSQL (PostgreSQL, MySQL, SQL Server, Oracle, MongoDB, Redis). Conexões são somente leitura por padrão; defina read_only=false apenas se quiser permitir escrita. [BETA]",
)

register_common_tools(mcp)
register_sql_tools(mcp)
register_nosql_tools(mcp)
register_discovery_tools(mcp)
register_query_extra_tools(mcp)
register_audit_tools(mcp)
register_comparison_tools(mcp)
register_saved_queries_tools(mcp)
register_confirm_write_tools(mcp)


if __name__ == "__main__":
    mcp.run(transport="stdio")

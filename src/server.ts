import "./stdio-guard.js"
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js"
import { registerNautilusResources } from "./resources/nautilus-resources.js"
import { registerAllTools } from "./tools/index.js"

export function createMcpServer(): McpServer {
  const server = new McpServer(
    { name: "nautilus", version: "1.0.0" },
    {
      instructions:
        "Nautilus MCP multicloud: PostgreSQL, MySQL/MariaDB, SQLite, SQL Server, Oracle, MongoDB, Redis. Somente leitura. Primeiro use db_list_connections para obter os connection_id. Tools: db_list_connections, db_list_resources, db_get_metadata, db_describe_indexes, db_query_sql, db_fetch_documents, db_read_cache, db_peek_sample. Limites: padrão 50 linhas/documentos, máximo 200; timeout padrão 5000 ms (NAUTILUS_QUERY_TIMEOUT_MS). Conexões via env DATABASES__<id>__*. Recurso nautilus://connections: JSON com a mesma lista de conexões.",
    },
  )
  registerAllTools(server)
  registerNautilusResources(server)
  return server
}

import type { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js"
import { getAppContext } from "../context/app-context.js"

const CONNECTIONS_URI = "nautilus://connections"

export function registerNautilusResources(server: McpServer): void {
  server.registerResource(
    "connections",
    CONNECTIONS_URI,
    {
      description: "Conexões configuradas (connection_id, type, read_only).",
      mimeType: "application/json",
    },
    async (_uri, _extra) => ({
      contents: [
        {
          uri: CONNECTIONS_URI,
          mimeType: "application/json",
          text: JSON.stringify({ connections: getAppContext().listConnections() }),
        },
      ],
    }),
  )
}

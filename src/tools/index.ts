import type { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js"
import { registerDbTools } from "./db/register-db-tools.js"

export function registerAllTools(server: McpServer): void {
  registerDbTools(server)
}

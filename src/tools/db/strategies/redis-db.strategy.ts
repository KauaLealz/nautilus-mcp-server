import type { AppContext } from "../../../context/app-context.js"
import type { DbEngineStrategy } from "../db-engine-strategy.js"

export function createRedisDbStrategy(ctx: AppContext): DbEngineStrategy {
  return {
    async listResources(resolvedId, rawConnectionId, _engineLabel, p, cap) {
      const pat = p.keyPattern?.trim() || "*"
      const page = await ctx.redis.scanKeysRaw(resolvedId, pat, cap, p.cursor?.trim() || null)
      const resources = page.keys.map((k) => ({ kind: "key" as const, name: k }))
      return {
        text:
          `Chaves (${resources.length}):\n` +
          resources.map((r) => `  ${r.name}`).join("\n") +
          (page.truncated ? `\n(cursor: ${page.nextCursor})` : ""),
        structured: {
          tool: "db_list_resources",
          connection_id: resolvedId,
          engine: "redis",
          resources,
          next_cursor: page.nextCursor,
          truncated: page.truncated,
        },
      }
    },

    async getMetadata(_resolvedId, rawConnectionId, _engineLabel, resourceName, _p, maxSampleRows) {
      return ctx.redis.readStructuredSample(rawConnectionId, resourceName.trim(), maxSampleRows)
    },

    async describeIndexes() {
      return "Redis não tem índices; use db_list_resources."
    },

    async peekSample(_resolvedId, rawConnectionId, resourceName, _p, peekLimit) {
      return ctx.redis.readStructuredSample(rawConnectionId, resourceName.trim(), peekLimit)
    },
  }
}

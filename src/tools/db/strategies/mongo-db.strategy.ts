import type { AppContext } from "../../../context/app-context.js"
import type { DbEngineStrategy } from "../db-engine-strategy.js"

export function createMongoDbStrategy(ctx: AppContext): DbEngineStrategy {
  return {
    async listResources(resolvedId, rawConnectionId, _engineLabel, p, cap) {
      if (!p.database?.trim()) return "Para MongoDB informe database."
      const cols = await ctx.mongodb.listCollectionNames(rawConnectionId, p.database.trim())
      const resources = cols.map((name) => ({
        kind: "collection" as const,
        database: p.database!.trim(),
        name,
      }))
      const sliced = resources.slice(0, cap)
      return {
        text: sliced.map((r) => `  ${r.database}.${r.name}`).join("\n"),
        structured: { tool: "db_list_resources", connection_id: resolvedId, engine: "mongodb", resources: sliced },
      }
    },

    async getMetadata(resolvedId, rawConnectionId, _engineLabel, resourceName, p, maxSampleRows) {
      if (!p.database?.trim()) return "database obrigatório."
      const lim = Math.min(maxSampleRows, ctx.settings.max_row_limit)
      const payload = await ctx.mongodb.findDocuments(
        rawConnectionId,
        p.database.trim(),
        resourceName.trim(),
        "{}",
        lim,
        0,
      )
      if (typeof payload === "string") return payload
      const st = payload.structured as Record<string, unknown>
      const docs = (st.documents as unknown[]) ?? []
      return {
        text: payload.text,
        structured: {
          tool: "db_get_metadata",
          connection_id: resolvedId,
          engine: "mongodb",
          collection: resourceName.trim(),
          sample_document_count: docs.length,
          documents: docs,
        },
      }
    },

    async describeIndexes(resolvedId, rawConnectionId, _engineLabel, tableName, p) {
      if (!p.database?.trim()) return "database obrigatório."
      return ctx.mongodb.listCollectionIndexes(rawConnectionId, p.database.trim(), tableName.trim())
    },

    async peekSample(resolvedId, rawConnectionId, resourceName, p, peekLimit) {
      if (!p.database?.trim()) return "database obrigatório."
      const payload = await ctx.mongodb.findDocuments(
        rawConnectionId,
        p.database.trim(),
        resourceName.trim(),
        "{}",
        peekLimit,
        0,
      )
      if (typeof payload === "string") return payload
      return {
        text: payload.text,
        structured: {
          ...(typeof payload.structured === "object" && payload.structured ? payload.structured : {}),
          tool: "db_peek_sample",
        },
      }
    },
  }
}

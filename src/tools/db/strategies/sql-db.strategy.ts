import type { AppContext } from "../../../context/app-context.js"
import type { DbEngineStrategy } from "../db-engine-strategy.js"

export function createSqlDbStrategy(ctx: AppContext): DbEngineStrategy {
  return {
    async listResources(resolvedId, _rawConnectionId, engineLabel, p, cap) {
      const adapter = ctx.getSqlAdapter(resolvedId)
      if (!adapter) return "Adapter SQL indisponível."
      const tables = await adapter.listTables(resolvedId, p.schema?.trim() || null)
      const resources = tables.map(([sch, name]) => ({ kind: "table" as const, schema: sch, name }))
      const sliced = resources.slice(0, cap)
      return {
        text:
          `${sliced.length} recurso(s) (tabelas), teto ${cap}.\n` +
          sliced.map((r) => `  ${r.schema}.${r.name}`).join("\n"),
        structured: {
          tool: "db_list_resources",
          connection_id: resolvedId,
          engine: engineLabel,
          resources: sliced,
          truncated: resources.length > cap,
        },
      }
    },

    async getMetadata(resolvedId, _rawConnectionId, engineLabel, resourceName, p, _maxSampleRows) {
      const adapter = ctx.getSqlAdapter(resolvedId)
      if (!adapter) return "Adapter SQL indisponível."
      const info = await adapter.describeTable(resolvedId, resourceName.trim(), p.schema?.trim() || null)
      if (!info) return "Recurso não encontrado."
      return {
        text: JSON.stringify(info, null, 2),
        structured: { tool: "db_get_metadata", connection_id: resolvedId, engine: engineLabel, table: info },
      }
    },

    async describeIndexes(resolvedId, _rawConnectionId, engineLabel, tableName, p) {
      const adapter = ctx.getSqlAdapter(resolvedId)
      if (!adapter) return "Adapter SQL indisponível."
      const idx = await adapter.listIndexes(resolvedId, tableName.trim(), p.schema?.trim() || null)
      return {
        text: JSON.stringify(idx, null, 2),
        structured: { tool: "db_describe_indexes", connection_id: resolvedId, engine: engineLabel, indexes: idx },
      }
    },

    async peekSample(resolvedId, rawConnectionId, resourceName, p, peekLimit) {
      const out = await ctx.sqlRead.getTableSample(
        rawConnectionId,
        resourceName.trim(),
        p.schema?.trim() || null,
        peekLimit,
        0,
      )
      return out
    },
  }
}

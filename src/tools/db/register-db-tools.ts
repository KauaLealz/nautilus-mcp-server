import type { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js"
import { z } from "zod"
import { getAppContext } from "../../context/app-context.js"
import { safeTool } from "../../shared/tool-runner.js"
import { isSqlEngineType } from "../../sql/sql-engine-types.js"
import { resolveDbEngineStrategy } from "./resolve-db-strategy.js"

function rowLimit(ctx: ReturnType<typeof getAppContext>, requested: number | null | undefined): number {
  const m = ctx.settings.max_row_limit
  const d = ctx.settings.default_row_limit
  if (requested == null || !Number.isFinite(requested)) return d
  return Math.max(1, Math.min(Math.floor(requested), m))
}

export function registerDbTools(server: McpServer): void {
  server.registerTool(
    "db_list_connections",
    {
      description:
        "Lista os connection_id configurados no ambiente (tipo de engine e read_only). Chame antes das outras db_* para saber qual id usar.",
      inputSchema: z.object({}),
    },
    async () =>
      safeTool("db_list_connections", async () => {
        const ctx = getAppContext()
        const connections = ctx.listConnections()
        if (connections.length === 0) {
          return {
            text: "Nenhuma conexão configurada. Defina variáveis DATABASES__<connection_id>__* (type, url ou host/port/user/password/database).",
            structured: { connections: [], tool: "db_list_connections" },
          }
        }
        const lines = connections.map(
          (c) => `- ${c.connection_id}: engine ${c.type}${c.read_only ? " (read_only)" : ""}`,
        )
        return {
          text: `Conexões disponíveis (${connections.length}):\n${lines.join("\n")}`,
          structured: { connections, tool: "db_list_connections" },
        }
      }),
  )

  server.registerTool(
    "db_list_resources",
    {
      description:
        "Lista recursos: tabelas (SQL), coleções (MongoDB com database) ou chaves Redis (key_pattern + cursor opcional).",
      inputSchema: {
        connection_id: z.string(),
        schema: z.string().optional(),
        database: z.string().optional(),
        key_pattern: z.string().optional(),
        cursor: z.string().optional(),
      },
    },
    async ({ connection_id, schema, database, key_pattern, cursor }) =>
      safeTool("db_list_resources", async () => {
        const ctx = getAppContext()
        const id = ctx.resolveConnectionId(connection_id)
        if (!id) return "connection_id não encontrado."
        const cfg = ctx.settings.databases[id]
        if (!cfg) return "connection_id não encontrado."
        const strat = resolveDbEngineStrategy(cfg, ctx)
        if (!strat) return "Tipo de conexão não suportado para db_list_resources."
        const cap = ctx.settings.max_row_limit
        return strat.listResources(id, connection_id, cfg.type, { schema, database, keyPattern: key_pattern, cursor }, cap)
      }),
  )

  server.registerTool(
    "db_get_metadata",
    {
      description:
        "Metadados: colunas SQL, amostra de documentos MongoDB (até 5) ou estrutura Redis (tipo + amostra).",
      inputSchema: {
        connection_id: z.string(),
        resource_name: z.string(),
        schema: z.string().optional(),
        database: z.string().optional(),
      },
    },
    async ({ connection_id, resource_name, schema, database }) =>
      safeTool("db_get_metadata", async () => {
        const ctx = getAppContext()
        const id = ctx.resolveConnectionId(connection_id)
        if (!id) return "connection_id não encontrado."
        const cfg = ctx.settings.databases[id]
        if (!cfg) return "connection_id não encontrado."
        const strat = resolveDbEngineStrategy(cfg, ctx)
        if (!strat) return "Engine não suportado."
        const lim = Math.min(5, ctx.settings.max_row_limit)
        return strat.getMetadata(id, connection_id, cfg.type, resource_name, { schema, database }, lim)
      }),
  )

  server.registerTool(
    "db_describe_indexes",
    {
      description: "Índices de tabela SQL ou coleção MongoDB.",
      inputSchema: {
        connection_id: z.string(),
        table_name: z.string(),
        schema: z.string().optional(),
        database: z.string().optional(),
      },
    },
    async ({ connection_id, table_name, schema, database }) =>
      safeTool("db_describe_indexes", async () => {
        const ctx = getAppContext()
        const id = ctx.resolveConnectionId(connection_id)
        if (!id) return "connection_id não encontrado."
        const cfg = ctx.settings.databases[id]
        if (!cfg) return "connection_id não encontrado."
        const strat = resolveDbEngineStrategy(cfg, ctx)
        if (!strat) return "Engine não suportado."
        return strat.describeIndexes(id, connection_id, cfg.type, table_name, { schema, database })
      }),
  )

  server.registerTool(
    "db_query_sql",
    {
      description: "SELECT/WITH somente leitura. Limite padrão 50, máximo 200.",
      inputSchema: {
        connection_id: z.string(),
        query: z.string(),
        limit: z.coerce.number().int().positive().optional(),
        output_format: z.enum(["table", "json", "csv"]).optional(),
      },
    },
    async ({ connection_id, query, limit, output_format }) =>
      safeTool("db_query_sql", async () => {
        const ctx = getAppContext()
        const id = ctx.resolveConnectionId(connection_id)
        if (!id) return "connection_id não encontrado."
        const cfg = ctx.settings.databases[id]
        if (!cfg || !isSqlEngineType(cfg.type)) return "db_query_sql só se aplica a conexões SQL."
        const lim = rowLimit(ctx, limit ?? null)
        const fmt = output_format ?? "json"
        const payload = await ctx.sqlRead.executeReadQueryPayload(connection_id, query, lim, fmt)
        if (typeof payload === "string") return payload
        return {
          text: payload.text,
          structured: { ...payload.structured, tool: "db_query_sql" },
        }
      }),
  )

  server.registerTool(
    "db_fetch_documents",
    {
      description: "MongoDB find com filter_json. Limite padrão 50, máximo 200.",
      inputSchema: {
        connection_id: z.string(),
        database: z.string(),
        collection: z.string(),
        filter_json: z.string().optional(),
        limit: z.coerce.number().int().positive().optional(),
        skip: z.coerce.number().int().nonnegative().optional(),
      },
    },
    async ({ connection_id, database, collection, filter_json, limit, skip }) =>
      safeTool("db_fetch_documents", async () => {
        const ctx = getAppContext()
        const lim = rowLimit(ctx, limit ?? null)
        return ctx.mongodb.findDocuments(
          connection_id,
          database,
          collection,
          filter_json ?? "{}",
          lim,
          skip ?? 0,
        )
      }),
  )

  server.registerTool(
    "db_read_cache",
    {
      description: "Redis: string, hash, list, set ou zset conforme tipo da chave.",
      inputSchema: {
        connection_id: z.string(),
        key: z.string(),
        max_elements: z.coerce.number().int().positive().optional(),
      },
    },
    async ({ connection_id, key, max_elements }) =>
      safeTool("db_read_cache", async () => {
        const ctx = getAppContext()
        const cap =
          max_elements != null && Number.isFinite(max_elements)
            ? Math.min(max_elements, ctx.settings.max_row_limit)
            : ctx.settings.default_row_limit
        const out = await ctx.redis.readStructuredSample(connection_id, key, cap)
        if (typeof out === "string") return out
        return {
          text: out.text,
          structured: { ...out.structured, tool: "db_read_cache" },
        }
      }),
  )

  server.registerTool(
    "db_peek_sample",
    {
      description: "Até 3 registros: linhas SQL ou documentos MongoDB ou amostra Redis.",
      inputSchema: {
        connection_id: z.string(),
        resource_name: z.string(),
        schema: z.string().optional(),
        database: z.string().optional(),
      },
    },
    async ({ connection_id, resource_name, schema, database }) =>
      safeTool("db_peek_sample", async () => {
        const ctx = getAppContext()
        const id = ctx.resolveConnectionId(connection_id)
        if (!id) return "connection_id não encontrado."
        const cfg = ctx.settings.databases[id]
        if (!cfg) return "connection_id não encontrado."
        const strat = resolveDbEngineStrategy(cfg, ctx)
        if (!strat) return "Engine não suportado para peek."
        const n = Math.min(3, ctx.settings.max_row_limit)
        const p = await strat.peekSample(id, connection_id, resource_name, { schema, database }, n)
        if (typeof p === "string") return p
        return {
          text: p.text,
          structured: {
            ...(typeof p.structured === "object" && p.structured ? p.structured : {}),
            tool: "db_peek_sample",
          },
        }
      }),
  )
}

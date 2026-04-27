import { MongodbAdapter } from "./mongodb.adapter.js"
import type { ConnectionServerProbe } from "../shared/types.js"
import type { Settings } from "../config/config-service.js"
import { FormatterService } from "../shared/formatter-service.js"
import type { ToolSuccessPayload } from "../shared/tool-result.js"

export class MongodbService {
  constructor(
    private readonly settings: Settings,
    private readonly adapter: MongodbAdapter | null,
    private readonly resolveConnectionId: (raw: string) => string | null,
  ) {}

  private requireMongoId(connectionId: string): string {
    const id = this.resolveConnectionId(connectionId)
    if (!id) throw new Error(`Conexão não encontrada: ${connectionId}`)
    const cfg = this.settings.databases[id]
    if (!cfg || cfg.type !== "mongodb") {
      throw new Error(`Conexão '${connectionId}' não é MongoDB.`)
    }
    if (!this.adapter) throw new Error("Nenhuma conexão MongoDB configurada.")
    return id
  }

  async testConnection(rawConnectionId: string): Promise<boolean> {
    const id = this.resolveConnectionId(rawConnectionId)
    if (!id || this.settings.databases[id]?.type !== "mongodb" || !this.adapter) return false
    return this.adapter.testConnection(id)
  }

  async probe(rawConnectionId: string): Promise<ConnectionServerProbe> {
    const id = this.resolveConnectionId(rawConnectionId)
    if (!id || this.settings.databases[id]?.type !== "mongodb" || !this.adapter) {
      return {
        ok: false,
        latency_ms: 0,
        server_version: null,
        error: "Conexão MongoDB não encontrada ou não configurada neste processo.",
      }
    }
    const r = await this.adapter.probeConnection(id)
    return {
      ok: r.ok,
      latency_ms: r.latencyMs,
      server_version: r.version,
      error: r.error,
    }
  }

  async listCollections(connectionId: string, database: string): Promise<string> {
    if (!database?.trim()) return "Erro: database não pode ser vazio."
    const id = this.requireMongoId(connectionId)
    const names = await this.adapter!.listCollections(id, database.trim())
    if (!names.length) return `Nenhuma coleção no database '${database}'.`
    return "Coleções:\n" + names.map((n) => `  - ${n}`).join("\n")
  }

  async listCollectionNames(connectionId: string, database: string): Promise<string[]> {
    if (!database?.trim()) return []
    const id = this.requireMongoId(connectionId)
    return this.adapter!.listCollections(id, database.trim())
  }

  async findDocuments(
    connectionId: string,
    database: string,
    collection: string,
    filterJson: string,
    limit: number | null | undefined,
    skip: number | null | undefined,
  ): Promise<ToolSuccessPayload> {
    if (!database?.trim()) return "Erro: database não pode ser vazio."
    if (!collection?.trim()) return "Erro: collection não pode ser vazio."
    const id = this.requireMongoId(connectionId)
    const cap = this.settings.max_row_limit
    let lim = limit ?? this.settings.default_row_limit
    if (!Number.isFinite(lim)) lim = this.settings.default_row_limit
    lim = Math.min(Math.max(1, lim), cap)
    let sk = skip ?? 0
    if (!Number.isFinite(sk)) sk = 0
    sk = Math.max(0, Math.min(Math.floor(sk), 50_000))
    let docs = await this.adapter!.findDocuments(
      id,
      database.trim(),
      collection.trim(),
      filterJson?.trim() ?? "{}",
      lim,
      sk,
    )
    if (docs.length > cap) docs = docs.slice(0, cap)
    const text = FormatterService.formatMongoDocuments(docs, Math.min(50, cap))
    return {
      text,
      structured: {
        tool: "db_fetch_documents",
        backend: "mongodb",
        operation: "find_documents",
        connection_id: id,
        database: database.trim(),
        collection: collection.trim(),
        limit: lim,
        skip: sk,
        documents: docs,
        returned_count: docs.length,
      },
    }
  }

  async aggregate(
    connectionId: string,
    database: string,
    collection: string,
    pipelineJson: string,
    limit: number | null | undefined,
    skip: number | null | undefined,
  ): Promise<ToolSuccessPayload> {
    if (!database?.trim() || !collection?.trim()) {
      return "Erro: database e collection não podem ser vazios."
    }
    const id = this.requireMongoId(connectionId)
    const cap = this.settings.max_row_limit
    let lim = limit ?? this.settings.default_row_limit
    if (!Number.isFinite(lim)) lim = this.settings.default_row_limit
    lim = Math.min(Math.max(1, lim), cap)
    let sk = skip ?? 0
    if (!Number.isFinite(sk)) sk = 0
    sk = Math.max(0, Math.min(Math.floor(sk), 50_000))
    let docs = await this.adapter!.aggregate(
      id,
      database.trim(),
      collection.trim(),
      pipelineJson.trim() || "[]",
      lim,
      sk,
    )
    if (docs.length > cap) docs = docs.slice(0, cap)
    const text = FormatterService.formatMongoDocuments(docs, Math.min(50, cap))
    return {
      text,
      structured: {
        tool: "db_fetch_documents",
        backend: "mongodb",
        operation: "aggregate",
        connection_id: id,
        database: database.trim(),
        collection: collection.trim(),
        result_limit: lim,
        skip: sk,
        documents: docs,
        returned_count: docs.length,
      },
    }
  }

  async listCollectionIndexes(
    connectionId: string,
    database: string,
    collection: string,
  ): Promise<ToolSuccessPayload> {
    if (!database?.trim() || !collection?.trim()) {
      return "database e collection são obrigatórios."
    }
    const id = this.requireMongoId(connectionId)
    const indexes = await this.adapter!.listCollectionIndexes(id, database.trim(), collection.trim())
    const text = indexes.length ? JSON.stringify(indexes, null, 2) : "Nenhum índice listado."
    return {
      text,
      structured: {
        tool: "db_describe_indexes",
        backend: "mongodb",
        connection_id: id,
        database: database.trim(),
        collection: collection.trim(),
        indexes,
      },
    }
  }
}

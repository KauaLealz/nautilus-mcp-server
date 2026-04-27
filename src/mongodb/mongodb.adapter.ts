import { MongoClient, ObjectId } from "mongodb"
import type { DatabaseConfig } from "../config/config-service.js"

const ALLOWED_AGGREGATE = new Set([
  "$match",
  "$project",
  "$group",
  "$sort",
  "$limit",
  "$skip",
  "$unwind",
  "$lookup",
  "$count",
  "$addFields",
])

const FORBIDDEN_AGGREGATE = new Set([
  "$out",
  "$merge",
  "$currentOp",
  "$listSessions",
  "$collStats",
  "$indexStats",
  "$planCacheStats",
])

function deepSanitize(value: unknown): unknown {
  if (value === null || value === undefined) return value
  if (value instanceof ObjectId) return value.toHexString()
  if (value instanceof Date) return value.toISOString()
  if (typeof Buffer !== "undefined" && Buffer.isBuffer(value)) {
    return value.toString("utf-8")
  }
  if (typeof value === "bigint") return value.toString()
  if (Array.isArray(value)) return value.map(deepSanitize)
  if (typeof value === "object") {
    const o = value as Record<string, unknown>
    if (o._bsontype === "Decimal128" && typeof (o as { toString?: () => string }).toString === "function") {
      return (o as { toString: () => string }).toString()
    }
    return Object.fromEntries(Object.entries(o).map(([k, v]) => [k, deepSanitize(v)]))
  }
  return value
}

function sanitizeDocs(docs: Record<string, unknown>[]): Record<string, unknown>[] {
  return docs.map((d) => deepSanitize(d) as Record<string, unknown>)
}

export class MongodbAdapter {
  private readonly clients = new Map<string, MongoClient>()

  constructor(
    private readonly configs: Map<string, DatabaseConfig>,
    private readonly timeoutMs: number,
  ) {}

  private async getClient(connectionId: string): Promise<MongoClient> {
    let client = this.clients.get(connectionId)
    if (!client) {
      const cfg = this.configs.get(connectionId)
      if (!cfg) throw new Error(`Conexão não encontrada: ${connectionId}`)
      client = new MongoClient(cfg.url, {
        serverSelectionTimeoutMS: this.timeoutMs,
        connectTimeoutMS: this.timeoutMs,
      })
      await client.connect()
      this.clients.set(connectionId, client)
    }
    return client
  }

  async testConnection(connectionId: string): Promise<boolean> {
    try {
      const client = await this.getClient(connectionId)
      await client.db("admin").command({ ping: 1 })
      return true
    } catch {
      return false
    }
  }

  async probeConnection(
    connectionId: string,
  ): Promise<{ ok: boolean; latencyMs: number; version: string | null; error?: string }> {
    const t0 = performance.now()
    try {
      const client = await this.getClient(connectionId)
      const bi = (await client.db("admin").command({ buildInfo: 1 })) as { version?: string }
      const latencyMs = Math.round(performance.now() - t0)
      const version = bi.version != null ? String(bi.version).slice(0, 500) : null
      return { ok: true, latencyMs, version }
    } catch (e) {
      return {
        ok: false,
        latencyMs: Math.round(performance.now() - t0),
        version: null,
        error: e instanceof Error ? e.message : String(e),
      }
    }
  }

  async listCollections(connectionId: string, database: string): Promise<string[]> {
    const client = await this.getClient(connectionId)
    const cols = await client.db(database).listCollections().toArray()
    return cols.map((c) => c.name).filter((n): n is string => Boolean(n)).sort()
  }

  async findDocuments(
    connectionId: string,
    database: string,
    collection: string,
    filterJson: string,
    limit: number,
    skip: number,
  ): Promise<Record<string, unknown>[]> {
    const client = await this.getClient(connectionId)
    let filt: Record<string, unknown> = {}
    try {
      filt = filterJson.trim() ? (JSON.parse(filterJson) as Record<string, unknown>) : {}
    } catch {
      filt = {}
    }
    const cap = Math.max(1, limit)
    const sk = Math.max(0, Math.min(skip, 50_000))
    const cursor = client
      .db(database)
      .collection(collection)
      .find(filt as never)
      .skip(sk)
      .limit(cap)
      .maxTimeMS(this.timeoutMs)
    const raw = await cursor.toArray()
    return sanitizeDocs(raw as Record<string, unknown>[])
  }

  validateAndNormalizePipeline(
    pipelineJson: string,
    limit: number,
    skip: number,
  ): Record<string, unknown>[] {
    let pipeline: unknown
    try {
      pipeline = pipelineJson.trim() ? JSON.parse(pipelineJson) : []
    } catch {
      throw new Error("Pipeline inválido: JSON malformado")
    }
    if (!Array.isArray(pipeline)) {
      throw new Error("Pipeline deve ser uma lista de stages")
    }
    const stages = pipeline as Record<string, unknown>[]
    for (let i = 0; i < stages.length; i++) {
      const stage = stages[i]
      if (!stage || typeof stage !== "object" || Array.isArray(stage)) {
        throw new Error(`Stage ${i} inválido`)
      }
      const keys = Object.keys(stage)
      if (keys.length !== 1) throw new Error(`Stage ${i} inválido`)
      const op = keys[0]!
      if (FORBIDDEN_AGGREGATE.has(op)) {
        throw new Error(`Stage '${op}' não é permitido (somente leitura)`)
      }
      if (!ALLOWED_AGGREGATE.has(op) && !op.startsWith("$")) {
        throw new Error(`Stage '${op}' desconhecido`)
      }
    }
    const copy = stages.map((s) => ({ ...s }))
    const sk = Math.max(0, Math.min(skip, 50_000))
    if (sk > 0) {
      copy.unshift({ $skip: sk })
    }
    const lastKey = copy.length ? Object.keys(copy[copy.length - 1]!)[0] : null
    const lim = Math.max(1, limit)
    if (!copy.length || lastKey !== "$limit") {
      copy.push({ $limit: lim })
    } else {
      const last = copy[copy.length - 1] as { $limit?: number }
      last.$limit = Math.min(Number(last.$limit) || lim, lim)
    }
    return copy
  }

  async aggregate(
    connectionId: string,
    database: string,
    collection: string,
    pipelineJson: string,
    limit: number,
    skip: number,
  ): Promise<Record<string, unknown>[]> {
    const client = await this.getClient(connectionId)
    const pipeline = this.validateAndNormalizePipeline(pipelineJson, limit, skip)
    const coll = client.db(database).collection(collection)
    const raw = await coll
      .aggregate(pipeline as never, { maxTimeMS: this.timeoutMs })
      .toArray()
    return sanitizeDocs(raw as Record<string, unknown>[])
  }

  async listCollectionIndexes(
    connectionId: string,
    database: string,
    collection: string,
  ): Promise<Record<string, unknown>[]> {
    const client = await this.getClient(connectionId)
    const idx = await client.db(database).collection(collection).indexes()
    return idx as Record<string, unknown>[]
  }
}

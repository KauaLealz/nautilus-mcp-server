import { createClient, type RedisClientType } from "redis"
import type { DatabaseConfig } from "../config/config-service.js"

export class RedisAdapter {
  static readonly DEFAULT_SCAN_MAX_KEYS = 200
  static readonly ABSOLUTE_MAX_SCAN_KEYS = 2000
  static readonly MAX_MGET = 50

  private readonly clients = new Map<string, RedisClientType>()

  constructor(
    private readonly configs: Map<string, DatabaseConfig>,
    private readonly timeoutMs: number,
  ) {}

  private async getClient(connectionId: string): Promise<RedisClientType> {
    let client = this.clients.get(connectionId)
    if (!client) {
      const cfg = this.configs.get(connectionId)
      if (!cfg) throw new Error(`Conexão não encontrada: ${connectionId}`)
      const ms = this.timeoutMs
      client = createClient({
        url: cfg.url,
        socket: { connectTimeout: ms },
      })
      client.on("error", () => void 0)
      await client.connect()
      this.clients.set(connectionId, client)
    }
    return client
  }

  async testConnection(connectionId: string): Promise<boolean> {
    try {
      const client = await this.getClient(connectionId)
      const pong = await client.ping()
      return pong === "PONG"
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
      await client.ping()
      const info = await client.info("server")
      const latencyMs = Math.round(performance.now() - t0)
      const m = /^redis_version:([^\r\n]+)/m.exec(info)
      const version = m?.[1]?.trim() ? m[1]!.trim().slice(0, 200) : null
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

  async getKey(connectionId: string, key: string): Promise<string | null> {
    const client = await this.getClient(connectionId)
    const value = await client.get(key)
    return value ?? null
  }

  async scanKeys(
    connectionId: string,
    pattern: string,
    maxKeys: number,
    startCursor: string | null | undefined,
  ): Promise<{ keys: string[]; nextCursor: string | null; truncated: boolean }> {
    const client = await this.getClient(connectionId)
    const cap = Math.max(1, Math.min(maxKeys, RedisAdapter.ABSOLUTE_MAX_SCAN_KEYS))
    const pat = pattern?.trim() || "*"
    let cursor = startCursor && startCursor !== "0" ? startCursor : "0"
    const out: string[] = []
    let truncated = false
    do {
      const reply = await client.scan(cursor, { MATCH: pat, COUNT: Math.min(100, cap - out.length + 20) })
      cursor = String(reply.cursor)
      for (const k of reply.keys) {
        out.push(k)
        if (out.length >= cap) {
          truncated = cursor !== "0"
          break
        }
      }
      if (out.length >= cap) break
    } while (cursor !== "0")
    return {
      keys: out,
      nextCursor: truncated ? cursor : null,
      truncated,
    }
  }

  async keyType(connectionId: string, key: string): Promise<string> {
    const client = await this.getClient(connectionId)
    return await client.type(key)
  }

  async keyTtl(connectionId: string, key: string): Promise<number> {
    const client = await this.getClient(connectionId)
    return await client.ttl(key)
  }

  async mget(connectionId: string, keysList: string[]): Promise<(string | null)[]> {
    const client = await this.getClient(connectionId)
    const slice = keysList.slice(0, RedisAdapter.MAX_MGET)
    if (!slice.length) return []
    return await client.mGet(slice)
  }

  async readStructuredSample(
    connectionId: string,
    key: string,
    maxElements: number,
  ): Promise<Record<string, unknown>> {
    const client = await this.getClient(connectionId)
    const t = await client.type(key)
    const cap = Math.max(1, Math.min(maxElements, 200))
    if (t === "string") {
      const v = await client.get(key)
      return { redis_type: "string", value: v }
    }
    if (t === "hash") {
      const h = await client.hGetAll(key)
      const entries = Object.entries(h).slice(0, cap)
      return {
        redis_type: "hash",
        fields: Object.fromEntries(entries),
        truncated: Object.keys(h).length > cap,
        field_count: Object.keys(h).length,
      }
    }
    if (t === "list") {
      const len = await client.lLen(key)
      const slice = await client.lRange(key, 0, cap - 1)
      return { redis_type: "list", length: len, elements: slice, truncated: len > cap }
    }
    if (t === "set") {
      const members = await client.sMembers(key)
      return {
        redis_type: "set",
        cardinality: members.length,
        sample: members.slice(0, cap),
        truncated: members.length > cap,
      }
    }
    if (t === "zset") {
      const len = await client.zCard(key)
      const members = await client.zRange(key, 0, cap - 1)
      return { redis_type: "zset", cardinality: len, members, truncated: len > cap }
    }
    return { redis_type: t }
  }
}

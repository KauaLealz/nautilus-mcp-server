import { RedisAdapter } from "./redis.adapter.js"
import type { ConnectionServerProbe, RedisKeysPage } from "../shared/types.js"
import type { Settings } from "../config/config-service.js"
import { FormatterService } from "../shared/formatter-service.js"
import type { ToolSuccessPayload } from "../shared/tool-result.js"

export class RedisService {
  constructor(
    private readonly settings: Settings,
    private readonly adapter: RedisAdapter | null,
    private readonly resolveConnectionId: (raw: string) => string | null,
  ) {}

  private requireRedisId(connectionId: string): string {
    const id = this.resolveConnectionId(connectionId)
    if (!id) throw new Error(`Conexão não encontrada: ${connectionId}`)
    const cfg = this.settings.databases[id]
    if (!cfg || cfg.type !== "redis") {
      throw new Error(`Conexão '${connectionId}' não é Redis.`)
    }
    if (!this.adapter) throw new Error("Nenhuma conexão Redis configurada.")
    return id
  }

  async testConnection(rawConnectionId: string): Promise<boolean> {
    const id = this.resolveConnectionId(rawConnectionId)
    if (!id || this.settings.databases[id]?.type !== "redis" || !this.adapter) return false
    return this.adapter.testConnection(id)
  }

  async probe(rawConnectionId: string): Promise<ConnectionServerProbe> {
    const id = this.resolveConnectionId(rawConnectionId)
    if (!id || this.settings.databases[id]?.type !== "redis" || !this.adapter) {
      return {
        ok: false,
        latency_ms: 0,
        server_version: null,
        error: "Conexão Redis não encontrada ou não configurada neste processo.",
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

  async getKey(connectionId: string, key: string): Promise<string> {
    if (!key?.trim()) return "Erro: key não pode ser vazia."
    const id = this.requireRedisId(connectionId)
    const value = await this.adapter!.getKey(id, key.trim())
    if (value === null) return `Chave '${key}' não encontrada ou sem valor.`
    return value
  }

  async scanKeysRaw(
    connectionId: string,
    pattern: string,
    maxKeys: number,
    cursor: string | null,
  ): Promise<{ keys: string[]; nextCursor: string | null; truncated: boolean }> {
    const id = this.requireRedisId(connectionId)
    return this.adapter!.scanKeys(id, pattern, maxKeys, cursor)
  }

  async scanKeys(
    connectionId: string,
    pattern: string | null | undefined,
    maxKeys: number | null | undefined,
    cursor: string | null | undefined,
  ): Promise<ToolSuccessPayload> {
    const id = this.requireRedisId(connectionId)
    const pat = pattern?.trim() || "*"
    let mk = maxKeys ?? RedisAdapter.DEFAULT_SCAN_MAX_KEYS
    if (!Number.isFinite(mk)) mk = RedisAdapter.DEFAULT_SCAN_MAX_KEYS
    mk = Math.max(1, Math.min(Math.floor(mk), RedisAdapter.ABSOLUTE_MAX_SCAN_KEYS))
    const page = await this.adapter!.scanKeys(id, pat, mk, cursor?.trim() || null)
    const block: RedisKeysPage = {
      keys: page.keys,
      next_cursor: page.nextCursor,
      truncated: page.truncated,
      pattern: pat,
      max_keys: mk,
    }
    let text: string
    if (!page.keys.length) {
      text = `Nenhuma chave encontrada para o pattern '${pat}'.`
    } else {
      const tail = page.truncated
        ? `\n\n(truncado: há mais chaves; use cursor='${page.nextCursor}' e o mesmo pattern/max_keys para continuar)`
        : ""
      text = `Chaves (máx. ${mk}):\n${FormatterService.formatRedisKeys(page.keys)}${tail}`
    }
    return {
      text,
      structured: {
        tool: "nosql",
        backend: "redis",
        operation: "keys",
        connection_id: id,
        ...block,
      },
    }
  }

  async keyType(connectionId: string, key: string): Promise<string> {
    const id = this.requireRedisId(connectionId)
    const t = await this.adapter!.keyType(id, key.trim())
    return `Tipo da chave '${key}': ${t}`
  }

  async keyTtl(connectionId: string, key: string): Promise<string> {
    const id = this.requireRedisId(connectionId)
    const ttl = await this.adapter!.keyTtl(id, key.trim())
    if (ttl === -2) return `Chave '${key}' não existe.`
    if (ttl === -1) return `Chave '${key}' não tem expiração.`
    return `TTL da chave '${key}': ${ttl} segundos.`
  }

  async mget(connectionId: string, keys: string): Promise<string> {
    const id = this.requireRedisId(connectionId)
    const keyList = keys
      .split(",")
      .map((k) => k.trim())
      .filter(Boolean)
      .slice(0, RedisAdapter.MAX_MGET)
    if (!keyList.length) return "Nenhuma chave fornecida."
    const values = await this.adapter!.mget(id, keyList)
    const lines: string[] = ["Valores:"]
    for (let i = 0; i < keyList.length; i++) {
      const v = values[i]
      lines.push(`  ${keyList[i]}: ${v ?? "(não existe)"}`)
    }
    return lines.join("\n")
  }

  async readStructuredSample(
    connectionId: string,
    key: string,
    maxElements: number,
  ): Promise<ToolSuccessPayload> {
    if (!key?.trim()) return "key é obrigatória."
    const id = this.requireRedisId(connectionId)
    const cap = Math.max(1, Math.min(maxElements, this.settings.max_row_limit))
    const data = await this.adapter!.readStructuredSample(id, key.trim(), cap)
    return {
      text: JSON.stringify(data, null, 2),
      structured: {
        tool: "db_read_cache",
        connection_id: id,
        key: key.trim(),
        payload: data,
      },
    }
  }
}

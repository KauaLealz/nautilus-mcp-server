import { config as loadDotenv } from "dotenv"

export type DatabaseConfig = {
  type: string
  url: string
  read_only: boolean
}

export type Settings = {
  query_max_length: number
  default_row_limit: number
  max_row_limit: number
  query_timeout_ms: number
  read_only_mode: boolean
  databases: Record<string, DatabaseConfig>
}

const SUPPORTED = new Set([
  "postgresql",
  "mysql",
  "mariadb",
  "sqlite",
  "sqlserver",
  "oracle",
  "mongodb",
  "redis",
])

export function parseHostAndPort(hostRaw: string, portFallback: string): { host: string; port: string } {
  const h = hostRaw.trim()
  if (!h) return { host: "", port: portFallback.trim() }
  if (h.startsWith("[")) {
    const m = h.match(/^\[([^\]]+)\](?::(\d+))?$/)
    if (m) {
      return { host: `[${m[1]}]`, port: (m[2] ?? portFallback).trim() }
    }
    return { host: h, port: portFallback.trim() }
  }
  const idx = h.lastIndexOf(":")
  if (idx > 0) {
    const tail = h.slice(idx + 1)
    if (/^\d{1,5}$/.test(tail)) {
      return { host: h.slice(0, idx), port: tail }
    }
  }
  return { host: h, port: portFallback.trim() }
}

function defaultPort(dbType: string): string {
  const ports: Record<string, string> = {
    postgresql: "5432",
    mysql: "3306",
    sqlserver: "1433",
    oracle: "1521",
    mongodb: "27017",
    redis: "6379",
  }
  return ports[dbType] ?? ""
}

function odbcEscape(value: string): string {
  return value.replaceAll("}", "}}")
}

function buildUrlFromComponents(
  dbType: string,
  user: string,
  password: string,
  host: string,
  port: string,
  database: string,
): string {
  const safeUser = encodeURIComponent(user)
  const safePassword = encodeURIComponent(password)
  if (dbType === "postgresql") {
    const path = database.trim().replace(/^\//, "") || "postgres"
    return `postgresql://${safeUser}:${safePassword}@${host}:${port}/${path}`
  }
  if (dbType === "mysql") {
    const path = database.trim().replace(/^\//, "")
    return path
      ? `mysql://${safeUser}:${safePassword}@${host}:${port}/${path}`
      : `mysql://${safeUser}:${safePassword}@${host}:${port}`
  }
  if (dbType === "oracle") {
    const service = database.trim().replace(/^\//, "") || "ORCL"
    return `oracle://${safeUser}:${safePassword}@${host}:${port}/${service}`
  }
  if (dbType === "mongodb") {
    const path = database.trim().replace(/^\//, "")
    let base: string
    if (safeUser && safePassword) {
      base = `mongodb://${safeUser}:${safePassword}@${host}:${port}`
    } else if (safeUser) {
      base = `mongodb://${safeUser}@${host}:${port}`
    } else if (safePassword) {
      base = `mongodb://:${safePassword}@${host}:${port}`
    } else {
      base = `mongodb://${host}:${port}`
    }
    return path ? `${base}/${path}` : base
  }
  if (dbType === "redis") {
    const dbNum = database.trim().replace(/^\//, "") || "0"
    if (safeUser && safePassword) {
      return `redis://${safeUser}:${safePassword}@${host}:${port}/${dbNum}`
    }
    if (safePassword) {
      return `redis://:${safePassword}@${host}:${port}/${dbNum}`
    }
    return `redis://${host}:${port}/${dbNum}`
  }
  if (dbType === "sqlserver") {
    const driver = "ODBC Driver 17 for SQL Server"
    const u = odbcEscape(user)
    const p = odbcEscape(password)
    const dbName = database || "master"
    return `Server=${host},${port};Database=${dbName};User Id=${u};Password=${p};Encrypt=true;TrustServerCertificate=true`
  }
  return ""
}

function normalizeDbType(t: string): string {
  if (t === "mariadb") return "mysql"
  return t
}

function loadDatabasesFromEnv(): Record<string, DatabaseConfig> {
  const prefix = "DATABASES__"
  const raw: Record<string, Record<string, string>> = {}
  for (const [key, value] of Object.entries(process.env)) {
    if (!key.startsWith(prefix) || !value?.trim()) continue
    const rest = key.slice(prefix.length).toLowerCase()
    const parts = rest.split("__")
    if (parts.length !== 2) continue
    const connId = parts[0]!
    const attr = parts[1]!
    if (!raw[connId]) raw[connId] = {}
    raw[connId]![attr] = value.trim()
  }
  const result: Record<string, DatabaseConfig> = {}
  for (const [connId, data] of Object.entries(raw)) {
    const typeVal = data.type
    if (!typeVal) continue
    const rawT = typeVal.toLowerCase()
    if (!SUPPORTED.has(rawT)) continue
    const t = normalizeDbType(rawT)
    let urlVal = (data.url ?? "").trim()
    const user = (data.user ?? "").trim()
    const password = (data.password ?? "").trim()
    const hostRaw = (data.host ?? "").trim()
    const portFallback = (data.port ?? defaultPort(t === "mysql" ? "mysql" : t)).trim()
    const { host, port } = parseHostAndPort(hostRaw, portFallback || defaultPort(t))
    const database = (data.database ?? data.db ?? "").trim()
    if (t === "sqlite") {
      if (!urlVal) {
        const p = (data.path ?? data.file ?? "").trim()
        if (p) urlVal = p.startsWith("file:") || p === ":memory:" ? p : `file:${p.replace(/\\/g, "/")}`
      }
      if (!urlVal) continue
    } else if (host) {
      const buildType =
        t === "mysql"
          ? "mysql"
          : t === "postgresql"
            ? "postgresql"
            : t === "oracle"
              ? "oracle"
              : t === "mongodb"
                ? "mongodb"
                : t === "redis"
                  ? "redis"
                  : t === "sqlserver"
                    ? "sqlserver"
                    : ""
      if (buildType) urlVal = buildUrlFromComponents(buildType, user, password, host, port, database)
    }
    if (!urlVal) continue
    const readOnly = !["false", "0", "no"].includes((data.read_only ?? "true").toLowerCase())
    result[connId] = { type: t, url: urlVal, read_only: readOnly }
  }
  return result
}

function numEnv(name: string, fallback: number): number {
  const v = process.env[name]
  if (v === undefined || v === "") return fallback
  const n = Number(v)
  return Number.isFinite(n) ? n : fallback
}

function boolEnv(name: string, fallback: boolean): boolean {
  const v = process.env[name]?.trim().toLowerCase()
  if (v === undefined || v === "") return fallback
  if (["false", "0", "no"].includes(v)) return false
  if (["true", "1", "yes"].includes(v)) return true
  return fallback
}

export function loadSettings(): Settings {
  loadDotenv()
  let maxRowLimit = numEnv("NAUTILUS_MAX_ROW_LIMIT", 0)
  if (maxRowLimit <= 0) maxRowLimit = numEnv("NAUTILUS_MAX_ROWS", 200)
  if (maxRowLimit <= 0) maxRowLimit = 200
  maxRowLimit = Math.min(Math.max(1, maxRowLimit), 10_000)
  let defaultRowLimit = numEnv("NAUTILUS_DEFAULT_ROW_LIMIT", 50)
  if (defaultRowLimit <= 0) defaultRowLimit = 50
  defaultRowLimit = Math.min(Math.max(1, defaultRowLimit), maxRowLimit)
  let timeoutMs = numEnv("NAUTILUS_QUERY_TIMEOUT_MS", 0)
  if (timeoutMs <= 0) {
    const sec = numEnv("NAUTILUS_QUERY_TIMEOUT_SECONDS", 0)
    timeoutMs = sec > 0 ? Math.round(sec * 1000) : 5000
  }
  timeoutMs = Math.min(Math.max(1000, timeoutMs), 600_000)
  return {
    query_max_length: numEnv("NAUTILUS_QUERY_MAX_LENGTH", 2000),
    default_row_limit: defaultRowLimit,
    max_row_limit: maxRowLimit,
    query_timeout_ms: timeoutMs,
    read_only_mode: boolEnv("NAUTILUS_READ_ONLY_MODE", true),
    databases: loadDatabasesFromEnv(),
  }
}

let cached: Settings | null = null

export function getSettings(): Settings {
  if (!cached) cached = loadSettings()
  return cached
}

export function resetSettingsCache(): void {
  cached = null
}

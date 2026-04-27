import type { ConnectionInfo } from "../shared/types.js"
import type { SqlEngineAdapter } from "../sql/sql-engine.port.js"
import { MongodbAdapter } from "../mongodb/mongodb.adapter.js"
import { RedisAdapter } from "../redis/redis.adapter.js"
import { MssqlAdapter } from "../sql/adapters/mssql.adapter.js"
import { MysqlAdapter } from "../sql/adapters/mysql.adapter.js"
import { OracleAdapter } from "../sql/adapters/oracle.adapter.js"
import { PostgresAdapter } from "../sql/adapters/postgres.adapter.js"
import { SqliteAdapter } from "../sql/adapters/sqlite.adapter.js"
import { getSettings, type DatabaseConfig, type Settings } from "../config/config-service.js"
import { MongodbService } from "../mongodb/mongodb.service.js"
import { RedisService } from "../redis/redis.service.js"
import { SqlQueryValidator } from "../sql/sql-query.validator.js"
import { SqlReadService } from "../sql/sql-read.service.js"
import { isSqlEngineType } from "../sql/sql-engine-types.js"

export type AppContext = {
  settings: Settings
  sqlRead: SqlReadService
  mongodb: MongodbService
  redis: RedisService
  resolveConnectionId(raw: string): string | null
  listConnections(): ConnectionInfo[]
  getSqlAdapter(resolvedId: string): SqlEngineAdapter | null
}

function configsByType(settings: Settings, type: string): Map<string, DatabaseConfig> {
  const m = new Map<string, DatabaseConfig>()
  for (const [id, cfg] of Object.entries(settings.databases)) {
    if (cfg.type === type) m.set(id, cfg)
  }
  return m
}

let ctx: AppContext | null = null
let contextOverride: AppContext | null = null

export function setAppContextOverride(override: AppContext | null): void {
  contextOverride = override
}

export function getAppContext(): AppContext {
  if (contextOverride) return contextOverride
  if (!ctx) {
    const settings = getSettings()
    const pgMap = configsByType(settings, "postgresql")
    const mysqlMap = configsByType(settings, "mysql")
    const sqliteMap = configsByType(settings, "sqlite")
    const mssqlMap = configsByType(settings, "sqlserver")
    const oracleMap = configsByType(settings, "oracle")
    const mongoMap = configsByType(settings, "mongodb")
    const redisMap = configsByType(settings, "redis")
    const postgresAdapter = pgMap.size ? new PostgresAdapter(pgMap) : null
    const mysqlAdapter = mysqlMap.size ? new MysqlAdapter(mysqlMap) : null
    const sqliteAdapter = sqliteMap.size ? new SqliteAdapter(sqliteMap) : null
    const mssqlAdapter = mssqlMap.size ? new MssqlAdapter(mssqlMap) : null
    const oracleAdapter = oracleMap.size ? new OracleAdapter(oracleMap) : null
    const mongoAdapter = mongoMap.size ? new MongodbAdapter(mongoMap, settings.query_timeout_ms) : null
    const redisAdapter = redisMap.size ? new RedisAdapter(redisMap, settings.query_timeout_ms) : null
    const sqlValidator = new SqlQueryValidator({
      maxLength: settings.query_max_length,
      maxRowsCap: settings.max_row_limit,
    })
    const resolveConnectionId = (raw: string): string | null => {
      const t = raw?.trim()
      if (!t) return null
      const key = Object.keys(settings.databases).find((k) => k.toLowerCase() === t.toLowerCase())
      return key ?? null
    }
    const getSqlAdapter = (resolvedId: string): SqlEngineAdapter | null => {
      const cfg = settings.databases[resolvedId]
      if (!cfg || !isSqlEngineType(cfg.type)) return null
      if (cfg.type === "postgresql" && postgresAdapter) return postgresAdapter
      if (cfg.type === "mysql" && mysqlAdapter) return mysqlAdapter
      if (cfg.type === "sqlite" && sqliteAdapter) return sqliteAdapter
      if (cfg.type === "sqlserver" && mssqlAdapter) return mssqlAdapter
      if (cfg.type === "oracle" && oracleAdapter) return oracleAdapter
      return null
    }
    const listConnections = (): ConnectionInfo[] => {
      const out: ConnectionInfo[] = []
      for (const [connection_id, c] of Object.entries(settings.databases)) {
        out.push({ connection_id, type: c.type, read_only: c.read_only })
      }
      return out.sort((a, b) => a.connection_id.localeCompare(b.connection_id))
    }
    ctx = {
      settings,
      sqlRead: new SqlReadService(
        settings,
        postgresAdapter,
        mysqlAdapter,
        sqliteAdapter,
        mssqlAdapter,
        oracleAdapter,
        sqlValidator,
        resolveConnectionId,
      ),
      mongodb: new MongodbService(settings, mongoAdapter, resolveConnectionId),
      redis: new RedisService(settings, redisAdapter, resolveConnectionId),
      resolveConnectionId,
      listConnections,
      getSqlAdapter,
    }
  }
  return ctx
}

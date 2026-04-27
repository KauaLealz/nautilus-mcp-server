import type { DatabaseConfig } from "../../config/config-service.js"
import type { AppContext } from "../../context/app-context.js"
import { isSqlEngineType } from "../../sql/sql-engine-types.js"
import type { DbEngineStrategy } from "./db-engine-strategy.js"
import { createMongoDbStrategy } from "./strategies/mongo-db.strategy.js"
import { createRedisDbStrategy } from "./strategies/redis-db.strategy.js"
import { createSqlDbStrategy } from "./strategies/sql-db.strategy.js"

export function resolveDbEngineStrategy(cfg: DatabaseConfig, ctx: AppContext): DbEngineStrategy | null {
  if (isSqlEngineType(cfg.type)) return createSqlDbStrategy(ctx)
  if (cfg.type === "mongodb") return createMongoDbStrategy(ctx)
  if (cfg.type === "redis") return createRedisDbStrategy(ctx)
  return null
}

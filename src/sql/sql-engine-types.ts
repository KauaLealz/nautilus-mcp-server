export const SQL_ENGINE_TYPES = new Set([
  "postgresql",
  "mysql",
  "sqlite",
  "sqlserver",
  "oracle",
])

export function isSqlEngineType(t: string): boolean {
  return SQL_ENGINE_TYPES.has(t)
}

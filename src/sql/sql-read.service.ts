import { MssqlAdapter } from "./adapters/mssql.adapter.js"
import { MysqlAdapter } from "./adapters/mysql.adapter.js"
import { OracleAdapter } from "./adapters/oracle.adapter.js"
import { PostgresAdapter } from "./adapters/postgres.adapter.js"
import { SqliteAdapter } from "./adapters/sqlite.adapter.js"
import type { SqlEngineAdapter } from "./sql-engine.port.js"
import type { Settings } from "../config/config-service.js"
import { FormatterService } from "../shared/formatter-service.js"
import { clipQueryResult } from "../shared/query-result-clip.js"
import type { ToolSuccessPayload } from "../shared/tool-result.js"
import { SqlQueryValidator } from "./sql-query.validator.js"
import { isSqlEngineType } from "./sql-engine-types.js"

export class SqlReadService {
  constructor(
    private readonly settings: Settings,
    private readonly postgresAdapter: PostgresAdapter | null,
    private readonly mysqlAdapter: MysqlAdapter | null,
    private readonly sqliteAdapter: SqliteAdapter | null,
    private readonly mssqlAdapter: MssqlAdapter | null,
    private readonly oracleAdapter: OracleAdapter | null,
    private readonly validator: SqlQueryValidator,
    private readonly resolveConnectionId: (raw: string) => string | null,
  ) {}

  private adapterForType(t: string): SqlEngineAdapter | null {
    if (t === "postgresql") return this.postgresAdapter
    if (t === "mysql") return this.mysqlAdapter
    if (t === "sqlite") return this.sqliteAdapter
    if (t === "sqlserver") return this.mssqlAdapter
    if (t === "oracle") return this.oracleAdapter
    return null
  }

  private requireSql(connectionId: string): { id: string; adapter: SqlEngineAdapter } {
    const id = this.resolveConnectionId(connectionId)
    if (!id) throw new Error(`Conexão não encontrada: ${connectionId}`)
    const cfg = this.settings.databases[id]
    if (!cfg) throw new Error(`Conexão não encontrada: ${connectionId}`)
    if (!isSqlEngineType(cfg.type)) {
      throw new Error(`Conexão '${connectionId}' não é SQL relacional. Use db_fetch_documents ou db_read_cache.`)
    }
    const adapter = this.adapterForType(cfg.type)
    if (!adapter) throw new Error(`Nenhum adapter configurado para ${cfg.type}.`)
    return { id, adapter }
  }

  private timeoutSeconds(): number {
    return Math.max(1, Math.ceil(this.settings.query_timeout_ms / 1000))
  }

  async executeReadQueryPayload(
    connectionId: string,
    query: string,
    maxRows: number | null | undefined,
    outputFormat: "table" | "json" | "csv",
  ): Promise<ToolSuccessPayload> {
    const { id, adapter } = this.requireSql(connectionId)
    if (!query?.trim()) return "Erro: query não pode ser vazia."
    this.validator.sanitizeOrRaise(query)
    const cap = this.settings.max_row_limit
    let want = this.settings.default_row_limit
    if (maxRows != null && Number.isFinite(maxRows)) want = Math.min(maxRows, cap)
    const effective = Math.max(1, Math.min(want, cap))
    let result = await adapter.executeReadOnly(
      id,
      query.trim(),
      effective,
      this.timeoutSeconds(),
    )
    result = clipQueryResult(result, cap)
    const baseStructured = {
      tool: "db_query_sql",
      connection_id: id,
      columns: result.columns,
      rows: result.rows,
      row_count: result.row_count,
    }
    if (outputFormat === "json") {
      return {
        text: FormatterService.queryResultToJson(result),
        structured: baseStructured,
      }
    }
    if (outputFormat === "csv") {
      return {
        text: FormatterService.queryResultToCsv(result),
        structured: baseStructured,
      }
    }
    return {
      text: FormatterService.formatQueryResult(result),
      structured: baseStructured,
    }
  }

  async getTableSample(
    connectionId: string,
    tableName: string,
    schema: string | null | undefined,
    limit: number | null | undefined,
    offset: number | null | undefined,
  ): Promise<ToolSuccessPayload> {
    const { id, adapter } = this.requireSql(connectionId)
    const cap = this.settings.max_row_limit
    let base = limit ?? 5
    if (!Number.isFinite(base)) base = 5
    const lim = Math.min(Math.max(1, base), cap)
    let off = offset ?? 0
    if (!Number.isFinite(off)) off = 0
    off = Math.max(0, Math.min(Math.floor(off), 1_000_000))
    let result = await adapter.getTableSample(
      id,
      tableName.trim(),
      schema?.trim() || null,
      lim,
      off,
    )
    result = clipQueryResult(result, lim)
    const text = FormatterService.formatQueryResult(result, lim)
    return {
      text,
      structured: {
        tool: "table_sample",
        operation: "table_sample",
        connection_id: id,
        table_name: tableName.trim(),
        schema: schema?.trim() || null,
        limit: lim,
        offset: off,
        columns: result.columns,
        rows: result.rows,
        row_count: result.row_count,
      },
    }
  }
}

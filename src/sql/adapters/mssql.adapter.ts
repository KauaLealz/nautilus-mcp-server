import sql from "mssql"
import type { SchemaExport, SqlEngineAdapter } from "../sql-engine.port.js"
import type { DatabaseConfig } from "../../config/config-service.js"
import type {
  ColumnInfo,
  ColumnStat,
  ForeignKeyInfo,
  IndexInfo,
  QueryResult,
  SchemaTableSummary,
  TableInfo,
  TableRelationship,
  ViewInfo,
} from "../../shared/types.js"

function bId(s: string): string {
  return `[${String(s).replaceAll("]", "]]")}]`
}

function rowToArray(columns: string[], row: Record<string, unknown>): unknown[] {
  return columns.map((c) => row[c])
}

function recordsetToQueryResult(rows: Record<string, unknown>[]): QueryResult {
  if (!rows.length) return { columns: [], rows: [], row_count: 0 }
  const columns = Object.keys(rows[0]!)
  const out = rows.map((r) => rowToArray(columns, r))
  return { columns, rows: out, row_count: out.length }
}

const SYS_SCHEMAS = "('INFORMATION_SCHEMA','sys','guest')"

export class MssqlAdapter implements SqlEngineAdapter {
  private readonly configs: Map<string, DatabaseConfig>
  private readonly pools = new Map<string, any>()

  constructor(configs: Map<string, DatabaseConfig>) {
    this.configs = configs
  }

  private async getPool(connectionId: string): Promise<any> {
    let p = this.pools.get(connectionId)
    if (!p) {
      const cfg = this.configs.get(connectionId)
      if (!cfg) throw new Error(`Conexão não encontrada: ${connectionId}`)
      p = new sql.ConnectionPool(cfg.url)
      await p.connect()
      this.pools.set(connectionId, p)
    }
    return p
  }

  async testConnection(connectionId: string): Promise<boolean> {
    try {
      const pool = await this.getPool(connectionId)
      const r = await pool.request().query("SELECT 1 AS ok")
      return (r.recordset?.length ?? 0) > 0
    } catch {
      return false
    }
  }

  async probeConnection(
    connectionId: string,
  ): Promise<{ ok: boolean; latencyMs: number; version: string | null; error?: string }> {
    const t0 = performance.now()
    try {
      const pool = await this.getPool(connectionId)
      const r = await pool.request().query("SELECT @@VERSION AS v")
      const latencyMs = Math.round(performance.now() - t0)
      const raw = r.recordset?.[0] && (r.recordset[0] as Record<string, unknown>)["v"]
      const version = raw != null ? String(raw).split("\n")[0]!.trim().slice(0, 500) : null
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

  async executeReadOnly(
    connectionId: string,
    query: string,
    maxRows: number,
    timeoutSeconds: number,
  ): Promise<QueryResult> {
    const pool = await this.getPool(connectionId)
    let q = query.trim().replace(/;\s*$/, "")
    const head = q.split("--")[0] ?? q
    if (
      !/\bTOP\s*\(\s*\d+\s*\)/i.test(head) &&
      !/\bFETCH\s+NEXT\s+\d+\s+ROWS\s+ONLY/i.test(head)
    ) {
      q = `SELECT TOP (${maxRows}) * FROM (${q}) AS _nautilus_sub`
    }
    const req = pool.request()
    req.timeout = Math.min(Math.max(1000, timeoutSeconds * 1000), 600_000)
    const r = await req.query(q)
    const rows = (r.recordset ?? []) as Record<string, unknown>[]
    return recordsetToQueryResult(rows)
  }

  async listTables(connectionId: string, schema?: string | null): Promise<[string, string][]> {
    const pool = await this.getPool(connectionId)
    if (schema?.trim()) {
      const r = await pool
        .request()
        .input("sch", sql.NVarChar, schema.trim())
        .query(
          `SELECT TABLE_SCHEMA AS table_schema, TABLE_NAME AS table_name FROM INFORMATION_SCHEMA.TABLES
           WHERE TABLE_TYPE='BASE TABLE' AND TABLE_SCHEMA = @sch ORDER BY 1,2`,
        )
      return (r.recordset as { table_schema: string; table_name: string }[]).map((x) => [
        String(x.table_schema),
        String(x.table_name),
      ])
    }
    const r = await pool.request().query(
      `SELECT TABLE_SCHEMA AS table_schema, TABLE_NAME AS table_name FROM INFORMATION_SCHEMA.TABLES
       WHERE TABLE_TYPE='BASE TABLE' AND TABLE_SCHEMA NOT IN ${SYS_SCHEMAS} ORDER BY 1,2`,
    )
    return (r.recordset as { table_schema: string; table_name: string }[]).map((x) => [
      String(x.table_schema),
      String(x.table_name),
    ])
  }

  async describeTable(
    connectionId: string,
    tableName: string,
    schema?: string | null,
  ): Promise<TableInfo | null> {
    const pool = await this.getPool(connectionId)
    const sch = schema?.trim() ?? "dbo"
    const r = await pool
      .request()
      .input("tn", sql.NVarChar, tableName)
      .input("sc", sql.NVarChar, sch)
      .query(
        `SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, DATA_TYPE, IS_NULLABLE
         FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = @tn AND TABLE_SCHEMA = @sc ORDER BY ORDINAL_POSITION`,
      )
    const rows = r.recordset as Record<string, unknown>[] | undefined
    if (!rows?.length) return null
    const row0 = rows[0]!
    const columns: ColumnInfo[] = rows.map((x) => ({
      name: String(x.column_name),
      data_type: String(x.data_type),
      nullable: String(x.is_nullable).toUpperCase() === "YES",
    }))
    return {
      schema_name: String(row0.table_schema),
      table_name: String(row0.table_name),
      columns,
    }
  }

  async listDatabases(connectionId: string): Promise<string[]> {
    const pool = await this.getPool(connectionId)
    const r = await pool.request().query(`SELECT name FROM sys.databases WHERE name NOT IN ('master','tempdb','model','msdb') ORDER BY name`)
    return (r.recordset as { name: string }[]).map((x) => String(x.name))
  }

  async getTableSample(
    connectionId: string,
    tableName: string,
    schema: string | null | undefined,
    limit: number,
    offset: number,
  ): Promise<QueryResult> {
    const pool = await this.getPool(connectionId)
    const sch = schema?.trim() || "dbo"
    const lim = Math.max(1, Math.min(limit, 50_000))
    const off = Math.max(0, Math.min(offset, 1_000_000))
    const from = `${bId(sch)}.${bId(tableName)}`
    const q = `SELECT * FROM ${from} ORDER BY (SELECT NULL) OFFSET ${off} ROWS FETCH NEXT ${lim} ROWS ONLY`
    const r = await pool.request().query(q)
    return recordsetToQueryResult((r.recordset ?? []) as Record<string, unknown>[])
  }

  async getSchemaSummary(
    connectionId: string,
    schema: string | null | undefined,
    includeRowCount: boolean,
  ): Promise<SchemaTableSummary[]> {
    const tables = await this.listTables(connectionId, schema)
    const out: SchemaTableSummary[] = []
    const pool = await this.getPool(connectionId)
    for (const [sch, tbl] of tables) {
      let rowCount: number | null = null
      if (includeRowCount) {
        try {
          const r = await pool.request().query(`SELECT count_big(*) AS c FROM ${bId(sch)}.${bId(tbl)}`)
          rowCount = Number((r.recordset?.[0] as { c?: unknown } | undefined)?.c ?? 0)
        } catch {
          rowCount = null
        }
      }
      const r2 = await pool
        .request()
        .input("s", sql.NVarChar, sch)
        .input("t", sql.NVarChar, tbl)
        .query(
          `SELECT count(*) AS n FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA=@s AND TABLE_NAME=@t`,
        )
      const n = Number((r2.recordset?.[0] as { n?: unknown } | undefined)?.n ?? 0)
      out.push({ schema_name: sch, table_name: tbl, column_count: n, row_count: rowCount })
    }
    return out
  }

  async exportSchemaJson(
    connectionId: string,
    schema: string | null | undefined,
  ): Promise<SchemaExport> {
    const tablesList = await this.listTables(connectionId, schema)
    const tables: {
      schema: string
      table: string
      columns: { name: string; data_type: string; nullable: boolean }[]
    }[] = []
    for (const [sch, tbl] of tablesList) {
      const info = await this.describeTable(connectionId, tbl, sch)
      if (info) {
        tables.push({
          schema: info.schema_name,
          table: info.table_name,
          columns: info.columns.map((c) => ({
            name: c.name,
            data_type: c.data_type,
            nullable: c.nullable,
          })),
        })
      }
    }
    return { tables }
  }

  async explainQuerySql(connectionId: string, query: string): Promise<string> {
    void connectionId
    return `SQL Server: plano textual não exposto aqui; use SELECT com TOP/FETCH limitado. Prévia: ${query.trim().slice(0, 200)}`
  }

  async validateQuerySql(connectionId: string, query: string): Promise<boolean> {
    try {
      const pool = await this.getPool(connectionId)
      const q = query.trim().replace(/;\s*$/, "")
      await pool.request().query(`SELECT TOP 0 * FROM (${q}) AS _nautilus_validate`)
      return true
    } catch {
      return false
    }
  }

  async listIndexes(
    connectionId: string,
    tableName: string,
    schema: string | null | undefined,
  ): Promise<IndexInfo[]> {
    const pool = await this.getPool(connectionId)
    const sch = schema?.trim() || "dbo"
    const r = await pool
      .request()
      .input("s", sql.NVarChar, sch)
      .input("t", sql.NVarChar, tableName)
      .query(
        `SELECT i.name AS index_name, c.name AS column_name, i.is_unique
         FROM sys.indexes i
         JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
         JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
         JOIN sys.tables tb ON i.object_id = tb.object_id
         JOIN sys.schemas sch ON tb.schema_id = sch.schema_id
         WHERE sch.name = @s AND tb.name = @t AND i.name IS NOT NULL
         ORDER BY i.name, ic.key_ordinal`,
      )
    const by = new Map<string, IndexInfo>()
    for (const row of (r.recordset ?? []) as Record<string, unknown>[]) {
      const iname = String(row.index_name)
      let x = by.get(iname)
      if (!x) {
        x = { index_name: iname, columns: [], is_unique: Boolean(row.is_unique) }
        by.set(iname, x)
      }
      x.columns.push(String(row.column_name))
    }
    return [...by.values()]
  }

  async listViews(connectionId: string, schema: string | null | undefined): Promise<ViewInfo[]> {
    const pool = await this.getPool(connectionId)
    const filter = schema?.trim()
      ? "AND TABLE_SCHEMA = @sch"
      : `AND TABLE_SCHEMA NOT IN ${SYS_SCHEMAS}`
    const req = pool.request()
    if (schema?.trim()) req.input("sch", sql.NVarChar, schema.trim())
    const r = await req.query(
      `SELECT TABLE_SCHEMA, TABLE_NAME, VIEW_DEFINITION FROM INFORMATION_SCHEMA.VIEWS WHERE 1=1 ${filter} ORDER BY 1,2`,
    )
    return ((r.recordset ?? []) as Record<string, unknown>[]).map((x) => ({
      schema_name: String(x.table_schema),
      view_name: String(x.table_name),
      definition: x.view_definition != null ? String(x.view_definition) : null,
    }))
  }

  async getForeignKeys(
    connectionId: string,
    tableName: string,
    schema: string | null | undefined,
  ): Promise<ForeignKeyInfo[]> {
    const pool = await this.getPool(connectionId)
    const sch = schema?.trim() || "dbo"
    const r = await pool
      .request()
      .input("s", sql.NVarChar, sch)
      .input("t", sql.NVarChar, tableName)
      .query(
        `SELECT fk.name AS constraint_name, sch1.name AS from_schema, tb1.name AS from_table,
                col1.name AS from_column, sch2.name AS to_schema, tb2.name AS to_table, col2.name AS to_column
         FROM sys.foreign_keys fk
         JOIN sys.foreign_key_columns fkc ON fk.object_id = fkc.constraint_object_id
         JOIN sys.tables tb1 ON fkc.parent_object_id = tb1.object_id
         JOIN sys.schemas sch1 ON tb1.schema_id = sch1.schema_id
         JOIN sys.columns col1 ON fkc.parent_object_id = col1.object_id AND fkc.parent_column_id = col1.column_id
         JOIN sys.tables tb2 ON fkc.referenced_object_id = tb2.object_id
         JOIN sys.schemas sch2 ON tb2.schema_id = sch2.schema_id
         JOIN sys.columns col2 ON fkc.referenced_object_id = col2.object_id AND fkc.referenced_column_id = col2.column_id
         WHERE sch1.name = @s AND tb1.name = @t`,
      )
    const by = new Map<string, ForeignKeyInfo>()
    for (const row of (r.recordset ?? []) as Record<string, unknown>[]) {
      const cn = String(row.constraint_name)
      let fk = by.get(cn)
      if (!fk) {
        fk = {
          constraint_name: cn,
          from_schema: String(row.from_schema),
          from_table: String(row.from_table),
          from_columns: [],
          to_schema: String(row.to_schema),
          to_table: String(row.to_table),
          to_columns: [],
        }
        by.set(cn, fk)
      }
      fk.from_columns.push(String(row.from_column))
      fk.to_columns.push(String(row.to_column))
    }
    return [...by.values()]
  }

  async getTableRelationships(
    connectionId: string,
    schema: string | null | undefined,
  ): Promise<TableRelationship[]> {
    const pool = await this.getPool(connectionId)
    const sch = schema?.trim() || "dbo"
    const r = await pool.request().input("s", sql.NVarChar, sch).query(
      `SELECT fk.name AS constraint_name, tb1.name AS from_t, tb2.name AS to_t
       FROM sys.foreign_keys fk
       JOIN sys.tables tb1 ON fk.parent_object_id = tb1.object_id
       JOIN sys.schemas sch1 ON tb1.schema_id = sch1.schema_id
       JOIN sys.tables tb2 ON fk.referenced_object_id = tb2.object_id
       WHERE sch1.name = @s`,
    )
    return ((r.recordset ?? []) as Record<string, unknown>[]).map((x) => ({
      from_table: String(x.from_t),
      to_table: String(x.to_t),
      constraint_name: String(x.constraint_name),
    }))
  }

  async getRowCount(
    connectionId: string,
    tableName: string,
    schema: string | null | undefined,
    whereClause: string | null | undefined,
  ): Promise<number> {
    const pool = await this.getPool(connectionId)
    const sch = schema?.trim() || "dbo"
    const where = whereClause?.trim() ? ` WHERE ${whereClause.trim()}` : ""
    const r = await pool.request().query(`SELECT count_big(*) AS c FROM ${bId(sch)}.${bId(tableName)}${where}`)
    return Number((r.recordset?.[0] as { c?: unknown } | undefined)?.c ?? 0)
  }

  async getColumnStats(
    connectionId: string,
    tableName: string,
    schema: string | null | undefined,
    columnNames: string[] | null | undefined,
  ): Promise<ColumnStat[]> {
    const info = await this.describeTable(connectionId, tableName, schema ?? "dbo")
    if (!info) return []
    let cols = info.columns
    if (columnNames?.length) {
      const set = new Set(columnNames)
      cols = cols.filter((c) => set.has(c.name))
    }
    cols = cols.slice(0, 10)
    const pool = await this.getPool(connectionId)
    const sch = schema?.trim() || "dbo"
    const from = `${bId(sch)}.${bId(tableName)}`
    const out: ColumnStat[] = []
    for (const c of cols) {
      try {
        const r = await pool.request().query(`SELECT count_big(*) AS cnt FROM ${from}`)
        out.push({ column_name: c.name, stat_type: "table_rows_hint", value: Number((r.recordset?.[0] as { cnt?: unknown })?.cnt ?? 0) })
        break
      } catch {
        void 0
      }
    }
    return out
  }

  async suggestTables(
    connectionId: string,
    searchTerm: string,
    schema: string | null | undefined,
  ): Promise<[string, string, string][]> {
    const pool = await this.getPool(connectionId)
    const term = `%${searchTerm}%`
    const req = pool.request().input("a", sql.NVarChar, term).input("b", sql.NVarChar, term)
    const filter = schema?.trim() ? "AND c.TABLE_SCHEMA = @sch" : ""
    if (schema?.trim()) req.input("sch", sql.NVarChar, schema.trim())
    const r = await req.query(
      `SELECT DISTINCT c.TABLE_SCHEMA, c.TABLE_NAME, c.COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS c
       WHERE (c.TABLE_NAME LIKE @a OR c.COLUMN_NAME LIKE @b) ${filter}`,
    )
    return ((r.recordset ?? []) as Record<string, unknown>[]).map((x) => [
      String(x.TABLE_SCHEMA ?? x.table_schema),
      String(x.TABLE_NAME ?? x.table_name),
      String(x.COLUMN_NAME ?? x.column_name),
    ])
  }
}

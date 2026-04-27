import mysql from "mysql2/promise"
import type { FieldPacket, Pool, RowDataPacket } from "mysql2/promise"
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

function qId(s: string): string {
  return `\`${String(s).replaceAll("`", "``")}\``
}

function rowToArray(columns: string[], row: Record<string, unknown>): unknown[] {
  return columns.map((c) => row[c])
}

const SYS_SCHEMAS = "('information_schema','mysql','performance_schema','sys')"

export class MysqlAdapter implements SqlEngineAdapter {
  private readonly configs: Map<string, DatabaseConfig>
  private readonly pools = new Map<string, Pool>()

  constructor(configs: Map<string, DatabaseConfig>) {
    this.configs = configs
  }

  private getPool(connectionId: string): Pool {
    let p = this.pools.get(connectionId)
    if (!p) {
      const cfg = this.configs.get(connectionId)
      if (!cfg) throw new Error(`Conexão não encontrada: ${connectionId}`)
      p = mysql.createPool({
        uri: cfg.url,
        connectionLimit: 5,
        connectTimeout: 10_000,
      })
      this.pools.set(connectionId, p)
    }
    return p
  }

  private rowsToQueryResult(rows: RowDataPacket[], fields: FieldPacket[] | undefined): QueryResult {
    if (!rows.length) {
      const cols = fields?.map((f) => f.name) ?? []
      return { columns: cols, rows: [], row_count: 0 }
    }
    const columns = fields?.map((f) => f.name) ?? Object.keys(rows[0] ?? {})
    const out = rows.map((r) => rowToArray(columns, r as Record<string, unknown>))
    return { columns, rows: out, row_count: out.length }
  }

  async testConnection(connectionId: string): Promise<boolean> {
    const cfg = this.configs.get(connectionId)
    if (!cfg) return false
    const pool = this.getPool(connectionId)
    try {
      const [rows] = await pool.query<RowDataPacket[]>("SELECT 1 AS ok")
      return rows.length > 0
    } catch {
      return false
    }
  }

  async probeConnection(
    connectionId: string,
  ): Promise<{ ok: boolean; latencyMs: number; version: string | null; error?: string }> {
    const t0 = performance.now()
    try {
      const pool = this.getPool(connectionId)
      const [rows] = await pool.query<RowDataPacket[]>("SELECT VERSION() AS v")
      const latencyMs = Math.round(performance.now() - t0)
      const raw = rows[0] && (rows[0] as Record<string, unknown>)["v"]
      const version =
        raw != null ? String(raw).split("\n")[0]!.trim().slice(0, 500) || null : null
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
    const pool = this.getPool(connectionId)
    let q = query.trim().replace(/;\s*$/, "")
    const head = q.split("--")[0] ?? q
    if (!/\bLIMIT\s+\d+/i.test(head)) {
      q = `${q} LIMIT ${maxRows}`
    }
    const [rows, fields] = await pool.query<RowDataPacket[]>({
      sql: q,
      timeout: timeoutSeconds * 1000,
    })
    const arr = Array.isArray(rows) ? rows : []
    return this.rowsToQueryResult(arr, fields as FieldPacket[] | undefined)
  }

  async listTables(connectionId: string, schema?: string | null): Promise<[string, string][]> {
    const pool = this.getPool(connectionId)
    if (schema?.trim()) {
      const [rows] = await pool.query<RowDataPacket[]>(
        `SELECT table_schema, table_name FROM information_schema.tables
         WHERE table_type = 'BASE TABLE' AND table_schema = ? ORDER BY table_schema, table_name`,
        [schema.trim()],
      )
      return rows.map((r) => [String(r.table_schema), String(r.table_name)])
    }
    const [rows] = await pool.query<RowDataPacket[]>(
      `SELECT table_schema, table_name FROM information_schema.tables
       WHERE table_type = 'BASE TABLE' AND table_schema NOT IN ${SYS_SCHEMAS}
       ORDER BY table_schema, table_name`,
    )
    return rows.map((r) => [String(r.table_schema), String(r.table_name)])
  }

  async describeTable(
    connectionId: string,
    tableName: string,
    schema?: string | null,
  ): Promise<TableInfo | null> {
    const pool = this.getPool(connectionId)
    const sch = schema?.trim()
    const q = sch
      ? `SELECT table_schema, table_name, column_name, data_type, is_nullable
         FROM information_schema.columns
         WHERE table_name = ? AND table_schema = ? ORDER BY ordinal_position`
      : `SELECT table_schema, table_name, column_name, data_type, is_nullable
         FROM information_schema.columns
         WHERE table_name = ? AND table_schema = DATABASE() ORDER BY ordinal_position`
    const params = sch ? [tableName, sch] : [tableName]
    const [rows] = await pool.query<RowDataPacket[]>(q, params)
    if (!rows.length) return null
    const row0 = rows[0] as Record<string, unknown>
    const schemaName = String(row0.table_schema)
    const table = String(row0.table_name)
    const columns: ColumnInfo[] = rows.map((r) => {
      const x = r as Record<string, unknown>
      return {
        name: String(x.column_name),
        data_type: String(x.data_type),
        nullable: String(x.is_nullable).toUpperCase() === "YES",
      }
    })
    return { schema_name: schemaName, table_name: table, columns }
  }

  async listDatabases(connectionId: string): Promise<string[]> {
    const pool = this.getPool(connectionId)
    const [rows] = await pool.query<RowDataPacket[]>(
      `SELECT schema_name AS d FROM information_schema.schemata
       WHERE schema_name NOT IN ${SYS_SCHEMAS} ORDER BY schema_name`,
    )
    return rows.map((r) => String(r.d))
  }

  async getTableSample(
    connectionId: string,
    tableName: string,
    schema: string | null | undefined,
    limit: number,
    offset: number,
  ): Promise<QueryResult> {
    const pool = this.getPool(connectionId)
    const lim = Math.max(1, Math.min(limit, 50_000))
    const off = Math.max(0, Math.min(offset, 1_000_000))
    const sch = schema?.trim()
    const from = sch
      ? `${qId(sch)}.${qId(tableName)}`
      : `${qId(tableName)}`
    const q = `SELECT * FROM ${from} LIMIT ? OFFSET ?`
    const [rows, fields] = await pool.query<RowDataPacket[]>(q, [lim, off])
    const arr = Array.isArray(rows) ? rows : []
    return this.rowsToQueryResult(arr, fields as FieldPacket[] | undefined)
  }

  async getSchemaSummary(
    connectionId: string,
    schema: string | null | undefined,
    includeRowCount: boolean,
  ): Promise<SchemaTableSummary[]> {
    const pool = this.getPool(connectionId)
    const sch = schema?.trim()
    const filter = sch
      ? "AND table_schema = ?"
      : `AND table_schema NOT IN ${SYS_SCHEMAS}`
    const params = sch ? [sch] : []
    const q = `
      SELECT table_schema, table_name, table_rows AS approx_rows,
        (SELECT count(*) FROM information_schema.columns c
         WHERE c.table_schema = t.table_schema AND c.table_name = t.table_name) AS col_count
      FROM information_schema.tables t
      WHERE table_type = 'BASE TABLE' ${filter}
      ORDER BY table_schema, table_name`
    const [rows] = await pool.query<RowDataPacket[]>(q, params)
    const out: SchemaTableSummary[] = []
    for (const r of rows) {
      const tableSchema = String(r.table_schema)
      const tableName = String(r.table_name)
      let rowCount: number | null = null
      if (includeRowCount) {
        try {
          const [countRows] = await pool.query<RowDataPacket[]>(
            `SELECT count(*) AS c FROM ${qId(tableSchema)}.${qId(tableName)}`,
          )
          rowCount = Number((countRows[0] as Record<string, unknown> | undefined)?.c ?? 0)
        } catch {
          rowCount = r.approx_rows != null ? Number(r.approx_rows) : null
        }
      }
      out.push({
        schema_name: tableSchema,
        table_name: tableName,
        column_count: Number(r.col_count),
        row_count: rowCount,
      })
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
    const pool = this.getPool(connectionId)
    const q = query.trim().replace(/;\s*$/, "")
    const [rows] = await pool.query<RowDataPacket[]>(`EXPLAIN ${q}`)
    const arr = Array.isArray(rows) ? rows : []
    return arr
      .map((row) => {
        const rec = row as Record<string, unknown>
        return Object.entries(rec)
          .map(([k, v]) => `${k}: ${v}`)
          .join(", ")
      })
      .join("\n")
  }

  async validateQuerySql(connectionId: string, query: string): Promise<boolean> {
    const pool = this.getPool(connectionId)
    const q = query.trim().replace(/;\s*$/, "")
    try {
      await pool.query<RowDataPacket[]>(`EXPLAIN ${q}`)
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
    const pool = this.getPool(connectionId)
    const sch = schema?.trim() ?? ""
    const useDb = sch || (await this.defaultDatabase(connectionId))
    if (!useDb) return []
    const [rows] = await pool.query<RowDataPacket[]>(
      `SELECT index_name, column_name, non_unique, seq_in_index
       FROM information_schema.statistics
       WHERE table_schema = ? AND table_name = ?
       ORDER BY index_name, seq_in_index`,
      [useDb, tableName],
    )
    const byIndex = new Map<string, IndexInfo>()
    for (const r of rows) {
      const iname = String(r.index_name)
      let idx = byIndex.get(iname)
      if (!idx) {
        idx = { index_name: iname, columns: [], is_unique: Number(r.non_unique) === 0 }
        byIndex.set(iname, idx)
      }
      idx.columns.push(String(r.column_name))
    }
    return [...byIndex.values()]
  }

  private async defaultDatabase(connectionId: string): Promise<string | null> {
    const pool = this.getPool(connectionId)
    const [rows] = await pool.query<RowDataPacket[]>("SELECT DATABASE() AS d")
    const d = rows[0]?.d
    return d != null ? String(d) : null
  }

  async listViews(connectionId: string, schema: string | null | undefined): Promise<ViewInfo[]> {
    const pool = this.getPool(connectionId)
    const sch = schema?.trim()
    const filter = sch ? "AND table_schema = ?" : `AND table_schema NOT IN ${SYS_SCHEMAS}`
    const params = sch ? [sch] : []
    const q = `
      SELECT table_schema, table_name, view_definition
      FROM information_schema.views WHERE 1=1 ${filter}
      ORDER BY table_schema, table_name`
    const [rows] = await pool.query<RowDataPacket[]>(q, params)
    return rows.map((r) => ({
      schema_name: String(r.table_schema),
      view_name: String(r.table_name),
      definition: r.view_definition != null ? String(r.view_definition) : null,
    }))
  }

  async getForeignKeys(
    connectionId: string,
    tableName: string,
    schema: string | null | undefined,
  ): Promise<ForeignKeyInfo[]> {
    const pool = this.getPool(connectionId)
    const sch = schema?.trim() ?? (await this.defaultDatabase(connectionId))
    if (!sch) return []
    const [rows] = await pool.query<RowDataPacket[]>(
      `SELECT constraint_name, table_schema, table_name, column_name,
              referenced_table_schema, referenced_table_name, referenced_column_name, ordinal_position
       FROM information_schema.key_column_usage
       WHERE table_schema = ? AND table_name = ? AND referenced_table_name IS NOT NULL
       ORDER BY constraint_name, ordinal_position`,
      [sch, tableName],
    )
    const byC = new Map<string, ForeignKeyInfo>()
    for (const r of rows) {
      const cn = String(r.constraint_name)
      let fk = byC.get(cn)
      if (!fk) {
        fk = {
          constraint_name: cn,
          from_schema: String(r.table_schema),
          from_table: String(r.table_name),
          from_columns: [],
          to_schema: String(r.referenced_table_schema),
          to_table: String(r.referenced_table_name),
          to_columns: [],
        }
        byC.set(cn, fk)
      }
      fk.from_columns.push(String(r.column_name))
      fk.to_columns.push(String(r.referenced_column_name))
    }
    return [...byC.values()]
  }

  async getTableRelationships(
    connectionId: string,
    schema: string | null | undefined,
  ): Promise<TableRelationship[]> {
    const pool = this.getPool(connectionId)
    const sch = schema?.trim() ?? (await this.defaultDatabase(connectionId))
    if (!sch) return []
    const [rows] = await pool.query<RowDataPacket[]>(
      `SELECT DISTINCT constraint_name, table_name, referenced_table_name
       FROM information_schema.key_column_usage
       WHERE table_schema = ? AND referenced_table_name IS NOT NULL`,
      [sch],
    )
    return rows.map((r) => ({
      from_table: String(r.table_name),
      to_table: String(r.referenced_table_name),
      constraint_name: String(r.constraint_name),
    }))
  }

  async getRowCount(
    connectionId: string,
    tableName: string,
    schema: string | null | undefined,
    whereClause: string | null | undefined,
  ): Promise<number> {
    const pool = this.getPool(connectionId)
    const sch = schema?.trim()
    const from = sch ? `${qId(sch)}.${qId(tableName)}` : `${qId(tableName)}`
    const where = whereClause?.trim() ? ` WHERE ${whereClause.trim()}` : ""
    const [rows] = await pool.query<RowDataPacket[]>(`SELECT count(*) AS c FROM ${from}${where}`)
    const arr = Array.isArray(rows) ? rows : []
    return Number((arr[0] as Record<string, unknown> | undefined)?.c ?? 0)
  }

  async getColumnStats(
    connectionId: string,
    tableName: string,
    schema: string | null | undefined,
    columnNames: string[] | null | undefined,
  ): Promise<ColumnStat[]> {
    const pool = this.getPool(connectionId)
    const sch = schema?.trim() ?? (await this.defaultDatabase(connectionId))
    if (!sch) return []
    const info = await this.describeTable(connectionId, tableName, sch)
    if (!info) return []
    let cols = info.columns
    if (columnNames?.length) {
      const set = new Set(columnNames)
      cols = cols.filter((c) => set.has(c.name))
    }
    cols = cols.slice(0, 20)
    const result: ColumnStat[] = []
    const safeTable = `${qId(sch)}.${qId(tableName)}`
    for (const col of cols) {
      const safeCol = qId(col.name)
      try {
        const numericish = /int|decimal|numeric|float|double|real/i.test(col.data_type)
        if (numericish) {
          const [rrows] = await pool.query<RowDataPacket[]>(
            `SELECT count(*) AS cnt,
                    sum(case when ${safeCol} is null then 1 else 0 end) AS nulls,
                    min(${safeCol}) AS mn, max(${safeCol}) AS mx, avg(${safeCol}) AS av
             FROM ${safeTable}`,
          )
          const r0 = rrows[0] as Record<string, unknown> | undefined
          if (r0) {
            result.push({ column_name: col.name, stat_type: "count", value: Number(r0.cnt) })
            result.push({
              column_name: col.name,
              stat_type: "null_count",
              value: Number(r0.nulls),
            })
            if (r0.mn != null) {
              result.push({ column_name: col.name, stat_type: "min", value: r0.mn })
              result.push({ column_name: col.name, stat_type: "max", value: r0.mx })
              result.push({
                column_name: col.name,
                stat_type: "avg",
                value: r0.av != null ? Number(r0.av) : null,
              })
            }
          }
        } else {
          const [rrows] = await pool.query<RowDataPacket[]>(
            `SELECT count(*) AS cnt, count(distinct ${safeCol}) AS d FROM ${safeTable}`,
          )
          const r0 = rrows[0] as Record<string, unknown> | undefined
          if (r0) {
            result.push({ column_name: col.name, stat_type: "count", value: Number(r0.cnt) })
            result.push({
              column_name: col.name,
              stat_type: "distinct_count",
              value: Number(r0.d),
            })
          }
        }
      } catch {
        void 0
      }
    }
    return result
  }

  async suggestTables(
    connectionId: string,
    searchTerm: string,
    schema: string | null | undefined,
  ): Promise<[string, string, string][]> {
    const pool = this.getPool(connectionId)
    const term = `%${searchTerm}%`
    const sch = schema?.trim()
    const filter = sch ? "AND table_schema = ?" : `AND table_schema NOT IN ${SYS_SCHEMAS}`
    const params = sch ? [term, term, sch] : [term, term]
    const q = `
      SELECT DISTINCT table_schema, table_name, column_name
      FROM information_schema.columns
      WHERE (table_name LIKE ? OR column_name LIKE ?) ${filter}
      ORDER BY table_schema, table_name, column_name`
    const [rows] = await pool.query<RowDataPacket[]>(q, params)
    return rows.map((r) => [String(r.table_schema), String(r.table_name), String(r.column_name)])
  }
}

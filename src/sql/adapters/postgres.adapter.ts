import { Pool, type QueryResult as PgQueryResult } from "pg"
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
  return `"${String(s).replaceAll('"', '""')}"`
}

function rowToArray(columns: string[], row: Record<string, unknown>): unknown[] {
  return columns.map((c) => row[c])
}

export class PostgresAdapter implements SqlEngineAdapter {
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
      p = new Pool({
        connectionString: cfg.url,
        max: 5,
        connectionTimeoutMillis: 10_000,
      })
      this.pools.set(connectionId, p)
    }
    return p
  }

  async testConnection(connectionId: string): Promise<boolean> {
    const cfg = this.configs.get(connectionId)
    if (!cfg) return false
    const pool = this.getPool(connectionId)
    try {
      const r = await pool.query("SELECT 1 AS ok")
      return r.rows.length > 0
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
      const r = await pool.query("SELECT version() AS v")
      const latencyMs = Math.round(performance.now() - t0)
      const raw = r.rows[0] && (r.rows[0] as Record<string, unknown>)["v"]
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
    const client = await pool.connect()
    try {
      await client.query(`SET statement_timeout = ${timeoutSeconds * 1000}`)
      const res = await client.query(q)
      return this.pgResultToQueryResult(res)
    } finally {
      client.release()
    }
  }

  private pgResultToQueryResult(res: PgQueryResult): QueryResult {
    if (!res.rows.length) {
      return { columns: res.fields?.map((f) => f.name) ?? [], rows: [], row_count: 0 }
    }
    const columns = res.fields.map((f) => f.name)
    const rows = res.rows.map((row) => rowToArray(columns, row as Record<string, unknown>))
    return { columns, rows, row_count: rows.length }
  }

  async listTables(connectionId: string, schema?: string | null): Promise<[string, string][]> {
    const pool = this.getPool(connectionId)
    const schemaFilter = schema ? "AND table_schema = $1" : ""
    const params = schema ? [schema] : []
    const q = `
      SELECT table_schema, table_name
      FROM information_schema.tables
      WHERE table_schema NOT IN ('pg_catalog', 'information_schema') ${schemaFilter}
      ORDER BY table_schema, table_name
    `
    const res = await pool.query(q, params)
    return res.rows.map((r) => [String(r.table_schema), String(r.table_name)])
  }

  async describeTable(
    connectionId: string,
    tableName: string,
    schema?: string | null,
  ): Promise<TableInfo | null> {
    const pool = this.getPool(connectionId)
    const schemaCondition = schema ? "AND table_schema = $2" : ""
    const params = schema ? [tableName, schema] : [tableName]
    const q = `
      SELECT table_schema, table_name, column_name, data_type, is_nullable
      FROM information_schema.columns
      WHERE table_name = $1 ${schemaCondition}
      ORDER BY ordinal_position
    `
    const res = await pool.query(q, params)
    if (!res.rows.length) return null
    const row0 = res.rows[0] as Record<string, unknown>
    const schemaName = String(row0.table_schema)
    const table = String(row0.table_name)
    const columns: ColumnInfo[] = res.rows.map((r) => {
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
    const res = await pool.query(
      "SELECT datname FROM pg_database WHERE NOT datistemplate ORDER BY datname",
    )
    return res.rows.map((r) => String(r.datname))
  }

  async getTableSample(
    connectionId: string,
    tableName: string,
    schema: string | null | undefined,
    limit: number,
    offset: number,
  ): Promise<QueryResult> {
    const pool = this.getPool(connectionId)
    const sch = schema ?? "public"
    const lim = Math.max(1, Math.min(limit, 50_000))
    const off = Math.max(0, Math.min(offset, 1_000_000))
    const q = `SELECT * FROM ${qId(sch)}.${qId(tableName)} LIMIT $1 OFFSET $2`
    const res = await pool.query(q, [lim, off])
    return this.pgResultToQueryResult(res)
  }

  async getSchemaSummary(
    connectionId: string,
    schema: string | null | undefined,
    includeRowCount: boolean,
  ): Promise<SchemaTableSummary[]> {
    const pool = this.getPool(connectionId)
    const schemaFilter = schema ? "AND table_schema = $1" : "AND table_schema NOT IN ('pg_catalog', 'information_schema')"
    const params = schema ? [schema] : []
    const q = `
      SELECT table_schema, table_name,
        (SELECT count(*)::int FROM information_schema.columns c
         WHERE c.table_schema = t.table_schema AND c.table_name = t.table_name) AS col_count
      FROM information_schema.tables t
      WHERE table_type = 'BASE TABLE' ${schemaFilter}
      ORDER BY table_schema, table_name
    `
    const res = await pool.query(q, params)
    const out: SchemaTableSummary[] = []
    for (const r of res.rows) {
      const tableSchema = String(r.table_schema)
      const tableName = String(r.table_name)
      let rowCount: number | null = null
      if (includeRowCount) {
        try {
          const c = await pool.query(
            `SELECT count(*)::bigint AS c FROM ${qId(tableSchema)}.${qId(tableName)}`,
          )
          rowCount = Number(c.rows[0]?.c ?? 0)
        } catch {
          rowCount = null
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
    const res = await pool.query(`EXPLAIN (FORMAT TEXT) ${q}`)
    return res.rows
      .map((row) => {
        const rec = row as Record<string, unknown>
        const v =
          rec["QUERY PLAN"] ?? rec["query plan"] ?? Object.values(rec)[0]
        return String(v ?? "")
      })
      .join("\n")
  }

  async validateQuerySql(connectionId: string, query: string): Promise<boolean> {
    const pool = this.getPool(connectionId)
    const q = query.trim().replace(/;\s*$/, "")
    const client = await pool.connect()
    try {
      await client.query(`PREPARE _nautilus_validate AS ${q}`)
      await client.query("DEALLOCATE _nautilus_validate")
      return true
    } catch {
      return false
    } finally {
      client.release()
    }
  }

  async listIndexes(
    connectionId: string,
    tableName: string,
    schema: string | null | undefined,
  ): Promise<IndexInfo[]> {
    const pool = this.getPool(connectionId)
    const sch = schema ?? "public"
    const q = `
      SELECT i.relname AS index_name, a.attname AS column_name,
             ix.indisunique AS is_unique
      FROM pg_index ix
      JOIN pg_class t ON t.oid = ix.indrelid
      JOIN pg_class i ON i.oid = ix.indexrelid
      JOIN pg_namespace n ON n.oid = t.relnamespace
      JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(ix.indkey) AND a.attnum > 0 AND NOT a.attisdropped
      WHERE n.nspname = $1 AND t.relname = $2
      ORDER BY i.relname, array_position(ix.indkey, a.attnum)
    `
    const res = await pool.query(q, [sch, tableName])
    const byIndex = new Map<string, IndexInfo>()
    for (const r of res.rows) {
      const iname = String(r.index_name)
      let idx = byIndex.get(iname)
      if (!idx) {
        idx = { index_name: iname, columns: [], is_unique: Boolean(r.is_unique) }
        byIndex.set(iname, idx)
      }
      idx.columns.push(String(r.column_name))
    }
    return [...byIndex.values()]
  }

  async listViews(connectionId: string, schema: string | null | undefined): Promise<ViewInfo[]> {
    const pool = this.getPool(connectionId)
    const schemaFilter = schema
      ? "AND table_schema = $1"
      : "AND table_schema NOT IN ('pg_catalog', 'information_schema')"
    const params = schema ? [schema] : []
    const q = `
      SELECT table_schema, table_name, view_definition
      FROM information_schema.views
      WHERE 1=1 ${schemaFilter}
      ORDER BY table_schema, table_name
    `
    const res = await pool.query(q, params)
    return res.rows.map((r) => ({
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
    const sch = schema ?? "public"
    const q = `
      SELECT
        c.conname AS constraint_name,
        n1.nspname AS from_schema, t1.relname AS from_table,
        (SELECT array_agg(a.attname ORDER BY u.attposition)
         FROM pg_attribute a
         JOIN unnest(c.conkey) WITH ORDINALITY AS u(attnum, attposition) ON a.attnum = u.attnum
         WHERE a.attrelid = c.conrelid AND a.attnum > 0 AND NOT a.attisdropped) AS from_cols,
        n2.nspname AS to_schema, t2.relname AS to_table,
        (SELECT array_agg(a.attname ORDER BY u.attposition)
         FROM pg_attribute a
         JOIN unnest(c.confkey) WITH ORDINALITY AS u(attnum, attposition) ON a.attnum = u.attnum
         WHERE a.attrelid = c.confrelid AND a.attnum > 0 AND NOT a.attisdropped) AS to_cols
      FROM pg_constraint c
      JOIN pg_class t1 ON t1.oid = c.conrelid
      JOIN pg_namespace n1 ON n1.oid = t1.relnamespace
      JOIN pg_class t2 ON t2.oid = c.confrelid
      JOIN pg_namespace n2 ON n2.oid = t2.relnamespace
      WHERE c.contype = 'f' AND n1.nspname = $1 AND t1.relname = $2
    `
    const res = await pool.query(q, [sch, tableName])
    return res.rows.map((r) => {
      const fromCols = Array.isArray(r.from_cols) ? (r.from_cols as string[]).map(String) : []
      const toCols = Array.isArray(r.to_cols) ? (r.to_cols as string[]).map(String) : []
      return {
        constraint_name: String(r.constraint_name),
        from_schema: String(r.from_schema),
        from_table: String(r.from_table),
        from_columns: fromCols,
        to_schema: String(r.to_schema),
        to_table: String(r.to_table),
        to_columns: toCols,
      }
    })
  }

  async getTableRelationships(
    connectionId: string,
    schema: string | null | undefined,
  ): Promise<TableRelationship[]> {
    const pool = this.getPool(connectionId)
    const sch = schema ?? "public"
    const q = `
      SELECT c.conname, t1.relname AS from_t, t2.relname AS to_t
      FROM pg_constraint c
      JOIN pg_class t1 ON t1.oid = c.conrelid
      JOIN pg_namespace n1 ON n1.oid = t1.relnamespace
      JOIN pg_class t2 ON t2.oid = c.confrelid
      WHERE c.contype = 'f' AND n1.nspname = $1
    `
    const res = await pool.query(q, [sch])
    return res.rows.map((r) => ({
      from_table: String(r.from_t),
      to_table: String(r.to_t),
      constraint_name: String(r.conname),
    }))
  }

  async getRowCount(
    connectionId: string,
    tableName: string,
    schema: string | null | undefined,
    whereClause: string | null | undefined,
  ): Promise<number> {
    const pool = this.getPool(connectionId)
    const sch = schema ?? "public"
    const where = whereClause?.trim() ? ` WHERE ${whereClause.trim()}` : ""
    const q = `SELECT count(*)::bigint AS c FROM ${qId(sch)}.${qId(tableName)}${where}`
    const res = await pool.query(q)
    return Number(res.rows[0]?.c ?? 0)
  }

  async getColumnStats(
    connectionId: string,
    tableName: string,
    schema: string | null | undefined,
    columnNames: string[] | null | undefined,
  ): Promise<ColumnStat[]> {
    const pool = this.getPool(connectionId)
    const sch = schema ?? "public"
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
        const numericTypes = new Set([
          "integer",
          "bigint",
          "smallint",
          "numeric",
          "real",
          "double precision",
        ])
        if (numericTypes.has(col.data_type)) {
          const row = await pool.query(
            `SELECT count(*)::bigint AS cnt, count(${safeCol})::bigint AS non_null, min(${safeCol}) AS mn, max(${safeCol}) AS mx, avg(${safeCol})::numeric AS av FROM ${safeTable}`,
          )
          const r0 = row.rows[0] as Record<string, unknown> | undefined
          if (r0) {
            result.push({ column_name: col.name, stat_type: "count", value: Number(r0.cnt) })
            result.push({
              column_name: col.name,
              stat_type: "null_count",
              value: Number(r0.cnt) - Number(r0.non_null),
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
          const row = await pool.query(
            `SELECT count(*)::bigint AS cnt, count(DISTINCT ${safeCol})::bigint AS d FROM ${safeTable}`,
          )
          const r0 = row.rows[0] as Record<string, unknown> | undefined
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
    const schemaFilter = schema ? "AND table_schema = $2" : ""
    const params = schema ? [term, schema] : [term]
    const q = `
      SELECT DISTINCT table_schema, table_name, column_name
      FROM information_schema.columns
      WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
        AND (table_name ILIKE $1 OR column_name ILIKE $1) ${schemaFilter}
      ORDER BY table_schema, table_name, column_name
    `
    const res = await pool.query(q, params)
    return res.rows.map((r) => [String(r.table_schema), String(r.table_name), String(r.column_name)])
  }
}

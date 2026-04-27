import BetterSqlite3 from "better-sqlite3"
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

function sqlitePath(url: string): string {
  const u = url.trim()
  if (u === ":memory:" || u.endsWith(":memory:")) return ":memory:"
  return u.replace(/^file:/i, "")
}

type SqliteDatabase = InstanceType<typeof BetterSqlite3>

export class SqliteAdapter implements SqlEngineAdapter {
  private readonly configs: Map<string, DatabaseConfig>
  private readonly dbs = new Map<string, SqliteDatabase>()

  constructor(configs: Map<string, DatabaseConfig>) {
    this.configs = configs
  }

  private getDb(connectionId: string): SqliteDatabase {
    let d = this.dbs.get(connectionId)
    if (!d) {
      const cfg = this.configs.get(connectionId)
      if (!cfg) throw new Error(`Conexão não encontrada: ${connectionId}`)
      d = new BetterSqlite3(sqlitePath(cfg.url))
      d.pragma("foreign_keys = ON")
      this.dbs.set(connectionId, d)
    }
    return d
  }

  private runToQueryResult(db: SqliteDatabase, q: string): QueryResult {
    const stmt = db.prepare(q)
    const rows = stmt.all() as Record<string, unknown>[]
    if (!rows.length) return { columns: [], rows: [], row_count: 0 }
    const columns = Object.keys(rows[0]!)
    const out = rows.map((r) => rowToArray(columns, r))
    return { columns, rows: out, row_count: out.length }
  }

  async testConnection(connectionId: string): Promise<boolean> {
    try {
      const db = this.getDb(connectionId)
      db.prepare("SELECT 1").get()
      return true
    } catch {
      return false
    }
  }

  async probeConnection(
    connectionId: string,
  ): Promise<{ ok: boolean; latencyMs: number; version: string | null; error?: string }> {
    const t0 = performance.now()
    try {
      const db = this.getDb(connectionId)
      const r = db.prepare("SELECT sqlite_version() AS v").get() as Record<string, unknown> | undefined
      const latencyMs = Math.round(performance.now() - t0)
      const version = r?.v != null ? String(r.v).slice(0, 500) : null
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
    const db = this.getDb(connectionId)
    db.pragma(`busy_timeout = ${Math.min(Math.max(1, timeoutSeconds * 1000), 600_000)}`)
    let q = query.trim().replace(/;\s*$/, "")
    const head = q.split("--")[0] ?? q
    if (!/\bLIMIT\s+\d+/i.test(head)) {
      q = `${q} LIMIT ${maxRows}`
    }
    return await Promise.resolve(this.runToQueryResult(db, q))
  }

  async listTables(connectionId: string, schema?: string | null): Promise<[string, string][]> {
    const db = this.getDb(connectionId)
    const sch = schema?.trim()
    if (sch && sch !== "main") return []
    const rows = db
      .prepare(
        `SELECT 'main' AS s, name AS t FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name`,
      )
      .all() as { s: string; t: string }[]
    return rows.map((r) => [String(r.s), String(r.t)])
  }

  async describeTable(
    connectionId: string,
    tableName: string,
    schema?: string | null,
  ): Promise<TableInfo | null> {
    const db = this.getDb(connectionId)
    const rows = db.prepare(`PRAGMA table_info(${qId(tableName)})`).all() as {
      cid: number
      name: string
      type: string
      notnull: number
    }[]
    if (!rows.length) return null
    const schemaName = schema?.trim() || "main"
    const columns: ColumnInfo[] = rows.map((r) => ({
      name: r.name,
      data_type: r.type || "unknown",
      nullable: r.notnull === 0,
    }))
    return { schema_name: schemaName, table_name: tableName, columns }
  }

  async listDatabases(connectionId: string): Promise<string[]> {
    void connectionId
    return ["main"]
  }

  async getTableSample(
    connectionId: string,
    tableName: string,
    schema: string | null | undefined,
    limit: number,
    offset: number,
  ): Promise<QueryResult> {
    void schema
    const db = this.getDb(connectionId)
    const lim = Math.max(1, Math.min(limit, 50_000))
    const off = Math.max(0, Math.min(offset, 1_000_000))
    const q = `SELECT * FROM ${qId(tableName)} LIMIT ${lim} OFFSET ${off}`
    return await Promise.resolve(this.runToQueryResult(db, q))
  }

  async getSchemaSummary(
    connectionId: string,
    schema: string | null | undefined,
    includeRowCount: boolean,
  ): Promise<SchemaTableSummary[]> {
    void schema
    const tables = await this.listTables(connectionId, null)
    const out: SchemaTableSummary[] = []
    const db = this.getDb(connectionId)
    for (const [sch, tbl] of tables) {
      let rowCount: number | null = null
      if (includeRowCount) {
        try {
          const r = db.prepare(`SELECT count(*) AS c FROM ${qId(tbl)}`).get() as { c: number }
          rowCount = Number(r.c)
        } catch {
          rowCount = null
        }
      }
      const cols = db.prepare(`PRAGMA table_info(${qId(tbl)})`).all() as { cid: number }[]
      out.push({
        schema_name: sch,
        table_name: tbl,
        column_count: cols.length,
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
    const db = this.getDb(connectionId)
    const q = query.trim().replace(/;\s*$/, "")
    const rows = db.prepare(`EXPLAIN QUERY PLAN ${q}`).all() as Record<string, unknown>[]
    return rows.map((r) => JSON.stringify(r)).join("\n")
  }

  async validateQuerySql(connectionId: string, query: string): Promise<boolean> {
    try {
      const db = this.getDb(connectionId)
      const q = query.trim().replace(/;\s*$/, "")
      db.prepare(`EXPLAIN QUERY PLAN ${q}`).all()
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
    void schema
    const db = this.getDb(connectionId)
    const rows = db.prepare(`PRAGMA index_list(${qId(tableName)})`).all() as {
      name: string
      unique: number
    }[]
    const out: IndexInfo[] = []
    for (const r of rows) {
      const cols = db.prepare(`PRAGMA index_info(${qId(r.name)})`).all() as {
        name: string
        seqno: number
      }[]
      cols.sort((a, b) => a.seqno - b.seqno)
      out.push({
        index_name: r.name,
        columns: cols.map((c) => c.name).filter(Boolean),
        is_unique: r.unique === 1,
      })
    }
    return out
  }

  async listViews(connectionId: string, schema: string | null | undefined): Promise<ViewInfo[]> {
    void schema
    const db = this.getDb(connectionId)
    const rows = db
      .prepare(
        `SELECT name, sql FROM sqlite_master WHERE type='view' ORDER BY name`,
      )
      .all() as { name: string; sql: string | null }[]
    return rows.map((r) => ({
      schema_name: "main",
      view_name: r.name,
      definition: r.sql,
    }))
  }

  async getForeignKeys(
    connectionId: string,
    tableName: string,
    schema: string | null | undefined,
  ): Promise<ForeignKeyInfo[]> {
    void schema
    const db = this.getDb(connectionId)
    const rows = db.prepare(`PRAGMA foreign_key_list(${qId(tableName)})`).all() as {
      id: number
      seq: number
      table: string
      from: string
      to: string
    }[]
    const byId = new Map<number, ForeignKeyInfo>()
    for (const r of rows) {
      let fk = byId.get(r.id)
      if (!fk) {
        fk = {
          constraint_name: `fk_${r.id}`,
          from_schema: "main",
          from_table: tableName,
          from_columns: [],
          to_schema: "main",
          to_table: r.table,
          to_columns: [],
        }
        byId.set(r.id, fk)
      }
      fk.from_columns.push(r.from)
      fk.to_columns.push(r.to || r.from)
    }
    return [...byId.values()]
  }

  async getTableRelationships(
    connectionId: string,
    schema: string | null | undefined,
  ): Promise<TableRelationship[]> {
    void schema
    const db = this.getDb(connectionId)
    const tables = await this.listTables(connectionId, null)
    const rels: TableRelationship[] = []
    for (const [, tbl] of tables) {
      const fks = db.prepare(`PRAGMA foreign_key_list(${qId(tbl)})`).all() as { id: number; table: string }[]
      const seen = new Set<string>()
      for (const fk of fks) {
        const k = `${tbl}->${fk.table}`
        if (seen.has(k)) continue
        seen.add(k)
        rels.push({
          from_table: tbl,
          to_table: fk.table,
          constraint_name: `fk_${fk.id}`,
        })
      }
    }
    return rels
  }

  async getRowCount(
    connectionId: string,
    tableName: string,
    schema: string | null | undefined,
    whereClause: string | null | undefined,
  ): Promise<number> {
    void schema
    const db = this.getDb(connectionId)
    const where = whereClause?.trim() ? ` WHERE ${whereClause.trim()}` : ""
    const r = db.prepare(`SELECT count(*) AS c FROM ${qId(tableName)}${where}`).get() as { c: number }
    return Number(r.c)
  }

  async getColumnStats(
    connectionId: string,
    tableName: string,
    schema: string | null | undefined,
    columnNames: string[] | null | undefined,
  ): Promise<ColumnStat[]> {
    const db = this.getDb(connectionId)
    const info = await this.describeTable(connectionId, tableName, schema?.trim() || "main")
    if (!info) return []
    let cols = info.columns
    if (columnNames?.length) {
      const set = new Set(columnNames)
      cols = cols.filter((c) => set.has(c.name))
    }
    cols = cols.slice(0, 20)
    const result: ColumnStat[] = []
    const safeTable = qId(tableName)
    for (const col of cols) {
      const cname = qId(col.name)
      try {
        const row = db
          .prepare(`SELECT count(*) AS cnt, count(${cname}) AS nn FROM ${safeTable}`)
          .get() as { cnt: number; nn: number }
        result.push({ column_name: col.name, stat_type: "count", value: row.cnt })
        result.push({ column_name: col.name, stat_type: "non_null", value: row.nn })
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
    void schema
    const db = this.getDb(connectionId)
    const term = `%${searchTerm}%`
    const rows = db
      .prepare(
        `SELECT m.name AS t, p.name AS c FROM sqlite_master m JOIN pragma_table_info(m.name) p WHERE m.type='table' AND (m.name LIKE ? OR p.name LIKE ?)`,
      )
      .all(term, term) as { t: string; c: string }[]
    return rows.map((r) => ["main", r.t, r.c])
  }
}

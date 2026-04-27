import oracledb from "oracledb"
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

oracledb.outFormat = oracledb.OUT_FORMAT_OBJECT
oracledb.autoCommit = true

function parseOracleUrl(raw: string): { user: string; password: string; connectString: string } {
  const u = new URL(raw.replace(/^oracle:/i, "http:"))
  return {
    user: decodeURIComponent(u.username || ""),
    password: decodeURIComponent(u.password || ""),
    connectString: `${u.hostname}:${u.port || "1521"}${u.pathname || "/ORCL"}`,
  }
}

function rowToArray(columns: string[], row: Record<string, unknown>): unknown[] {
  return columns.map((c) => {
    const v = row[c] ?? row[c.toUpperCase()] ?? row[c.toLowerCase()]
    return v
  })
}

function rowsToQueryResult(rows: Record<string, unknown>[]): QueryResult {
  if (!rows.length) return { columns: [], rows: [], row_count: 0 }
  const columns = Object.keys(rows[0]!).map((k) => k)
  const out = rows.map((r) => rowToArray(columns, r))
  return { columns, rows: out, row_count: out.length }
}

function qOra(ident: string): string {
  return `"${String(ident).replaceAll('"', '""')}"`
}

export class OracleAdapter implements SqlEngineAdapter {
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
      const o = parseOracleUrl(cfg.url)
      p = await oracledb.createPool({
        user: o.user,
        password: o.password,
        connectString: o.connectString,
        poolMin: 0,
        poolMax: 4,
      })
      this.pools.set(connectionId, p)
    }
    return p
  }

  async testConnection(connectionId: string): Promise<boolean> {
    try {
      const pool = await this.getPool(connectionId)
      const c = await pool.getConnection()
      try {
        await c.execute("SELECT 1 FROM DUAL")
        return true
      } finally {
        await c.close()
      }
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
      const c = await pool.getConnection()
      try {
        const r = await c.execute("SELECT BANNER FROM V$VERSION WHERE ROWNUM = 1")
        const rows = r.rows as Record<string, unknown>[] | undefined
        const latencyMs = Math.round(performance.now() - t0)
        const banner = rows?.[0]?.BANNER ?? rows?.[0]?.banner
        const version = banner != null ? String(banner).slice(0, 500) : null
        return { ok: true, latencyMs, version }
      } finally {
        await c.close()
      }
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
    if (!/\bFETCH\s+FIRST\s+\d+/i.test(head) && !/\bROWNUM\b/i.test(head)) {
      q = `${q} FETCH FIRST ${maxRows} ROWS ONLY`
    }
    const c = await pool.getConnection()
    try {
      const r = await c.execute(q, [], { maxRows: maxRows + 1 })
      const rows = (r.rows ?? []) as Record<string, unknown>[]
      return rowsToQueryResult(rows)
    } finally {
      await c.close()
    }
  }

  async listTables(connectionId: string, schema?: string | null): Promise<[string, string][]> {
    const pool = await this.getPool(connectionId)
    const c = await pool.getConnection()
    try {
      if (schema?.trim()) {
        const o = schema.trim().toUpperCase()
        const r = await c.execute(
          `SELECT owner AS table_schema, table_name FROM all_tables WHERE owner = :o ORDER BY table_name`,
          { o },
        )
        const rows = (r.rows ?? []) as Record<string, unknown>[]
        return rows.map((x) => [String(x.TABLE_SCHEMA ?? x.owner), String(x.TABLE_NAME ?? x.table_name)])
      }
      const r = await c.execute(
        `SELECT owner AS table_schema, table_name FROM all_tables
         WHERE owner NOT IN ('SYS','SYSTEM','XDB','CTXSYS','MDSYS','ORDSYS','OUTLN','WMSYS','AUDSYS')
         ORDER BY owner, table_name`,
      )
      const rows = (r.rows ?? []) as Record<string, unknown>[]
      return rows.map((x) => [String(x.TABLE_SCHEMA ?? x.owner), String(x.TABLE_NAME ?? x.table_name)])
    } finally {
      await c.close()
    }
  }

  async describeTable(
    connectionId: string,
    tableName: string,
    schema?: string | null,
  ): Promise<TableInfo | null> {
    const pool = await this.getPool(connectionId)
    const own = (schema?.trim() || "").toUpperCase() || null
    const c = await pool.getConnection()
    try {
      const tn = tableName.toUpperCase()
      const r = own
        ? await c.execute(
            `SELECT owner AS table_schema, table_name, column_name, data_type, nullable
             FROM all_tab_columns WHERE owner = :o AND table_name = :t ORDER BY column_id`,
            { o: own, t: tn },
          )
        : await c.execute(
            `SELECT 'USER' AS table_schema, table_name, column_name, data_type, nullable
             FROM user_tab_columns WHERE table_name = :t ORDER BY column_id`,
            { t: tn },
          )
      const rows = (r.rows ?? []) as Record<string, unknown>[]
      if (!rows.length) return null
      const row0 = rows[0]!
      const columns: ColumnInfo[] = rows.map((x) => ({
        name: String(x.COLUMN_NAME ?? x.column_name),
        data_type: String(x.DATA_TYPE ?? x.data_type),
        nullable: String(x.NULLABLE ?? x.nullable).toUpperCase() === "Y",
      }))
      return {
        schema_name: String(row0.TABLE_SCHEMA ?? row0.table_schema ?? "USER"),
        table_name: String(row0.TABLE_NAME ?? row0.table_name ?? tableName),
        columns,
      }
    } finally {
      await c.close()
    }
  }

  async listDatabases(connectionId: string): Promise<string[]> {
    void connectionId
    return []
  }

  async getTableSample(
    connectionId: string,
    tableName: string,
    schema: string | null | undefined,
    limit: number,
    offset: number,
  ): Promise<QueryResult> {
    const pool = await this.getPool(connectionId)
    const lim = Math.max(1, Math.min(limit, 50_000))
    const off = Math.max(0, Math.min(offset, 1_000_000))
    const own = schema?.trim()
    const from = own ? `${qOra(own)}.${qOra(tableName)}` : qOra(tableName)
    const q = `SELECT * FROM ${from} OFFSET ${off} ROWS FETCH NEXT ${lim} ROWS ONLY`
    const c = await pool.getConnection()
    try {
      const r = await c.execute(q, [], { maxRows: lim + 1 })
      return rowsToQueryResult((r.rows ?? []) as Record<string, unknown>[])
    } finally {
      await c.close()
    }
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
          const c = await pool.getConnection()
          try {
            const r = await c.execute(`SELECT count(*) AS c FROM ${qOra(sch)}.${qOra(tbl)}`)
            const row = (r.rows as Record<string, unknown>[] | undefined)?.[0]
            rowCount = Number(row?.C ?? row?.c ?? 0)
          } finally {
            await c.close()
          }
        } catch {
          rowCount = null
        }
      }
      const info = await this.describeTable(connectionId, tbl, sch)
      out.push({
        schema_name: sch,
        table_name: tbl,
        column_count: info?.columns.length ?? 0,
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
    void connectionId
    return `Oracle: use EXPLAIN PLAN no cliente nativo. Prévia: ${query.trim().slice(0, 200)}`
  }

  async validateQuerySql(connectionId: string, query: string): Promise<boolean> {
    try {
      await this.executeReadOnly(connectionId, query.replace(/;\s*$/, ""), 1, 30)
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
    const own = schema?.trim().toUpperCase() || null
    const c = await pool.getConnection()
    try {
      const r = own
        ? await c.execute(
            `SELECT index_name, column_name, uniqueness FROM all_ind_columns ic
             JOIN all_indexes i ON ic.index_owner = i.owner AND ic.index_name = i.index_name
             WHERE ic.table_owner = :o AND ic.table_name = :t ORDER BY ic.index_name, ic.column_position`,
            { o: own, t: tableName.toUpperCase() },
          )
        : await c.execute(
            `SELECT index_name, column_name, uniqueness FROM user_ind_columns ic
             JOIN user_indexes i ON ic.index_name = i.index_name
             WHERE ic.table_name = :t ORDER BY ic.index_name, ic.column_position`,
            { t: tableName.toUpperCase() },
          )
      const rows = (r.rows ?? []) as Record<string, unknown>[]
      const by = new Map<string, IndexInfo>()
      for (const row of rows) {
        const iname = String(row.INDEX_NAME ?? row.index_name)
        let x = by.get(iname)
        if (!x) {
          const u = String(row.UNIQUENESS ?? row.uniqueness ?? "").toUpperCase()
          x = { index_name: iname, columns: [], is_unique: u === "UNIQUE" }
          by.set(iname, x)
        }
        x.columns.push(String(row.COLUMN_NAME ?? row.column_name))
      }
      return [...by.values()]
    } finally {
      await c.close()
    }
  }

  async listViews(connectionId: string, schema: string | null | undefined): Promise<ViewInfo[]> {
    const pool = await this.getPool(connectionId)
    const c = await pool.getConnection()
    try {
      const r = schema?.trim()
        ? await c.execute(
            `SELECT owner AS table_schema, view_name AS table_name, text AS view_definition FROM all_views WHERE owner = :o`,
            { o: schema.trim().toUpperCase() },
          )
        : await c.execute(`SELECT 'USER' AS table_schema, view_name AS table_name, text AS view_definition FROM user_views`)
      const rows = (r.rows ?? []) as Record<string, unknown>[]
      return rows.map((x) => ({
        schema_name: String(x.TABLE_SCHEMA ?? x.table_schema ?? "USER"),
        view_name: String(x.TABLE_NAME ?? x.view_name ?? x.VIEW_NAME),
        definition: x.VIEW_DEFINITION != null ? String(x.VIEW_DEFINITION) : x.view_definition != null ? String(x.view_definition) : null,
      }))
    } finally {
      await c.close()
    }
  }

  async getForeignKeys(
    connectionId: string,
    tableName: string,
    schema: string | null | undefined,
  ): Promise<ForeignKeyInfo[]> {
    void connectionId
    void tableName
    void schema
    return []
  }

  async getTableRelationships(
    connectionId: string,
    schema: string | null | undefined,
  ): Promise<TableRelationship[]> {
    void connectionId
    void schema
    return []
  }

  async getRowCount(
    connectionId: string,
    tableName: string,
    schema: string | null | undefined,
    whereClause: string | null | undefined,
  ): Promise<number> {
    const pool = await this.getPool(connectionId)
    const from =
      schema?.trim() ? `${qOra(schema.trim())}.${qOra(tableName)}` : qOra(tableName)
    const where = whereClause?.trim() ? ` WHERE ${whereClause.trim()}` : ""
    const c = await pool.getConnection()
    try {
      const r = await c.execute(`SELECT count(*) AS c FROM ${from}${where}`)
      const row = (r.rows as Record<string, unknown>[] | undefined)?.[0]
      return Number(row?.C ?? row?.c ?? 0)
    } finally {
      await c.close()
    }
  }

  async getColumnStats(
    connectionId: string,
    tableName: string,
    schema: string | null | undefined,
    columnNames: string[] | null | undefined,
  ): Promise<ColumnStat[]> {
    const info = await this.describeTable(connectionId, tableName, schema ?? undefined)
    if (!info) return []
    let cols = info.columns
    if (columnNames?.length) {
      const set = new Set(columnNames)
      cols = cols.filter((c) => set.has(c.name))
    }
    return cols.slice(0, 5).map((c) => ({ column_name: c.name, stat_type: "data_type", value: c.data_type }))
  }

  async suggestTables(
    connectionId: string,
    searchTerm: string,
    schema: string | null | undefined,
  ): Promise<[string, string, string][]> {
    const pool = await this.getPool(connectionId)
    const term = `%${searchTerm.toUpperCase()}%`
    const c = await pool.getConnection()
    try {
      const r = schema?.trim()
        ? await c.execute(
            `SELECT owner, table_name, column_name FROM all_tab_columns WHERE owner = :o AND (table_name LIKE :t OR column_name LIKE :t2)`,
            { o: schema.trim().toUpperCase(), t: term, t2: term },
          )
        : await c.execute(
            `SELECT 'USER' AS owner, table_name, column_name FROM user_tab_columns WHERE table_name LIKE :t OR column_name LIKE :t2`,
            { t: term, t2: term },
          )
      const rows = (r.rows ?? []) as Record<string, unknown>[]
      return rows.map((x) => [
        String(x.OWNER ?? x.owner ?? "USER"),
        String(x.TABLE_NAME ?? x.table_name),
        String(x.COLUMN_NAME ?? x.column_name),
      ])
    } finally {
      await c.close()
    }
  }
}

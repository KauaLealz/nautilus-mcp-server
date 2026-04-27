import type {
  ColumnStat,
  ForeignKeyInfo,
  IndexInfo,
  QueryResult,
  SchemaTableSummary,
  TableInfo,
  TableRelationship,
  ViewInfo,
} from "../shared/types.js"

export type SchemaExport = {
  tables: {
    schema: string
    table: string
    columns: { name: string; data_type: string; nullable: boolean }[]
  }[]
}

export type SqlProbeResult = {
  ok: boolean
  latencyMs: number
  version: string | null
  error?: string
}

export interface SqlEngineAdapter {
  testConnection(connectionId: string): Promise<boolean>
  probeConnection(connectionId: string): Promise<SqlProbeResult>
  executeReadOnly(
    connectionId: string,
    query: string,
    maxRows: number,
    timeoutSeconds: number,
  ): Promise<QueryResult>
  listTables(connectionId: string, schema?: string | null): Promise<[string, string][]>
  describeTable(
    connectionId: string,
    tableName: string,
    schema?: string | null,
  ): Promise<TableInfo | null>
  listDatabases(connectionId: string): Promise<string[]>
  getTableSample(
    connectionId: string,
    tableName: string,
    schema: string | null | undefined,
    limit: number,
    offset: number,
  ): Promise<QueryResult>
  getSchemaSummary(
    connectionId: string,
    schema: string | null | undefined,
    includeRowCount: boolean,
  ): Promise<SchemaTableSummary[]>
  exportSchemaJson(connectionId: string, schema: string | null | undefined): Promise<SchemaExport>
  explainQuerySql(connectionId: string, query: string): Promise<string>
  validateQuerySql(connectionId: string, query: string): Promise<boolean>
  listIndexes(
    connectionId: string,
    tableName: string,
    schema: string | null | undefined,
  ): Promise<IndexInfo[]>
  listViews(connectionId: string, schema: string | null | undefined): Promise<ViewInfo[]>
  getForeignKeys(
    connectionId: string,
    tableName: string,
    schema: string | null | undefined,
  ): Promise<ForeignKeyInfo[]>
  getTableRelationships(
    connectionId: string,
    schema: string | null | undefined,
  ): Promise<TableRelationship[]>
  getRowCount(
    connectionId: string,
    tableName: string,
    schema: string | null | undefined,
    whereClause: string | null | undefined,
  ): Promise<number>
  getColumnStats(
    connectionId: string,
    tableName: string,
    schema: string | null | undefined,
    columnNames: string[] | null | undefined,
  ): Promise<ColumnStat[]>
  suggestTables(
    connectionId: string,
    searchTerm: string,
    schema: string | null | undefined,
  ): Promise<[string, string, string][]>
}

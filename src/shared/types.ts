export type ConnectionInfo = {
  connection_id: string
  type: string
  read_only: boolean
}

export type ColumnInfo = {
  name: string
  data_type: string
  nullable: boolean
}

export type TableInfo = {
  schema_name: string
  table_name: string
  columns: ColumnInfo[]
}

export type QueryResult = {
  columns: string[]
  rows: unknown[][]
  row_count: number
}

export type IndexInfo = {
  index_name: string
  columns: string[]
  is_unique: boolean
}

export type ViewInfo = {
  schema_name: string
  view_name: string
  definition: string | null
}

export type ForeignKeyInfo = {
  constraint_name: string
  from_schema: string
  from_table: string
  from_columns: string[]
  to_schema: string
  to_table: string
  to_columns: string[]
}

export type TableRelationship = {
  from_table: string
  to_table: string
  constraint_name: string
}

export type SchemaTableSummary = {
  schema_name: string
  table_name: string
  column_count: number
  row_count: number | null
}

export type ColumnStat = {
  column_name: string
  stat_type: string
  value: unknown
}

export type ConnectionServerProbe = {
  ok: boolean
  latency_ms: number
  server_version: string | null
  error?: string
}

export type RedisKeysPage = {
  keys: string[]
  next_cursor: string | null
  truncated: boolean
  pattern: string
  max_keys: number
}

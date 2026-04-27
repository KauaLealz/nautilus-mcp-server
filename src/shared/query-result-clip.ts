import type { QueryResult } from "./types.js"

export function clipQueryResult(result: QueryResult, maxRows: number): QueryResult {
  if (result.rows.length <= maxRows) return result
  return {
    columns: result.columns,
    rows: result.rows.slice(0, maxRows),
    row_count: maxRows,
  }
}

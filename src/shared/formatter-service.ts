import type { QueryResult } from "./types.js"

function cellStr(value: unknown): string {
  if (value === null || value === undefined) return ""
  const s = String(value)
    .replaceAll("\n", " ")
    .replaceAll("\r", " ")
    .trim()
  return s.length > 200 ? `${s.slice(0, 200)}...` : s
}

export class FormatterService {
  static formatQueryResult(result: QueryResult, maxDisplayRows = 100): string {
    if (!result.columns.length) return "Nenhuma coluna retornada."
    const header = result.columns.map(String).join(" | ")
    const sep = result.columns.map(() => "---").join(" | ")
    const rows = result.rows.slice(0, maxDisplayRows)
    const body = rows.map((row) => row.map(cellStr).join(" | ")).join("\n")
    let truncated = ""
    if (result.row_count > maxDisplayRows) {
      truncated = `\n\n... (${result.row_count - maxDisplayRows} linhas omitidas. Total: ${result.row_count} linhas.)`
    }
    return `${header}\n${sep}\n${body}${truncated}`
  }

  static queryResultToCsv(result: QueryResult): string {
    const esc = (v: unknown) => {
      const s = cellStr(v)
      if (/[",\n]/.test(s)) return `"${s.replaceAll('"', '""')}"`
      return s
    }
    const header = result.columns.map(esc).join(",")
    const body = result.rows.map((row) => row.map(esc).join(",")).join("\n")
    return body ? `${header}\n${body}` : header
  }

  static queryResultToJson(result: QueryResult): string {
    const rows = result.rows.map((row) => {
      const o: Record<string, unknown> = {}
      for (let i = 0; i < result.columns.length; i++) {
        o[result.columns[i]!] = row[i]
      }
      return o
    })
    return JSON.stringify(rows, null, 2)
  }

  static formatMongoDocuments(docs: unknown[], maxDisplay = 50): string {
    if (!docs.length) return "Nenhum documento encontrado."
    const lines: string[] = []
    const slice = docs.slice(0, maxDisplay)
    for (let i = 0; i < slice.length; i++) {
      try {
        lines.push(JSON.stringify(slice[i], null, 2))
      } catch {
        lines.push(String(slice[i]))
      }
      if (i < slice.length - 1) lines.push("---")
    }
    if (docs.length > maxDisplay) {
      lines.push(`\n... (${docs.length - maxDisplay} documentos omitidos. Total: ${docs.length}).`)
    }
    return lines.join("\n")
  }

  static formatRedisKeys(keys: string[]): string {
    if (!keys.length) return "Nenhuma chave encontrada."
    return keys.map((k) => `  - ${k}`).join("\n")
  }
}

export class QuerySafetyError extends Error {
  readonly reason: string

  constructor(message: string, reason = "QUERY_NOT_ALLOWED") {
    super(message)
    this.name = "QuerySafetyError"
    this.reason = reason
  }
}

const ALLOWED_FIRST = new Set(["select", "with"])

const BLOCKLIST = new Set([
  "insert",
  "update",
  "delete",
  "drop",
  "alter",
  "truncate",
  "create",
  "replace",
  "grant",
  "revoke",
  "exec",
  "execute",
  "declare",
  "cursor",
  "begin",
  "commit",
  "rollback",
  "copy",
  "vacuum",
  "reindex",
  "cluster",
  "lock",
  "unlock",
])

const PREFIX_BLOCK = ["xp_", "sp_"] as const

export class SqlQueryValidator {
  readonly maxLength: number
  readonly maxRowsCap: number

  constructor(opts?: { maxLength?: number; maxRowsCap?: number }) {
    this.maxLength = opts?.maxLength ?? 2000
    this.maxRowsCap = opts?.maxRowsCap ?? 200
  }

  private checkRowLimitOrRaise(query: string): void {
    const q = query.trim().toUpperCase()
    const qNoComments = q
      .split("\n")
      .map((line) => line.split("--")[0] ?? "")
      .join("\n")
    const limitM = qNoComments.match(/\bLIMIT\s+(\d+)\b/i)
    if (limitM) {
      const n = parseInt(limitM[1]!, 10)
      if (n > this.maxRowsCap) {
        throw new QuerySafetyError(
          `LIMIT ${n} excede o máximo permitido de ${this.maxRowsCap} linhas. Use LIMIT ${this.maxRowsCap} ou menos.`,
          "ROW_LIMIT_EXCEEDED",
        )
      }
    }
    const fetchM = qNoComments.match(/\bFETCH\s+FIRST\s+(\d+)\s+ROWS?\b/i)
    if (fetchM) {
      const n = parseInt(fetchM[1]!, 10)
      if (n > this.maxRowsCap) {
        throw new QuerySafetyError(
          `FETCH FIRST ${n} ROWS excede o máximo de ${this.maxRowsCap} linhas.`,
          "ROW_LIMIT_EXCEEDED",
        )
      }
    }
    const topM = qNoComments.match(/\bTOP\s+(\d+)\b/i)
    if (topM) {
      const n = parseInt(topM[1]!, 10)
      if (n > this.maxRowsCap) {
        throw new QuerySafetyError(
          `TOP ${n} excede o máximo permitido de ${this.maxRowsCap} linhas.`,
          "ROW_LIMIT_EXCEEDED",
        )
      }
    }
    const rownumM = qNoComments.match(/\bROWNUM\s*[<>=]+\s*(\d+)\b/i)
    if (rownumM) {
      const n = parseInt(rownumM[1]!, 10)
      if (n > this.maxRowsCap) {
        throw new QuerySafetyError(
          `ROWNUM <= ${n} excede o máximo de ${this.maxRowsCap} linhas.`,
          "ROW_LIMIT_EXCEEDED",
        )
      }
    }
  }

  isAllowed(query: string): boolean {
    try {
      this.sanitizeOrRaise(query)
      return true
    } catch {
      return false
    }
  }

  sanitizeOrRaise(query: string): string {
    if (!query?.trim()) {
      throw new QuerySafetyError("Query vazia não é permitida.", "EMPTY_QUERY")
    }
    const q = query.trim()
    if (q.length > this.maxLength) {
      throw new QuerySafetyError(
        `Query excede o tamanho máximo permitido de ${this.maxLength} caracteres.`,
        "QUERY_TOO_LONG",
      )
    }
    if (/;\s*\S/.test(q)) {
      throw new QuerySafetyError(
        "Múltiplos statements não são permitidos (use apenas uma query).",
        "MULTIPLE_STATEMENTS",
      )
    }
    const tokens = q.toLowerCase().match(/\b[\w_]+\b/g)
    if (!tokens?.length) {
      throw new QuerySafetyError("Query sem tokens válidos.", "INVALID_QUERY")
    }
    const first = tokens[0]!
    if (!ALLOWED_FIRST.has(first)) {
      throw new QuerySafetyError(
        `Apenas SELECT ou WITH (CTE seguida de leitura) são permitidos. Início da query: '${first}'.`,
        "WRITE_NOT_ALLOWED",
      )
    }
    for (const token of tokens) {
      for (const blocked of BLOCKLIST) {
        if (token === blocked) {
          throw new QuerySafetyError(`Palavra-chave proibida detectada: '${token}'.`, "BLOCKLIST_KEYWORD")
        }
      }
      for (const pref of PREFIX_BLOCK) {
        if (token.startsWith(pref)) {
          throw new QuerySafetyError(`Palavra-chave proibida detectada: '${token}'.`, "BLOCKLIST_KEYWORD")
        }
      }
    }
    this.checkRowLimitOrRaise(q)
    return q
  }
}

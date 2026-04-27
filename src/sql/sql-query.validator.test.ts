import { describe, expect, it } from "vitest"
import { QuerySafetyError, SqlQueryValidator } from "./sql-query.validator.js"

describe("SqlQueryValidator", () => {
  const v = new SqlQueryValidator({ maxLength: 500, maxRowsCap: 100 })

  it("permite SELECT simples", () => {
    expect(v.sanitizeOrRaise("SELECT 1")).toBe("SELECT 1")
  })

  it("permite WITH", () => {
    expect(v.sanitizeOrRaise("WITH a AS (SELECT 1) SELECT * FROM a")).toContain("WITH")
  })

  it("rejeita INSERT", () => {
    expect(() => v.sanitizeOrRaise("INSERT INTO t VALUES (1)")).toThrow(QuerySafetyError)
  })

  it("rejeita múltiplos statements", () => {
    expect(() => v.sanitizeOrRaise("SELECT 1; SELECT 2")).toThrow(QuerySafetyError)
  })

  it("rejeita query vazia", () => {
    expect(() => v.sanitizeOrRaise("   ")).toThrow(QuerySafetyError)
  })

  it("rejeita LIMIT acima do cap", () => {
    expect(() => v.sanitizeOrRaise("SELECT * FROM t LIMIT 999")).toThrow(QuerySafetyError)
  })
})

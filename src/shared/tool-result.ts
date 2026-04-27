import type { CallToolResult } from "@modelcontextprotocol/sdk/types.js"

export function textResult(text: string): CallToolResult {
  return { content: [{ type: "text", text }] }
}

export function textAndStructured(
  text: string,
  structured: Record<string, unknown>,
): CallToolResult {
  return {
    content: [{ type: "text", text }],
    structuredContent: structured,
  }
}

export type ToolSuccessPayload = string | { text: string; structured: Record<string, unknown> }

export function toCallToolResult(payload: ToolSuccessPayload): CallToolResult {
  if (typeof payload === "string") return textResult(payload)
  return textAndStructured(payload.text, payload.structured)
}

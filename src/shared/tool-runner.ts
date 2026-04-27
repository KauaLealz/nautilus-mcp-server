import type { CallToolResult } from "@modelcontextprotocol/sdk/types.js"
import { ErrorService } from "./error-service.js"
import { QuerySafetyError } from "../sql/sql-query.validator.js"
import { toCallToolResult, type ToolSuccessPayload } from "./tool-result.js"

export async function safeTool(
  context: string,
  fn: () => Promise<ToolSuccessPayload>,
): Promise<CallToolResult> {
  try {
    return toCallToolResult(await fn())
  } catch (e) {
    if (e instanceof QuerySafetyError) return toCallToolResult(e.message)
    return toCallToolResult(ErrorService.formatForAgent(ErrorService.handle(e, context)))
  }
}

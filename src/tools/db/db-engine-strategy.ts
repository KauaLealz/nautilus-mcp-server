import type { ToolSuccessPayload } from "../../shared/tool-result.js"

export type DbResourceListParams = {
  schema?: string
  database?: string
  keyPattern?: string
  cursor?: string | null
}

export type DbSidecarParams = {
  schema?: string
  database?: string
}

export interface DbEngineStrategy {
  listResources(
    resolvedId: string,
    rawConnectionId: string,
    engineLabel: string,
    p: DbResourceListParams,
    cap: number,
  ): Promise<ToolSuccessPayload>

  getMetadata(
    resolvedId: string,
    rawConnectionId: string,
    engineLabel: string,
    resourceName: string,
    p: DbSidecarParams,
    maxSampleRows: number,
  ): Promise<ToolSuccessPayload>

  describeIndexes(
    resolvedId: string,
    rawConnectionId: string,
    engineLabel: string,
    tableName: string,
    p: DbSidecarParams,
  ): Promise<ToolSuccessPayload>

  peekSample(
    resolvedId: string,
    rawConnectionId: string,
    resourceName: string,
    p: DbSidecarParams,
    peekLimit: number,
  ): Promise<ToolSuccessPayload>
}

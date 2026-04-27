import { QuerySafetyError } from "../sql/sql-query.validator.js"

export type ErrorInfo = {
  type: string
  message: string
  context: string
  user_message: string
}

export class ErrorService {
  static handle(error: unknown, context = ""): ErrorInfo {
    if (error instanceof QuerySafetyError) {
      return {
        type: "QUERY_SAFETY_ERROR",
        message: error.message,
        context,
        user_message: error.message,
      }
    }
    if (error instanceof Error) {
      const message = error.message
      if (error.name === "QuerySafetyError") {
        return {
          type: "QUERY_SAFETY_ERROR",
          message,
          context,
          user_message: message,
        }
      }
      if (message.includes("connect") || message.includes("ECONNREFUSED") || message.includes("timeout")) {
        return {
          type: "CONNECTION_ERROR",
          message: `Falha de conexão: ${message}`,
          context,
          user_message:
            "Não foi possível conectar ao banco. Verifique se o connection_id está correto e se o serviço está acessível.",
        }
      }
      if (error.name === "TimeoutError") {
        return {
          type: "TIMEOUT_ERROR",
          message,
          context,
          user_message:
            "A operação excedeu o tempo limite. Tente uma query mais simples ou aumente o timeout.",
        }
      }
      if (error instanceof TypeError || error instanceof SyntaxError) {
        return {
          type: "VALUE_ERROR",
          message,
          context,
          user_message: `Parâmetro inválido: ${message}`,
        }
      }
      if (message.startsWith("Conexão não encontrada") || message.includes("not found")) {
        return {
          type: "KEY_ERROR",
          message,
          context,
          user_message: `Configuração ou recurso não encontrado: ${message}`,
        }
      }
      return {
        type: "ERROR",
        message,
        context,
        user_message:
          "Ocorreu um erro ao processar a solicitação. Verifique os parâmetros e tente novamente.",
      }
    }
    return {
      type: "ERROR",
      message: String(error),
      context,
      user_message:
        "Ocorreu um erro ao processar a solicitação. Verifique os parâmetros e tente novamente.",
    }
  }

  static formatForAgent(info: ErrorInfo, includeDetails = false): string {
    let msg = info.user_message || info.message || "Erro desconhecido."
    if (includeDetails && info.context) {
      msg += ` (Contexto: ${info.context})`
    }
    return msg
  }
}

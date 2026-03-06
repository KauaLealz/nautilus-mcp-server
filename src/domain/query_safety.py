"""
Validação de segurança para queries SQL.
Protege contra comandos destrutivos e reduz superfície de alucinação.
"""
import re
from dataclasses import dataclass


@dataclass
class QuerySafetyError(Exception):
    """Erro de validação de segurança da query."""

    message: str
    reason: str = "QUERY_NOT_ALLOWED"

    def __str__(self) -> str:
        return self.message


class SqlQueryValidator:
    """
    Validador de queries SQL: allowlist de comandos, blocklist de palavras-chave e limites.
    """

    # Comandos permitidos (apenas leitura). WITH é permitido para CTEs usados em SELECT.
    ALLOWED_FIRST_TOKENS = {"select", "with"}

    # Palavras-chave que indicam escrita ou DDL (blocklist em qualquer lugar da query)
    BLOCKLIST_KEYWORDS = frozenset({
        "insert", "update", "delete", "drop", "alter", "truncate",
        "create", "replace", "grant", "revoke", "exec", "execute",
        "xp_", "sp_", "declare", "cursor", "begin", "commit", "rollback",
        "copy", "vacuum", "reindex", "cluster", "lock", "unlock",
    })

    def __init__(
        self,
        *,
        max_length: int = 2000,
        allow_write: bool = False,
        max_rows_cap: int = 500,
    ):
        self.max_length = max_length
        self.allow_write = allow_write
        self.max_rows_cap = max_rows_cap

    def _check_row_limit_or_raise(self, query: str) -> None:
        """
        Verifica se a query tem LIMIT/TOP/FETCH FIRST/ROWNUM com valor acima do cap.
        Barra na fase dry (antes de executar) para não travar a base nem retornar muitas linhas.
        """
        q = query.strip().upper()
        # Remove comentários de linha para não confundir
        q_no_comments = "\n".join(
            line.split("--")[0] for line in q.split("\n")
        )
        # LIMIT n (MySQL, PostgreSQL)
        m = re.search(r"\bLIMIT\s+(\d+)\b", q_no_comments, re.IGNORECASE)
        if m:
            n = int(m.group(1))
            if n > self.max_rows_cap:
                raise QuerySafetyError(
                    f"LIMIT {n} excede o máximo permitido de {self.max_rows_cap} linhas. Use LIMIT {self.max_rows_cap} ou menos.",
                    reason="ROW_LIMIT_EXCEEDED",
                )
        # FETCH FIRST n ROWS (Oracle 12c+)
        m = re.search(r"\bFETCH\s+FIRST\s+(\d+)\s+ROWS?\b", q_no_comments, re.IGNORECASE)
        if m:
            n = int(m.group(1))
            if n > self.max_rows_cap:
                raise QuerySafetyError(
                    f"FETCH FIRST {n} ROWS excede o máximo de {self.max_rows_cap} linhas.",
                    reason="ROW_LIMIT_EXCEEDED",
                )
        # TOP n (SQL Server)
        m = re.search(r"\bTOP\s+(\d+)\b", q_no_comments, re.IGNORECASE)
        if m:
            n = int(m.group(1))
            if n > self.max_rows_cap:
                raise QuerySafetyError(
                    f"TOP {n} excede o máximo permitido de {self.max_rows_cap} linhas.",
                    reason="ROW_LIMIT_EXCEEDED",
                )
        # ROWNUM <= n ou ROWNUM < n (Oracle)
        m = re.search(r"\bROWNUM\s*[<>=]+\s*(\d+)\b", q_no_comments, re.IGNORECASE)
        if m:
            n = int(m.group(1))
            if n > self.max_rows_cap:
                raise QuerySafetyError(
                    f"ROWNUM <= {n} excede o máximo de {self.max_rows_cap} linhas.",
                    reason="ROW_LIMIT_EXCEEDED",
                )

    def is_allowed(self, query: str) -> bool:
        """Retorna True se a query passou na validação."""
        try:
            self.sanitize_or_raise(query)
            return True
        except QuerySafetyError:
            return False

    def sanitize_or_raise(self, query: str) -> str:
        """
        Valida a query e retorna ela normalizada (strip).
        Levanta QuerySafetyError se for insegura.
        """
        if not query or not query.strip():
            raise QuerySafetyError("Query vazia não é permitida.", reason="EMPTY_QUERY")

        q = query.strip()
        if len(q) > self.max_length:
            raise QuerySafetyError(
                f"Query excede o tamanho máximo permitido de {self.max_length} caracteres.",
                reason="QUERY_TOO_LONG",
            )

        # Múltiplos statements (; seguido de mais conteúdo)
        if re.search(r";\s*\S", q):
            raise QuerySafetyError(
                "Múltiplos statements não são permitidos (use apenas uma query).",
                reason="MULTIPLE_STATEMENTS",
            )

        # Tokenização simples: palavras separadas por espaço, parênteses, vírgula, etc.
        tokens = re.findall(r"\b[\w_]+\b", q.lower())
        if not tokens:
            raise QuerySafetyError("Query sem tokens válidos.", reason="INVALID_QUERY")

        first = tokens[0]
        if first not in self.ALLOWED_FIRST_TOKENS and not self.allow_write:
            raise QuerySafetyError(
                f"Apenas comandos de leitura (SELECT, WITH) são permitidos. Início da query: '{first}'.",
                reason="WRITE_NOT_ALLOWED",
            )

        for token in tokens:
            for blocked in self.BLOCKLIST_KEYWORDS:
                if blocked.endswith("_"):
                    if token.startswith(blocked):
                        raise QuerySafetyError(
                            f"Palavra-chave proibida detectada: '{token}'.",
                            reason="BLOCKLIST_KEYWORD",
                        )
                elif token == blocked:
                    raise QuerySafetyError(
                        f"Palavra-chave proibida detectada: '{token}'.",
                        reason="BLOCKLIST_KEYWORD",
                    )

        self._check_row_limit_or_raise(q)
        return q

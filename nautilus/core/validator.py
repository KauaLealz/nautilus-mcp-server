import re

class QuerySafetyError(Exception):
    def __init__(self, message, reason="QUERY_NOT_ALLOWED"):
        super().__init__(message)
        self.reason = reason

ALLOWED_FIRST = {"select", "with"}

BLOCKLIST = {
    "insert", "update", "delete", "drop", "alter", "truncate", "create",
    "replace", "grant", "revoke", "exec", "execute", "declare", "cursor",
    "begin", "commit", "rollback", "copy", "vacuum", "reindex", "cluster",
    "lock", "unlock"
}

PREFIX_BLOCK = ["xp_", "sp_"]

class SqlQueryValidator:
    def __init__(self, max_length=2000, max_rows_cap=200):
        self.max_length = max_length
        self.max_rows_cap = max_rows_cap

    def check_row_limit_or_raise(self, query: str) -> None:
        q = query.strip().upper()
        lines = []
        for line in q.split("\n"):
            parts = line.split("--")
            lines.append(parts[0] if parts else "")
        q_no_comments = "\n".join(lines)

        limit_m = re.search(r"\bLIMIT\s+(\d+)\b", q_no_comments, re.IGNORECASE)
        if limit_m:
            n = int(limit_m.group(1))
            if n > self.max_rows_cap:
                raise QuerySafetyError(
                    f"LIMIT {n} excede o máximo permitido de {self.max_rows_cap} linhas. Use LIMIT {self.max_rows_cap} ou menos.",
                    "ROW_LIMIT_EXCEEDED"
                )

        fetch_m = re.search(r"\bFETCH\s+FIRST\s+(\d+)\s+ROWS?\b", q_no_comments, re.IGNORECASE)
        if fetch_m:
            n = int(fetch_m.group(1))
            if n > self.max_rows_cap:
                raise QuerySafetyError(
                    f"FETCH FIRST {n} ROWS excede o máximo de {self.max_rows_cap} linhas.",
                    "ROW_LIMIT_EXCEEDED"
                )

        top_m = re.search(r"\bTOP\s+(\d+)\b", q_no_comments, re.IGNORECASE)
        if top_m:
            n = int(top_m.group(1))
            if n > self.max_rows_cap:
                raise QuerySafetyError(
                    f"TOP {n} excede o máximo permitido de {self.max_rows_cap} linhas.",
                    "ROW_LIMIT_EXCEEDED"
                )

        rownum_m = re.search(r"\bROWNUM\s*[<>=]+\s*(\d+)\b", q_no_comments, re.IGNORECASE)
        if rownum_m:
            n = int(rownum_m.group(1))
            if n > self.max_rows_cap:
                raise QuerySafetyError(
                    f"ROWNUM <= {n} excede o máximo de {self.max_rows_cap} linhas.",
                    "ROW_LIMIT_EXCEEDED"
                )

    def is_allowed(self, query: str) -> bool:
        try:
            self.sanitize_or_raise(query)
            return True
        except Exception:
            return False

    def sanitize_or_raise(self, query: str) -> str:
        if not query or not query.strip():
            raise QuerySafetyError("Query vazia não é permitida.", "EMPTY_QUERY")
        q = query.strip()
        if len(q) > self.max_length:
            raise QuerySafetyError(
                f"Query excede o tamanho máximo permitido de {self.max_length} caracteres.",
                "QUERY_TOO_LONG"
            )
        if re.search(r";\s*\S", q):
            raise QuerySafetyError(
                "Múltiplos statements não são permitidos (use apenas uma query).",
                "MULTIPLE_STATEMENTS"
            )
        tokens = re.findall(r"\b[\w_]+\b", q.lower())
        if not tokens:
            raise QuerySafetyError("Query sem tokens válidos.", "INVALID_QUERY")
        first = tokens[0]
        if first not in ALLOWED_FIRST:
            raise QuerySafetyError(
                f"Apenas SELECT ou WITH (CTE seguida de leitura) são permitidos. Início da query: '{first}'.",
                "WRITE_NOT_ALLOWED"
            )
        for token in tokens:
            if token in BLOCKLIST:
                raise QuerySafetyError(f"Palavra-chave proibida detectada: '{token}'.", "BLOCKLIST_KEYWORD")
            for pref in PREFIX_BLOCK:
                if token.startswith(pref):
                    raise QuerySafetyError(f"Palavra-chave proibida detectada: '{token}'.", "BLOCKLIST_KEYWORD")
        self.check_row_limit_or_raise(q)
        return q

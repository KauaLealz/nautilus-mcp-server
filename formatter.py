import json

def cell_str(value) -> str:
    if value is None:
        return ""
    s = str(value).replace("\n", " ").replace("\r", " ").strip()
    return f"{s[:200]}..." if len(s) > 200 else s

class FormatterService:
    @staticmethod
    def format_query_result(columns, rows, total_row_count, max_display_rows=100) -> str:
        if not columns:
            return "Nenhuma coluna retornada."
        header = " | ".join(map(str, columns))
        sep = " | ".join(["---"] * len(columns))
        display_rows = rows[:max_display_rows]
        body = "\n".join(" | ".join(cell_str(val) for val in row) for row in display_rows)
        truncated = ""
        if total_row_count > max_display_rows:
            truncated = f"\n\n... ({total_row_count - max_display_rows} linhas omitidas. Total: {total_row_count} linhas.)"
        return f"{header}\n{sep}\n{body}{truncated}"

    @staticmethod
    def query_result_to_csv(columns, rows) -> str:
        def esc(v):
            s = cell_str(v)
            if any(char in s for char in ('"', ',', '\n')):
                return f'"{s.replace(chr(34), chr(34) + chr(34))}"'
            return s
        header = ",".join(esc(col) for col in columns)
        body = "\n".join(",".join(esc(val) for val in row) for row in rows)
        return f"{header}\n{body}" if body else header

    @staticmethod
    def query_result_to_json(columns, rows) -> str:
        res = []
        for row in rows:
            obj = {}
            for i, col in enumerate(columns):
                if i < len(row):
                    obj[col] = row[i]
            res.append(obj)
        return json.dumps(res, indent=2, default=str)

    @staticmethod
    def format_mongo_documents(docs, max_display=50) -> str:
        if not docs:
            return "Nenhum documento encontrado."
        lines = []
        slice_docs = docs[:max_display]
        for i, doc in enumerate(slice_docs):
            try:
                lines.append(json.dumps(doc, indent=2, default=str))
            except Exception:
                lines.append(str(doc))
            if i < len(slice_docs) - 1:
                lines.append("---")
        if len(docs) > max_display:
            lines.append(f"\n... ({len(docs) - max_display} documentos omitidos. Total: {len(docs)}).")
        return "\n".join(lines)

    @staticmethod
    def format_redis_keys(keys) -> str:
        if not keys:
            return "Nenhuma chave encontrada."
        return "\n".join(f"  - {k}" for k in keys)

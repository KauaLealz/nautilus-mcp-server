"""
Formatação de resultados para o agente MCP.
Tabelas, listas de conexões e metadados de schema em texto legível.
"""
from src.domain.models import (
    ConnectionInfo,
    ForeignKeyInfo,
    IndexInfo,
    QueryResult,
    SchemaTableSummary,
    TableInfo,
    TableRelationship,
    ViewInfo,
)


class ResultFormatter:
    """Formata DTOs do domínio em strings para as tools MCP."""

    @staticmethod
    def format_connections(connections: list[ConnectionInfo]) -> str:
        """Lista de conexões em texto (sem credenciais)."""
        if not connections:
            return "Nenhuma conexão configurada. Configure variáveis DATABASES__<id>__* no .env."
        lines = ["Conexões disponíveis:", ""]
        for c in connections:
            ro = " (somente leitura)" if c.read_only else ""
            lines.append(f"  - {c.connection_id}: tipo={c.type}{ro}")
        return "\n".join(lines)

    @staticmethod
    def format_query_result(result: QueryResult, max_display_rows: int = 100) -> str:
        """Formata QueryResult como tabela em texto (markdown-style)."""
        if not result.columns:
            return "Nenhuma coluna retornada."
        header = " | ".join(str(c) for c in result.columns)
        sep = " | ".join("---" for _ in result.columns)
        rows = result.rows[:max_display_rows]
        body = "\n".join(" | ".join(_cell_str(c) for c in row) for row in rows)
        truncated = ""
        if result.row_count > max_display_rows:
            truncated = f"\n\n... ({result.row_count - max_display_rows} linhas omitidas. Total: {result.row_count} linhas.)"
        return f"{header}\n{sep}\n{body}{truncated}"

    @staticmethod
    def format_tables(schema_table_list: list[tuple[str, str]]) -> str:
        """Lista de (schema, table) em texto."""
        if not schema_table_list:
            return "Nenhuma tabela encontrada."
        lines = ["Tabelas (schema | tabela):", ""]
        for schema, table in sorted(schema_table_list):
            lines.append(f"  - {schema}.{table}")
        return "\n".join(lines)

    @staticmethod
    def format_table_info(info: TableInfo) -> str:
        """Metadados de uma tabela (colunas e tipos)."""
        lines = [f"Tabela: {info.schema_name}.{info.table_name}", ""]
        if not info.columns:
            return "\n".join(lines) + "Nenhuma coluna listada."
        lines.append("Colunas:")
        for col in info.columns:
            null = "NULL" if col.nullable else "NOT NULL"
            lines.append(f"  - {col.name}: {col.data_type} ({null})")
        return "\n".join(lines)


def _cell_str(value: object) -> str:
    """Converte célula para string segura (evita quebra de linha)."""
    if value is None:
        return ""
    s = str(value).replace("\n", " ").replace("\r", " ").strip()
    return s[:200] + "..." if len(s) > 200 else s


def format_mongo_documents(docs: list[dict], max_display: int = 50) -> str:
    """Formata lista de documentos MongoDB para texto (JSON por documento)."""
    import json
    if not docs:
        return "Nenhum documento encontrado."
    lines = []
    for i, doc in enumerate(docs[:max_display]):
        try:
            # Converte ObjectId, datetime, bytes e outros não JSON-serializáveis
            if isinstance(doc, dict):
                sanitized = json.loads(json.dumps(doc, default=str))
            else:
                sanitized = doc
            lines.append(json.dumps(sanitized, ensure_ascii=False, indent=2))
        except (TypeError, ValueError, OverflowError):
            lines.append(str(doc))
        if i < len(docs[:max_display]) - 1:
            lines.append("---")
    if len(docs) > max_display:
        lines.append(f"\n... ({len(docs) - max_display} documentos omitidos. Total: {len(docs)}).")
    return "\n".join(lines)


def format_redis_keys(keys: list[str]) -> str:
    """Formata lista de chaves Redis."""
    if not keys:
        return "Nenhuma chave encontrada."
    return "\n".join(f"  - {k}" for k in keys)


def format_schema_summary(summaries: list[SchemaTableSummary]) -> str:
    if not summaries:
        return "Nenhuma tabela encontrada."
    lines = ["Tabela | Colunas" + (" | Linhas" if any(s.row_count is not None for s in summaries) else ""), ""]
    for s in summaries:
        rc = str(s.row_count) if s.row_count is not None else "-"
        if s.row_count is not None:
            lines.append(f"  {s.schema_name}.{s.table_name} | {s.column_count} | {rc}")
        else:
            lines.append(f"  {s.schema_name}.{s.table_name} | {s.column_count}")
    return "\n".join(lines)


def format_indexes(indexes: list[IndexInfo]) -> str:
    if not indexes:
        return "Nenhum índice encontrado."
    lines = ["Índices:", ""]
    for i in indexes:
        uniq = " (único)" if i.is_unique else ""
        lines.append(f"  - {i.index_name}: {', '.join(i.columns)}{uniq}")
    return "\n".join(lines)


def format_views(views: list[ViewInfo]) -> str:
    if not views:
        return "Nenhuma view encontrada."
    lines = ["Views:", ""]
    for v in views:
        lines.append(f"  - {v.schema_name}.{v.view_name}")
        if v.definition:
            lines.append(f"    Definição: {v.definition[:200]}..." if len(v.definition or "") > 200 else f"    Definição: {v.definition}")
    return "\n".join(lines)


def format_foreign_keys(fks: list[ForeignKeyInfo]) -> str:
    if not fks:
        return "Nenhuma chave estrangeira encontrada."
    lines = ["Chaves estrangeiras:", ""]
    for fk in fks:
        lines.append(f"  - {fk.constraint_name}: ({fk.from_schema}.{fk.from_table}.{', '.join(fk.from_columns)}) -> ({fk.to_schema}.{fk.to_table}.{', '.join(fk.to_columns)})")
    return "\n".join(lines)


def format_table_relationships(rels: list[TableRelationship]) -> str:
    if not rels:
        return "Nenhum relacionamento encontrado."
    lines = ["Relacionamentos (tabela -> tabela):", ""]
    for r in rels:
        lines.append(f"  - {r.from_table} -> {r.to_table} ({r.constraint_name})")
    return "\n".join(lines)


def format_column_stats(stats: list) -> str:
    from src.domain.models import ColumnStat
    if not stats:
        return "Nenhuma estatística disponível."
    by_col: dict[str, list[ColumnStat]] = {}
    for s in stats:
        if s.column_name not in by_col:
            by_col[s.column_name] = []
        by_col[s.column_name].append(s)
    lines = ["Estatísticas por coluna:", ""]
    for col, slist in by_col.items():
        lines.append(f"  {col}:")
        for s in slist:
            lines.append(f"    - {s.stat_type}: {s.value}")
    return "\n".join(lines)


def query_result_to_csv(result: QueryResult) -> str:
    import csv
    import io
    out = io.StringIO()
    w = csv.writer(out)
    w.writerow(result.columns)
    for row in result.rows:
        w.writerow(_cell_str(c) for c in row)
    return out.getvalue()


def query_result_to_json(result: QueryResult) -> str:
    import json
    rows = [dict(zip(result.columns, row)) for row in result.rows]
    return json.dumps(rows, ensure_ascii=False, indent=2, default=str)

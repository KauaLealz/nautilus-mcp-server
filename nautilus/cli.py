"""CLI de gestão das conexões do Nautilus.

Toda adição, alteração e remoção de conexão passa por aqui: o CLI edita o
`nautilus.yaml` e **cifra a senha** na gravação (`enc:...`), usando a chave
`NAUTILUS_KEY`. Comandos: keygen, add, list, edit, remove.

As funções de domínio (add_connection/edit_connection/remove_connection)
operam sobre um dicionário puro — o parse de argumentos e os prompts ficam
isolados em `main`, para manter o núcleo testável sem stdin.
"""

import argparse
import getpass
import os
import sys

import yaml

from nautilus.config import crypto
from nautilus.config.url_builder import SUPPORTED_ENGINES

DEFAULT_CONFIG_PATH = "nautilus.yaml"

_HEADER = (
    "# Gerido pelo CLI `nautilus` — não edite a senha à mão.\n"
    "# As senhas ficam cifradas (enc:...); a chave vive em NAUTILUS_KEY.\n"
)


class CliError(Exception):
    """Erro de uso do CLI — vira mensagem clara e exit != 0."""


# --- IO do YAML ------------------------------------------------------------

def read_yaml(path: str) -> dict:
    if not os.path.exists(path):
        return {}
    with open(path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f)
    return data or {}


def write_yaml(path: str, data: dict) -> None:
    with open(path, "w", encoding="utf-8") as f:
        f.write(_HEADER)
        yaml.safe_dump(data, f, sort_keys=False, allow_unicode=True)


# --- Núcleo (testável) -----------------------------------------------------

def _validate_type(conn_type: str) -> str:
    t = str(conn_type).strip().lower()
    if t not in SUPPORTED_ENGINES:
        raise CliError(
            f"type inválido: '{conn_type}'. Suportados: "
            f"{', '.join(sorted(SUPPORTED_ENGINES))}."
        )
    return t


def add_connection(data: dict, conn_id: str, fields: dict,
                   password_plain: str | None = None, key: str | None = None) -> dict:
    conns = data.setdefault("connections", {})
    if conn_id in conns:
        raise CliError(f"conexão '{conn_id}' já existe — use `nautilus edit {conn_id}`.")
    entry = {"type": _validate_type(fields.get("type", ""))}
    for k in ("url", "host", "port", "user", "database", "path", "read_only"):
        v = fields.get(k)
        if v not in (None, ""):
            entry[k] = v
    if password_plain:
        entry["password"] = crypto.encrypt(password_plain, key)
    conns[conn_id] = entry
    return data


def edit_connection(data: dict, conn_id: str, fields: dict,
                    password_plain: str | None = None, key: str | None = None) -> dict:
    conns = data.get("connections") or {}
    if conn_id not in conns:
        raise CliError(f"conexão '{conn_id}' não existe — use `nautilus add`.")
    entry = conns[conn_id]
    if "type" in fields and fields["type"] not in (None, ""):
        entry["type"] = _validate_type(fields["type"])
    for k in ("url", "host", "port", "user", "database", "path", "read_only"):
        if k in fields and fields[k] not in (None, ""):
            entry[k] = fields[k]
    if password_plain:
        entry["password"] = crypto.encrypt(password_plain, key)
    return data


def remove_connection(data: dict, conn_id: str) -> dict:
    conns = data.get("connections") or {}
    if conn_id not in conns:
        raise CliError(f"conexão '{conn_id}' não existe.")
    del conns[conn_id]
    return data


def list_connections(data: dict) -> list:
    """Resumo por conexão, sem jamais expor a senha."""
    conns = data.get("connections") or {}
    out = []
    for cid, entry in conns.items():
        entry = entry or {}
        out.append({
            "id": cid,
            "type": entry.get("type", "?"),
            "host": entry.get("host") or entry.get("url") or entry.get("path") or "",
            "has_password": bool(entry.get("password")),
        })
    return out


# --- Prompts / handlers do argparse ---------------------------------------

def _prompt(label: str, default: str = "") -> str:
    suffix = f" [{default}]" if default else ""
    val = input(f"{label}{suffix}: ").strip()
    return val or default


def _collect_fields(existing: dict | None = None) -> tuple[dict, str | None]:
    existing = existing or {}
    fields = {}
    fields["type"] = _prompt("type (engine)", existing.get("type", ""))
    if fields["type"] == "sqlite":
        fields["path"] = _prompt("path do arquivo (ou url)", existing.get("path", ""))
        return fields, None
    fields["host"] = _prompt("host", existing.get("host", ""))
    fields["port"] = _prompt("port (vazio = padrão do engine)", str(existing.get("port", "")))
    fields["user"] = _prompt("user", existing.get("user", ""))
    pw = getpass.getpass("senha (vazio = manter atual): ")
    fields["database"] = _prompt("database", existing.get("database", ""))
    return fields, (pw or None)


def _resolve_path(args) -> str:
    return args.config or os.environ.get("NAUTILUS_CONFIG", DEFAULT_CONFIG_PATH)


def _handle_keygen(args) -> int:
    key = crypto.generate_key()
    path = crypto.write_key_file(key, force=args.force)
    print(f"Chave gerada e salva em {path}.")
    print(f"Servidor e CLI a leem daí — não precisa de variável de ambiente.")
    print(
        f"\n(backup — guarde em local seguro; para CI defina {crypto.KEY_ENV}):\n  {key}",
        file=sys.stderr,
    )
    return 0


def _handle_add(args) -> int:
    path = _resolve_path(args)
    data = read_yaml(path)
    conn_id = args.id or _prompt("connection_id")
    if not conn_id:
        raise CliError("connection_id é obrigatório.")
    fields, pw = _collect_fields()
    add_connection(data, conn_id, fields, password_plain=pw)
    write_yaml(path, data)
    print(f"Conexão '{conn_id}' adicionada em {path}.")
    return 0


def _handle_edit(args) -> int:
    path = _resolve_path(args)
    data = read_yaml(path)
    conns = data.get("connections") or {}
    if args.id not in conns:
        raise CliError(f"conexão '{args.id}' não existe.")
    fields, pw = _collect_fields(conns[args.id])
    edit_connection(data, args.id, fields, password_plain=pw)
    write_yaml(path, data)
    print(f"Conexão '{args.id}' atualizada em {path}.")
    return 0


def _handle_remove(args) -> int:
    path = _resolve_path(args)
    data = read_yaml(path)
    remove_connection(data, args.id)
    write_yaml(path, data)
    print(f"Conexão '{args.id}' removida de {path}.")
    return 0


def _handle_list(args) -> int:
    path = _resolve_path(args)
    rows = list_connections(read_yaml(path))
    if not rows:
        print(f"Nenhuma conexão em {path}.")
        return 0
    print(f"Conexões em {path}:")
    for r in rows:
        pw = "cifrada" if r["has_password"] else "sem senha"
        print(f"  - {r['id']}  ({r['type']})  {r['host']}  [{pw}]")
    return 0


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="nautilus", description="Gestão das conexões do Nautilus.")
    p.add_argument("--config", help=f"caminho do YAML (padrão: NAUTILUS_CONFIG ou {DEFAULT_CONFIG_PATH})")
    sub = p.add_subparsers(dest="command", required=True)

    kg = sub.add_parser("keygen", help="gera e salva a chave de criptografia")
    kg.add_argument("--force", action="store_true", help="substitui a chave existente (invalida senhas já cifradas)")
    kg.set_defaults(func=_handle_keygen)

    a = sub.add_parser("add", help="adiciona uma conexão")
    a.add_argument("id", nargs="?", help="connection_id")
    a.set_defaults(func=_handle_add)

    e = sub.add_parser("edit", help="altera uma conexão")
    e.add_argument("id", help="connection_id")
    e.set_defaults(func=_handle_edit)

    r = sub.add_parser("remove", help="remove uma conexão")
    r.add_argument("id", help="connection_id")
    r.set_defaults(func=_handle_remove)

    sub.add_parser("list", help="lista as conexões").set_defaults(func=_handle_list)
    return p


def main(argv=None) -> int:
    args = build_parser().parse_args(argv)
    try:
        return args.func(args)
    except (CliError, crypto.CryptoError) as e:
        print(f"erro: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())

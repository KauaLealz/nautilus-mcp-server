"""Criptografia simétrica das senhas no YAML.

A senha nunca fica em texto puro no arquivo: o CLI a cifra com uma chave
Fernet (AES-128-CBC + HMAC-SHA256) e grava o token com o prefixo `enc:`.
O loader decifra na subida do servidor.

A chave é gerada uma vez (`nautilus keygen`) e persistida num **arquivo**
(`~/.nautilus/key`), lido tanto pelo servidor quanto pelo CLI — não é preciso
exportar variável de ambiente. Precedência ao resolver a chave:
argumento explícito → `NAUTILUS_KEY` (env, útil em CI) → arquivo de chave.
"""

import os
from pathlib import Path

from cryptography.fernet import Fernet, InvalidToken

ENC_PREFIX = "enc:"
KEY_ENV = "NAUTILUS_KEY"
KEY_FILE_ENV = "NAUTILUS_KEY_FILE"
DEFAULT_KEY_FILE = Path.home() / ".nautilus" / "key"


class CryptoError(Exception):
    """Falha de chave ausente/inválida ou de decifragem."""


def generate_key() -> str:
    """Gera uma chave Fernet nova (base64 url-safe de 32 bytes)."""
    return Fernet.generate_key().decode("ascii")


def key_file_path() -> Path:
    """Caminho do arquivo de chave (override por NAUTILUS_KEY_FILE)."""
    override = os.environ.get(KEY_FILE_ENV)
    return Path(override) if override else DEFAULT_KEY_FILE


def _read_key_file() -> str | None:
    p = key_file_path()
    try:
        if p.is_file():
            content = p.read_text(encoding="ascii").strip()
            return content or None
    except OSError:
        return None
    return None


def resolve_key(key: str | None = None) -> str:
    """Resolve a chave: argumento → env NAUTILUS_KEY → arquivo de chave."""
    raw = key if key is not None else os.environ.get(KEY_ENV)
    if not raw:
        raw = _read_key_file()
    if not raw:
        raise CryptoError(
            f"Nenhuma chave de criptografia encontrada. Gere uma com "
            f"`nautilus keygen` (salva em {key_file_path()}) ou defina {KEY_ENV}."
        )
    return raw


def write_key_file(key: str, *, force: bool = False) -> Path:
    """Persiste a chave no arquivo (permissão restrita). Não sobrescreve sem force."""
    p = key_file_path()
    if p.exists() and not force:
        raise CryptoError(
            f"Já existe uma chave em {p}. Use --force para substituir "
            f"(isso invalida as senhas já cifradas com a chave antiga)."
        )
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(key + "\n", encoding="ascii")
    try:
        os.chmod(p, 0o600)
    except OSError:
        pass
    return p


def _fernet(key: str | None = None) -> Fernet:
    raw = resolve_key(key)
    try:
        return Fernet(raw.encode("ascii") if isinstance(raw, str) else raw)
    except Exception:
        raise CryptoError("a chave de criptografia não é uma chave Fernet válida.")


def is_encrypted(value) -> bool:
    """True se o valor está no formato `enc:...`."""
    return isinstance(value, str) and value.startswith(ENC_PREFIX)


def encrypt(plain: str, key: str | None = None) -> str:
    """Cifra `plain` e devolve o token com prefixo `enc:`."""
    token = _fernet(key).encrypt(plain.encode("utf-8")).decode("ascii")
    return ENC_PREFIX + token


def decrypt(value: str, key: str | None = None) -> str:
    """Decifra um valor `enc:...`. Levanta CryptoError se a chave não bater."""
    if not is_encrypted(value):
        raise CryptoError("valor não está no formato 'enc:...'.")
    token = value[len(ENC_PREFIX):]
    try:
        return _fernet(key).decrypt(token.encode("ascii")).decode("utf-8")
    except InvalidToken:
        raise CryptoError(
            "falha ao decifrar — a chave não corresponde ao valor cifrado."
        )

"""Criptografia simétrica das senhas no YAML.

A senha nunca fica em texto puro no arquivo: o CLI a cifra com uma chave
Fernet (AES-128-CBC + HMAC-SHA256) e grava o token com o prefixo `enc:`.
O loader decifra na subida do servidor. A chave mora **fora** do YAML, na
variável de ambiente `NAUTILUS_KEY` — sem ela, o token cifrado é inútil.
"""

import os

from cryptography.fernet import Fernet, InvalidToken

ENC_PREFIX = "enc:"
KEY_ENV = "NAUTILUS_KEY"


class CryptoError(Exception):
    """Falha de chave ausente/inválida ou de decifragem."""


def generate_key() -> str:
    """Gera uma chave Fernet nova (base64 url-safe de 32 bytes)."""
    return Fernet.generate_key().decode("ascii")


def _fernet(key: str | None = None) -> Fernet:
    raw = key if key is not None else os.environ.get(KEY_ENV)
    if not raw:
        raise CryptoError(
            f"Variável {KEY_ENV} não definida. Gere uma chave com "
            f"`nautilus keygen` e exporte-a no ambiente."
        )
    try:
        return Fernet(raw.encode("ascii") if isinstance(raw, str) else raw)
    except Exception:
        raise CryptoError(f"{KEY_ENV} não é uma chave Fernet válida.")


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
            f"falha ao decifrar — a {KEY_ENV} não corresponde ao valor cifrado."
        )

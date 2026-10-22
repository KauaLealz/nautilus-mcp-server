import pytest

from nautilus.config import crypto


def test_roundtrip():
    key = crypto.generate_key()
    token = crypto.encrypt("minha-senha", key)
    assert token.startswith(crypto.ENC_PREFIX)
    assert crypto.decrypt(token, key) == "minha-senha"


def test_roundtrip_unicode():
    key = crypto.generate_key()
    token = crypto.encrypt("sênha@çÃo #1", key)
    assert crypto.decrypt(token, key) == "sênha@çÃo #1"


def test_is_encrypted():
    assert crypto.is_encrypted("enc:abc")
    assert not crypto.is_encrypted("texto")
    assert not crypto.is_encrypted("${VAR}")
    assert not crypto.is_encrypted(None)


def test_key_from_env(monkeypatch):
    key = crypto.generate_key()
    monkeypatch.setenv(crypto.KEY_ENV, key)
    token = crypto.encrypt("x")
    assert crypto.decrypt(token) == "x"


def test_missing_key_raises(monkeypatch):
    monkeypatch.delenv(crypto.KEY_ENV, raising=False)
    with pytest.raises(crypto.CryptoError) as exc:
        crypto.encrypt("x")
    assert crypto.KEY_ENV in str(exc.value)


def test_invalid_key_raises():
    with pytest.raises(crypto.CryptoError):
        crypto.encrypt("x", key="não-é-fernet")


def test_wrong_key_fails_decrypt():
    token = crypto.encrypt("x", crypto.generate_key())
    with pytest.raises(crypto.CryptoError):
        crypto.decrypt(token, crypto.generate_key())


def test_decrypt_non_enc_raises():
    with pytest.raises(crypto.CryptoError):
        crypto.decrypt("texto-puro", crypto.generate_key())

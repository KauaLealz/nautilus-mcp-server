import pytest

from nautilus.config import crypto


@pytest.fixture(autouse=True)
def _isolate_key(tmp_path, monkeypatch):
    # Aponta o arquivo de chave para um temp inexistente e limpa o env,
    # para os testes não lerem a chave real de ~/.nautilus/key.
    monkeypatch.setenv(crypto.KEY_FILE_ENV, str(tmp_path / "key"))
    monkeypatch.delenv(crypto.KEY_ENV, raising=False)


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


def test_resolves_from_key_file():
    # sem env: grava a chave no arquivo e cifra/decifra sem variável de ambiente
    key = crypto.generate_key()
    crypto.write_key_file(key)
    token = crypto.encrypt("segredo")
    assert crypto.decrypt(token) == "segredo"


def test_env_overrides_key_file(monkeypatch):
    file_key = crypto.generate_key()
    crypto.write_key_file(file_key)
    env_key = crypto.generate_key()
    monkeypatch.setenv(crypto.KEY_ENV, env_key)
    token = crypto.encrypt("x")                      # cifrado com a chave do env
    assert crypto.decrypt(token, env_key) == "x"
    with pytest.raises(crypto.CryptoError):          # a do arquivo não decifra
        crypto.decrypt(token, file_key)


def test_write_key_file_no_overwrite_without_force():
    crypto.write_key_file(crypto.generate_key())
    with pytest.raises(crypto.CryptoError):
        crypto.write_key_file(crypto.generate_key())
    # com force, substitui
    p = crypto.write_key_file(crypto.generate_key(), force=True)
    assert p.exists()


def test_no_key_anywhere_raises():
    with pytest.raises(crypto.CryptoError) as exc:
        crypto.encrypt("x")
    assert "keygen" in str(exc.value)

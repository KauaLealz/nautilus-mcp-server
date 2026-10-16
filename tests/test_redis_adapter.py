import pytest

fakeredis = pytest.importorskip("fakeredis")

from redis.exceptions import ConnectionError as RedisConnectionError

from nautilus.adapters.nosql.redis import RedisAdapter, MAX_VALUE_BYTES


def _adapter_with(client, cid="r"):
    a = RedisAdapter({cid: {"url": "redis://localhost:6379/0", "socket_timeout_ms": 5000}}, 30000)
    a.clients[cid] = client
    return a


def test_scan_pagination_respects_cap():
    fr = fakeredis.FakeStrictRedis()
    for i in range(20):
        fr.set(f"k:{i}", i)
    a = _adapter_with(fr)
    res = a.scan_keys("r", "k:*", max_keys=5)
    assert len(res["keys"]) == 5


def test_string_binary_value_is_base64_marked():
    fr = fakeredis.FakeStrictRedis()
    fr.set("bin", b"\xff\xfe\xfa\x00")
    a = _adapter_with(fr)
    res = a.read_structured_sample("r", "bin", 10)
    assert res["redis_type"] == "string"
    assert res["value"].startswith("<binary:")


def test_large_string_is_truncated():
    fr = fakeredis.FakeStrictRedis()
    fr.set("big", "a" * (MAX_VALUE_BYTES + 100))
    a = _adapter_with(fr)
    res = a.read_structured_sample("r", "big", 10)
    assert "truncado" in res["value"]


def test_get_key_wrongtype_returns_error_not_raise():
    fr = fakeredis.FakeStrictRedis()
    fr.hset("h", "f", "v")
    a = _adapter_with(fr)
    res = a.get_key("r", "h")
    assert isinstance(res, dict) and "error" in res


def test_hash_sample():
    fr = fakeredis.FakeStrictRedis()
    fr.hset("h", mapping={"a": "1", "b": "2"})
    a = _adapter_with(fr)
    res = a.read_structured_sample("r", "h", 10)
    assert res["redis_type"] == "hash"
    assert res["field_count"] == 2
    assert res["fields"]["a"] == "1"


def test_list_sample():
    fr = fakeredis.FakeStrictRedis()
    fr.rpush("l", "x", "y", "z")
    a = _adapter_with(fr)
    res = a.read_structured_sample("r", "l", 2)
    assert res["redis_type"] == "list"
    assert res["length"] == 3
    assert res["truncated"] is True
    assert res["elements"] == ["x", "y"]


def test_set_sample():
    fr = fakeredis.FakeStrictRedis()
    fr.sadd("s", "a", "b", "c")
    a = _adapter_with(fr)
    res = a.read_structured_sample("r", "s", 10)
    assert res["redis_type"] == "set"
    assert res["cardinality"] == 3


def test_zset_sample():
    fr = fakeredis.FakeStrictRedis()
    fr.zadd("z", {"a": 1, "b": 2})
    a = _adapter_with(fr)
    res = a.read_structured_sample("r", "z", 10)
    assert res["redis_type"] == "zset"
    assert res["cardinality"] == 2


def test_stream_sample():
    fr = fakeredis.FakeStrictRedis()
    fr.xadd("st", {"evt": "login"})
    fr.xadd("st", {"evt": "logout"})
    a = _adapter_with(fr)
    res = a.read_structured_sample("r", "st", 10)
    assert res["redis_type"] == "stream"
    assert res["length"] == 2
    assert res["entries"][0]["fields"]["evt"] == "login"


def test_scan_non_numeric_cursor_does_not_crash():
    fr = fakeredis.FakeStrictRedis()
    fr.set("k:1", 1)
    a = _adapter_with(fr)
    res = a.scan_keys("r", "k:*", max_keys=5, start_cursor="lixo")
    assert isinstance(res["keys"], list)


def test_missing_key_reports_not_exists():
    fr = fakeredis.FakeStrictRedis()
    a = _adapter_with(fr)
    res = a.read_structured_sample("r", "nope", 10)
    assert res["redis_type"] == "none"
    assert res["exists"] is False


def test_json_module_type_is_read():
    class JsonStub:
        def type(self, key):
            return b"ReJSON-RL"

        def execute_command(self, cmd, *args):
            assert cmd == "JSON.GET"
            return b'{"a": 1}'

    a = _adapter_with(JsonStub())
    res = a.read_structured_sample("r", "doc", 10)
    assert res["redis_type"] == "json"
    assert res["value"] == '{"a": 1}'


def test_run_reconnects_once_on_connection_drop():
    class Bad:
        def ping(self):
            raise RedisConnectionError("down")

    good = fakeredis.FakeStrictRedis()
    seq = [Bad(), good]
    a = RedisAdapter({"r": {"url": "redis://x"}}, 30000)
    a._build_client = lambda cid: seq.pop(0)
    assert a.test_connection("r") is True
    assert seq == []

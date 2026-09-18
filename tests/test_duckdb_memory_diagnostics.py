from types import SimpleNamespace

from mapmover import duckdb_helpers as d


class FakeConnection:
    def __init__(self, rows=None, error=None):
        self.rows = rows or []
        self.error = error
        self.closed = False

    def execute(self, sql):
        assert sql == "SELECT * FROM duckdb_memory()"
        if self.error:
            raise self.error
        cursor = SimpleNamespace(description=[("tag",), ("memory_usage_bytes",), ("temporary_storage_bytes",)])
        cursor.fetchall = lambda: self.rows
        return cursor

    def close(self):
        self.closed = True


def setup_pool(monkeypatch, entries, *, created=None, generation=7):
    monkeypatch.setattr(d, "_QUERY_POOL", d.queue.LifoQueue(maxsize=16))
    monkeypatch.setattr(d, "_QUERY_POOL_GENERATION", generation)
    monkeypatch.setattr(d, "_QUERY_POOL_CREATED", created if created is not None else len(entries))
    for entry in entries:
        d._QUERY_POOL.put(entry)


def test_inspect_samples_idle_and_sums_memory(monkeypatch):
    first = FakeConnection([("buffer", 100, 4)])
    second = FakeConnection([("temp", 200, 30)])
    setup_pool(monkeypatch, [(first, 7), (second, 7)], created=4)
    report = d.inspect_query_pool_memory()
    assert report["created"] == 4
    assert report["idle"] == 2
    assert report["inuse_estimate"] == 2
    assert report["memory_usage_bytes"] == 300
    assert report["temporary_storage_bytes"] == 34
    assert len(report["details"]) == 2
    assert report["active_leases"] == "unknown (not inspected)"
    assert d._QUERY_POOL.qsize() == 2


def test_stale_generation_is_discarded_and_errors_are_skipped(monkeypatch):
    stale = FakeConnection([("old", 9, 1)])
    broken = FakeConnection(error=RuntimeError("bad diagnostic query"))
    setup_pool(monkeypatch, [(stale, 6), (broken, 7)], created=2)
    report = d.inspect_query_pool_memory()
    assert report["skipped"] == 2
    assert report["sampled_idle"] == 0
    assert stale.closed
    assert d._QUERY_POOL.qsize() == 1

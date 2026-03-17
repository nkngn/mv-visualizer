from mv_lineage.config import load_settings


def test_load_settings_requires_host(monkeypatch):
    monkeypatch.delenv("CLICKHOUSE_HOST", raising=False)
    try:
        load_settings()
        assert False, "Expected ValueError"
    except ValueError:
        assert True

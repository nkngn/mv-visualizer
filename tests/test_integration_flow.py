import json
from pathlib import Path

from mv_lineage import cli


def test_end_to_end_generates_expected_flow(tmp_path, monkeypatch):
    fixture_path = Path("tests/fixtures/sample_mv_rows.json")
    rows = json.loads(fixture_path.read_text())

    monkeypatch.setenv("CLICKHOUSE_HOST", "localhost")
    monkeypatch.setenv("CLICKHOUSE_PORT", "8123")
    monkeypatch.setenv("CLICKHOUSE_USER", "default")
    monkeypatch.setenv("CLICKHOUSE_PASSWORD", "")

    monkeypatch.setattr(cli, "fetch_materialized_views", lambda settings, database: rows)

    output_path = tmp_path / "lineage.html"
    rc = cli.run(["--database", "peak", "--output", str(output_path)])

    assert rc == 0
    assert output_path.exists()

    html = output_path.read_text()
    assert "peak.mtxs" in html
    assert "peak.mtxs_daily_mv" in html
    assert "peak.mtxs_daily" in html

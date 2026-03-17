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
    monkeypatch.setattr(
        cli,
        "fetch_node_details",
        lambda settings, database, nodes, hours, limit: {
            "peak.mtxs": {
                "ddl": "CREATE TABLE peak.mtxs (...)",
                "status": {"state": "ok", "error_count": 0},
                "errors": [],
                "global_errors": [],
            }
        },
    )

    output_path = tmp_path / "lineage.html"
    rc = cli.run(["--database", "peak", "--output", str(output_path)])

    assert rc == 0
    assert output_path.exists()

    html = output_path.read_text()
    assert "peak.mtxs" in html
    assert "peak.mtxs_daily_mv" in html
    assert "peak.mtxs_daily" in html


def test_run_prefetches_node_details_and_renders(monkeypatch, tmp_path):
    rows = [
        {
            "database": "peak",
            "name": "mtxs_daily_mv",
            "create_table_query": "CREATE MATERIALIZED VIEW peak.mtxs_daily_mv TO peak.mtxs_daily AS SELECT * FROM peak.mtxs",
        }
    ]
    observed_nodes: list[str] = []

    monkeypatch.setenv("CLICKHOUSE_HOST", "localhost")
    monkeypatch.setenv("CLICKHOUSE_PORT", "8123")
    monkeypatch.setenv("CLICKHOUSE_USER", "default")
    monkeypatch.setenv("CLICKHOUSE_PASSWORD", "")
    monkeypatch.setattr(cli, "fetch_materialized_views", lambda settings, database: rows)

    def fake_fetch_node_details(settings, database, nodes, hours, limit):
        observed_nodes.extend(nodes)
        return {
            "peak.mtxs_daily_mv": {
                "ddl": "CREATE MATERIALIZED VIEW peak.mtxs_daily_mv ...",
                "status": {"state": "error", "error_count": 1},
                "errors": [{"message": "sample"}],
                "global_errors": [{"name": "SAMPLE", "code": 1}],
            }
        }

    monkeypatch.setattr(cli, "fetch_node_details", fake_fetch_node_details)

    output_path = tmp_path / "lineage_with_details.html"
    rc = cli.run(["--database", "peak", "--output", str(output_path)])
    assert rc == 0
    assert output_path.exists()

    html = output_path.read_text()
    assert "NODE_DETAILS" in html
    assert "peak.mtxs_daily_mv" in html
    assert "CREATE MATERIALIZED VIEW peak.mtxs_daily_mv" in html
    assert observed_nodes[0] == "peak.mtxs_daily_mv"


def test_unavailable_logs_still_render_panel(monkeypatch, tmp_path):
    rows = [
        {
            "database": "peak",
            "name": "mtxs_daily_mv",
            "create_table_query": "CREATE MATERIALIZED VIEW peak.mtxs_daily_mv TO peak.mtxs_daily AS SELECT * FROM peak.mtxs",
        }
    ]

    monkeypatch.setenv("CLICKHOUSE_HOST", "localhost")
    monkeypatch.setenv("CLICKHOUSE_PORT", "8123")
    monkeypatch.setenv("CLICKHOUSE_USER", "default")
    monkeypatch.setenv("CLICKHOUSE_PASSWORD", "")
    monkeypatch.setattr(cli, "fetch_materialized_views", lambda settings, database: rows)
    monkeypatch.setattr(cli, "fetch_node_details", lambda *args, **kwargs: (_ for _ in ()).throw(Exception("boom")))

    output_path = tmp_path / "lineage_unavailable.html"
    rc = cli.run(["--database", "peak", "--output", str(output_path)])

    assert rc == 0
    assert output_path.exists()
    html = output_path.read_text()
    assert 'id="node-detail-panel"' in html
    assert "No log data / log table unavailable." in html

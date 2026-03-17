from mv_lineage.graph import render_lineage_html


def test_render_html_contains_sidebar_tabs(tmp_path):
    output = tmp_path / "lineage.html"
    render_lineage_html(
        records=[{"source": "peak.mtxs", "mv": "peak.mtxs_daily_mv", "target": "peak.mtxs_daily"}],
        output_path=str(output),
        node_details={
            "peak.mtxs": {
                "ddl": "CREATE TABLE ...",
                "status": {"state": "ok", "error_count": 0},
                "errors": [],
                "global_errors": [],
            }
        },
    )
    html = output.read_text()
    assert 'id="node-detail-panel"' in html
    assert "DDL" in html and "Status" in html and "Errors" in html

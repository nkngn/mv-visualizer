from mv_lineage.parser import parse_mv_ddl


def test_parse_mtxs_daily_mv_path():
    ddl = """CREATE MATERIALIZED VIEW peak.mtxs_daily_mv TO peak.mtxs_daily AS SELECT * FROM peak.mtxs"""
    out = parse_mv_ddl("peak", "mtxs_daily_mv", ddl)
    assert out.source == "peak.mtxs"
    assert out.mv == "peak.mtxs_daily_mv"
    assert out.target == "peak.mtxs_daily"

from mv_lineage.extractor import build_query_log_sql


def test_build_query_log_sql_filters_window_and_limit():
    sql = build_query_log_sql()
    assert "system.query_log" in sql
    assert "event_time > now() - INTERVAL %(hours)s HOUR" in sql
    assert "LIMIT %(limit)s" in sql

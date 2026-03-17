from mv_lineage.extractor import build_mv_query


def test_build_mv_query_filters_single_database():
    q = build_mv_query()
    assert "engine = 'MaterializedView'" in q
    assert "database = %(db)s" in q

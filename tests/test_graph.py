from mv_lineage.graph import build_nodes_edges


def test_build_nodes_edges_for_single_mv_path():
    records = [{"source": "peak.mtxs", "mv": "peak.mtxs_daily_mv", "target": "peak.mtxs_daily"}]
    nodes, edges = build_nodes_edges(records)
    assert len(nodes) == 3
    assert ("peak.mtxs", "peak.mtxs_daily_mv") in edges
    assert ("peak.mtxs_daily_mv", "peak.mtxs_daily") in edges

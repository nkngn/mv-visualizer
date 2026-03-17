from __future__ import annotations

from pathlib import Path
from typing import Iterable

from pyvis.network import Network


def build_nodes_edges(records: Iterable[dict[str, str]]) -> tuple[set[str], set[tuple[str, str]]]:
    nodes: set[str] = set()
    edges: set[tuple[str, str]] = set()
    for record in records:
        source = record["source"]
        mv = record["mv"]
        target = record["target"]
        nodes.update({source, mv, target})
        edges.add((source, mv))
        edges.add((mv, target))
    return nodes, edges


def render_lineage_html(records: Iterable[dict[str, str]], output_path: str) -> Path:
    nodes, edges = build_nodes_edges(records)
    net = Network(height="900px", width="100%", directed=True)

    mv_nodes = {record["mv"] for record in records}
    for node in sorted(nodes):
        if node in mv_nodes:
            net.add_node(node, label=node, shape="box", color="#ff8c42")
        else:
            net.add_node(node, label=node, shape="dot", color="#2a9d8f")

    for source, target in sorted(edges):
        net.add_edge(source, target)

    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    net.write_html(str(output), open_browser=False)
    return output

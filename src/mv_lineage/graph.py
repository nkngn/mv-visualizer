from __future__ import annotations

import json
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


def _inject_node_panel(html: str, node_details: dict[str, dict]) -> str:
    payload = json.dumps(node_details)
    panel = f"""
<style>
#node-detail-panel {{
  position: fixed;
  top: 0;
  right: 0;
  width: 360px;
  height: 100%;
  background: #ffffff;
  border-left: 1px solid #ddd;
  box-shadow: -4px 0 12px rgba(0,0,0,0.08);
  z-index: 1000;
  padding: 16px;
  overflow-y: auto;
  font-family: ui-sans-serif, sans-serif;
}}
.nd-tabs {{ display: flex; gap: 8px; margin-bottom: 12px; }}
.nd-tab {{ border: 1px solid #ccc; border-radius: 6px; padding: 4px 8px; cursor: pointer; background: #f7f7f7; }}
.nd-tab.active {{ background: #e9f5ff; border-color: #6daee8; }}
.nd-pane {{ display: none; white-space: pre-wrap; font-size: 12px; }}
.nd-pane.active {{ display: block; }}
#mynetwork {{ margin-right: 360px; }}
</style>
<div id="node-detail-panel">
  <h3 id="nd-title">Node Details</h3>
  <div class="nd-tabs">
    <button class="nd-tab active" data-pane="ddl">DDL</button>
    <button class="nd-tab" data-pane="status">Status</button>
    <button class="nd-tab" data-pane="errors">Errors</button>
  </div>
  <div id="nd-ddl" class="nd-pane active">Click a node to view DDL.</div>
  <div id="nd-status" class="nd-pane"></div>
  <div id="nd-errors" class="nd-pane"></div>
</div>
<script>
const NODE_DETAILS = {payload};

function setPane(name) {{
  document.querySelectorAll('.nd-tab').forEach((tab) => {{
    tab.classList.toggle('active', tab.dataset.pane === name);
  }});
  document.querySelectorAll('.nd-pane').forEach((pane) => {{
    pane.classList.toggle('active', pane.id === 'nd-' + name);
  }});
}}

document.querySelectorAll('.nd-tab').forEach((tab) => {{
  tab.addEventListener('click', () => setPane(tab.dataset.pane));
}});

function renderNodeDetail(nodeId) {{
  const detail = NODE_DETAILS[nodeId] || {{}};
  document.getElementById('nd-title').textContent = nodeId || 'Node Details';
  document.getElementById('nd-ddl').textContent = detail.ddl || 'DDL not available.';
  document.getElementById('nd-status').textContent = JSON.stringify(detail.status || {{}}, null, 2);
  document.getElementById('nd-errors').textContent = JSON.stringify({{
    node_errors: detail.errors || [],
    global_errors: detail.global_errors || []
  }}, null, 2);
}}

if (typeof network !== 'undefined') {{
  network.on('click', (params) => {{
    if (!params.nodes || params.nodes.length === 0) return;
    renderNodeDetail(params.nodes[0]);
  }});
}}
</script>
"""
    if "</body>" in html:
        return html.replace("</body>", panel + "\n</body>")
    return html + panel


def render_lineage_html(
    records: Iterable[dict[str, str]],
    output_path: str,
    node_details: dict[str, dict] | None = None,
    highlight_error_nodes: bool = True,
) -> Path:
    nodes, edges = build_nodes_edges(records)
    net = Network(height="900px", width="100%", directed=True)

    mv_nodes = {record["mv"] for record in records}
    node_details = node_details or {}
    for node in sorted(nodes):
        status = node_details.get(node, {}).get("status", {})
        is_error_node = bool(status.get("error_count", 0) > 0) if highlight_error_nodes else False
        if node in mv_nodes:
            color = "#c0392b" if is_error_node else "#ff8c42"
            net.add_node(node, label=node, shape="box", color=color)
        else:
            color = "#c0392b" if is_error_node else "#2a9d8f"
            net.add_node(node, label=node, shape="dot", color=color)

    for source, target in sorted(edges):
        net.add_edge(source, target)

    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    net.write_html(str(output), open_browser=False)
    html = output.read_text(encoding="utf-8")
    html = _inject_node_panel(html, node_details)
    output.write_text(html, encoding="utf-8")
    return output

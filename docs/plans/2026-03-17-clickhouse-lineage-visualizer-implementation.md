# ClickHouse Lineage Visualizer Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Build a Python CLI that reads ClickHouse Materialized View DDL and generates an interactive HTML lineage graph (`source_table -> mv -> target_table`).

**Architecture:** Use a small modular structure: ClickHouse metadata extractor, regex-based lineage parser, graph builder, and HTML renderer (pyvis). Keep V1 table-level only, fail fast on config/connectivity issues, and continue on per-MV parse errors with warnings.

**Tech Stack:** Python 3.11+, `clickhouse-connect`, `pyvis`, `pytest`

---

### Task 1: Bootstrap project structure and dependencies

**Files:**
- Create: `requirements.txt`
- Create: `src/mv_lineage/__init__.py`
- Create: `src/mv_lineage/config.py`
- Create: `src/mv_lineage/models.py`
- Create: `README.md`

**Step 1: Write the failing test**

```python
# tests/test_bootstrap.py
from mv_lineage.config import load_settings


def test_load_settings_requires_host(monkeypatch):
    monkeypatch.delenv("CLICKHOUSE_HOST", raising=False)
    try:
        load_settings()
        assert False, "Expected ValueError"
    except ValueError:
        assert True
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/test_bootstrap.py::test_load_settings_requires_host -v`
Expected: FAIL (module/file missing)

**Step 3: Write minimal implementation**

```python
# src/mv_lineage/config.py
import os


def load_settings() -> dict:
    host = os.getenv("CLICKHOUSE_HOST")
    if not host:
        raise ValueError("Missing CLICKHOUSE_HOST")
    return {"host": host}
```

**Step 4: Run test to verify it passes**

Run: `pytest tests/test_bootstrap.py::test_load_settings_requires_host -v`
Expected: PASS

**Step 5: Commit**

```bash
git add requirements.txt README.md src/mv_lineage/__init__.py src/mv_lineage/config.py src/mv_lineage/models.py tests/test_bootstrap.py
git commit -m "chore: bootstrap mv lineage project skeleton"
```

### Task 2: Implement DDL lineage parser (regex-first)

**Files:**
- Create: `src/mv_lineage/parser.py`
- Create: `tests/test_parser.py`

**Step 1: Write the failing test**

```python
from mv_lineage.parser import parse_mv_ddl


def test_parse_mtxs_daily_mv_path():
    ddl = """CREATE MATERIALIZED VIEW peak.mtxs_daily_mv TO peak.mtxs_daily AS SELECT * FROM peak.mtxs"""
    out = parse_mv_ddl("peak", "mtxs_daily_mv", ddl)
    assert out.source == "peak.mtxs"
    assert out.mv == "peak.mtxs_daily_mv"
    assert out.target == "peak.mtxs_daily"
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/test_parser.py::test_parse_mtxs_daily_mv_path -v`
Expected: FAIL (`parse_mv_ddl` missing)

**Step 3: Write minimal implementation**

```python
# src/mv_lineage/parser.py
import re
from dataclasses import dataclass


@dataclass
class ParsedLineage:
    source: str
    mv: str
    target: str


def parse_mv_ddl(database: str, mv_name: str, ddl: str) -> ParsedLineage:
    to_match = re.search(r"\\bTO\\s+([\\w\.]+)", ddl, flags=re.IGNORECASE)
    from_match = re.search(r"\\bFROM\\s+([\\w\.]+)", ddl, flags=re.IGNORECASE)
    if not to_match or not from_match:
        raise ValueError("Unable to parse MV DDL")
    mv_full = mv_name if "." in mv_name else f"{database}.{mv_name}"
    return ParsedLineage(source=from_match.group(1), mv=mv_full, target=to_match.group(1))
```

**Step 4: Run test to verify it passes**

Run: `pytest tests/test_parser.py -v`
Expected: PASS

**Step 5: Commit**

```bash
git add src/mv_lineage/parser.py tests/test_parser.py
git commit -m "feat: add regex parser for MV source-target lineage"
```

### Task 3: Implement ClickHouse metadata extractor

**Files:**
- Create: `src/mv_lineage/extractor.py`
- Create: `tests/test_extractor_query.py`

**Step 1: Write the failing test**

```python
from mv_lineage.extractor import build_mv_query


def test_build_mv_query_filters_single_database():
    q = build_mv_query()
    assert "engine = 'MaterializedView'" in q
    assert "database = %(db)s" in q
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/test_extractor_query.py::test_build_mv_query_filters_single_database -v`
Expected: FAIL (`build_mv_query` missing)

**Step 3: Write minimal implementation**

```python
# src/mv_lineage/extractor.py

def build_mv_query() -> str:
    return (
        "SELECT database, name, create_table_query "
        "FROM system.tables "
        "WHERE database = %(db)s AND engine = 'MaterializedView'"
    )
```

**Step 4: Run test to verify it passes**

Run: `pytest tests/test_extractor_query.py -v`
Expected: PASS

**Step 5: Commit**

```bash
git add src/mv_lineage/extractor.py tests/test_extractor_query.py
git commit -m "feat: add MV metadata query builder"
```

### Task 4: Build graph and render HTML with pyvis

**Files:**
- Create: `src/mv_lineage/graph.py`
- Create: `tests/test_graph.py`

**Step 1: Write the failing test**

```python
from mv_lineage.graph import build_nodes_edges


def test_build_nodes_edges_for_single_mv_path():
    records = [{"source": "peak.mtxs", "mv": "peak.mtxs_daily_mv", "target": "peak.mtxs_daily"}]
    nodes, edges = build_nodes_edges(records)
    assert len(nodes) == 3
    assert ("peak.mtxs", "peak.mtxs_daily_mv") in edges
    assert ("peak.mtxs_daily_mv", "peak.mtxs_daily") in edges
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/test_graph.py::test_build_nodes_edges_for_single_mv_path -v`
Expected: FAIL (`build_nodes_edges` missing)

**Step 3: Write minimal implementation**

```python
# src/mv_lineage/graph.py
from typing import Dict, List, Set, Tuple


def build_nodes_edges(records: List[Dict[str, str]]) -> Tuple[Set[str], Set[Tuple[str, str]]]:
    nodes: Set[str] = set()
    edges: Set[Tuple[str, str]] = set()
    for r in records:
        source, mv, target = r["source"], r["mv"], r["target"]
        nodes.update([source, mv, target])
        edges.add((source, mv))
        edges.add((mv, target))
    return nodes, edges
```

**Step 4: Run test to verify it passes**

Run: `pytest tests/test_graph.py -v`
Expected: PASS

**Step 5: Commit**

```bash
git add src/mv_lineage/graph.py tests/test_graph.py
git commit -m "feat: add graph node-edge builder"
```

### Task 5: Wire CLI end-to-end and generate HTML

**Files:**
- Create: `src/mv_lineage/cli.py`
- Create: `scripts/generate_lineage.py`
- Create: `tests/test_cli_smoke.py`

**Step 1: Write the failing test**

```python
from mv_lineage.cli import parse_args


def test_parse_args_requires_database_and_output():
    args = parse_args(["--database", "peak", "--output", "lineage.html"])
    assert args.database == "peak"
    assert args.output == "lineage.html"
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/test_cli_smoke.py::test_parse_args_requires_database_and_output -v`
Expected: FAIL (`parse_args` missing)

**Step 3: Write minimal implementation**

```python
# src/mv_lineage/cli.py
import argparse


def parse_args(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--database", required=True)
    parser.add_argument("--output", required=True)
    return parser.parse_args(argv)
```

Add integration function to:
- load settings
- fetch MVs from ClickHouse
- parse lineage records
- render pyvis HTML to output path

**Step 4: Run test to verify it passes**

Run: `pytest tests/test_cli_smoke.py -v`
Expected: PASS

**Step 5: Commit**

```bash
git add src/mv_lineage/cli.py scripts/generate_lineage.py tests/test_cli_smoke.py
git commit -m "feat: add CLI and html generation entrypoint"
```

### Task 6: Verify end-to-end behavior and document usage

**Files:**
- Modify: `README.md`
- Create: `tests/fixtures/sample_mv_rows.json`
- Create: `tests/test_integration_flow.py`

**Step 1: Write the failing test**

```python
def test_end_to_end_generates_expected_flow(tmp_path):
    # feed sample MV rows containing peak.mtxs_daily_mv
    # assert output HTML exists
    # assert key node strings exist in HTML
    ...
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/test_integration_flow.py -v`
Expected: FAIL (fixture/wiring missing)

**Step 3: Write minimal implementation**

- Add fixture-driven integration wiring in CLI service layer.
- Ensure parser warnings do not abort full run.
- Add README usage:
  - env vars setup
  - install deps
  - run command example

**Step 4: Run test to verify it passes**

Run: `pytest -v`
Expected: PASS

**Step 5: Commit**

```bash
git add README.md tests/fixtures/sample_mv_rows.json tests/test_integration_flow.py src/mv_lineage
git commit -m "test: add integration flow validation and docs"
```

## Implementation Notes
- Keep parser in V1 intentionally narrow (first `FROM`, first `TO`).
- Use warnings for per-MV parse failure; never stop batch by default.
- Distinguish node styles by type (`table`, `mv`) in pyvis for readability.
- Use @superpowers:test-driven-development while implementing each task.
- Use @superpowers:verification-before-completion before claiming done.
- If working in parallelized execution, use @superpowers:subagent-driven-development.

# MV Visualizer

Python tool to read ClickHouse Materialized View metadata and generate table-level lineage graph HTML.

## Install

```bash
python3 -m pip install -r requirements.txt
```

## Configure

Set these environment variables:

```bash
export CLICKHOUSE_HOST=localhost
export CLICKHOUSE_PORT=8123
export CLICKHOUSE_USER=default
export CLICKHOUSE_PASSWORD=''
```

## Run

```bash
PYTHONPATH=src python3 scripts/generate_lineage.py \
  --database peak \
  --output lineage.html \
  --log-hours 24 \
  --log-limit-per-node 20
```

Open `lineage.html` in a browser to explore the graph.

## Node Detail Panel

Click any node in the graph to open the right-side panel:
- `DDL`: schema DDL from `system.tables.create_table_query`
- `Status`: summary from recent logs (query count, error count, latency)
- `Errors`: node-level errors and global `system.errors` reference

Optional flag:

```bash
PYTHONPATH=src python3 scripts/generate_lineage.py --database peak --output lineage.html --disable-node-logs
```

## Notes

- Default window is 24 hours and top 20 log entries per node.
- `system.errors` is shown as global reference and is not strictly mapped to a specific node.

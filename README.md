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
PYTHONPATH=src python3 scripts/generate_lineage.py --database peak --output lineage.html
```

Open `lineage.html` in a browser to explore the graph.

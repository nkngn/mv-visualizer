# ClickHouse Lineage Visualizer Design

## Goal
Build a Python script that connects to ClickHouse, reads Materialized View metadata, infers table-level data flow, and exports an interactive HTML graph.

## Scope (V1)
- Source of lineage: `system.tables.create_table_query` for `MaterializedView` engine.
- Lineage granularity: node-level only (`source_table -> mv -> target_table`).
- Rendering: HTML via `pyvis`.
- Database scope: one database per run.
- Connection config: environment variables.

## Non-Goals (V1)
- Column-level lineage.
- Full SQL semantic parsing for deeply nested/complex queries.
- Multi-database global graph in one run.

## Recommended Approach
Use a regex-first parser on MV DDL pulled from `system.tables`, with warning logs for parse failures.

Why:
- Fastest path to a usable V1.
- Matches current use case (`peak.mtxs -> peak.mtxs_daily_mv -> peak.mtxs_daily`).
- Easy to harden iteratively with fallback parsing later.

## Architecture
- `extractor`: connects to ClickHouse and fetches MV metadata for a target DB.
- `lineage_parser`: extracts `source_table`, `mv_name`, `target_table` from each MV DDL.
- `graph_builder`: builds directed graph nodes and edges.
- `html_renderer`: renders graph to interactive HTML using `pyvis`.
- `cli`: command entrypoint and argument handling.

## Data Flow
1. Read env vars:
   - `CLICKHOUSE_HOST`
   - `CLICKHOUSE_PORT`
   - `CLICKHOUSE_USER`
   - `CLICKHOUSE_PASSWORD`
   - `CLICKHOUSE_DATABASE` (optional default if CLI DB not passed)
2. Parse CLI args: `--database` and `--output`.
3. Query ClickHouse:
   - `SELECT database, name, create_table_query FROM system.tables WHERE database = %(db)s AND engine = 'MaterializedView'`
4. For each MV DDL:
   - parse `TO <target_table>`
   - parse primary `FROM <source_table>`
   - emit edges:
     - `<source_table> -> <mv_name>`
     - `<mv_name> -> <target_table>`
5. Render and write `lineage.html`.

## Error Handling
- Missing required env vars: fail fast with actionable error.
- ClickHouse connection/query errors: print clear message and exit non-zero.
- MV parse failures: warn and continue with other MVs.

## Testing Strategy
- Smoke test: run script and verify HTML file is generated.
- Functional test with known MV:
  - Input includes:
    - `CREATE MATERIALIZED VIEW peak.mtxs_daily_mv TO peak.mtxs_daily ... FROM peak.mtxs ...`
  - Expected graph path:
    - `peak.mtxs -> peak.mtxs_daily_mv -> peak.mtxs_daily`
- Resilience test:
  - malformed/unsupported MV DDL logs warning but does not stop run.

## Acceptance Criteria
1. Script connects to ClickHouse using env vars.
2. Script scans Materialized Views for a single database.
3. Script generates interactive HTML via `pyvis`.
4. Graph contains expected path for the `mtxs` example.

## Future Enhancements
- Add fallback SQL parser when regex extraction fails.
- Support multi-source extraction (`JOIN`, `UNION`).
- Add filter options by table/MV name.
- Add optional JSON export for automation.

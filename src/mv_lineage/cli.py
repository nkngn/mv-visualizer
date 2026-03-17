from __future__ import annotations

import argparse
import sys
from typing import Iterable

from clickhouse_connect.driver.exceptions import ClickHouseError

from mv_lineage.config import load_settings
from mv_lineage.extractor import fetch_materialized_views
from mv_lineage.graph import render_lineage_html
from mv_lineage.parser import parse_mv_ddl


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate ClickHouse MV lineage graph HTML")
    parser.add_argument("--database", required=True, help="ClickHouse database to scan")
    parser.add_argument("--output", required=True, help="Output HTML path")
    parser.add_argument("--log-hours", type=int, default=24, help="Time window for node logs")
    parser.add_argument(
        "--log-limit-per-node",
        type=int,
        default=20,
        help="Maximum status/error log entries per node",
    )
    parser.add_argument(
        "--disable-node-logs",
        action="store_true",
        help="Disable node log prefetch and render only lineage graph",
    )
    return parser.parse_args(argv)


def _to_lineage_records(rows: Iterable[dict[str, str]]) -> list[dict[str, str]]:
    records: list[dict[str, str]] = []
    for row in rows:
        db = row["database"]
        name = row["name"]
        ddl = row["create_table_query"]
        try:
            parsed = parse_mv_ddl(db, name, ddl)
        except ValueError as err:
            print(f"Warning: skip MV {db}.{name}: {err}", file=sys.stderr)
            continue
        records.append({"source": parsed.source, "mv": parsed.mv, "target": parsed.target})
    return records


def run(argv: list[str] | None = None) -> int:
    args = parse_args(argv)

    try:
        settings = load_settings()
    except ValueError as err:
        print(f"Config error: {err}", file=sys.stderr)
        return 1

    try:
        rows = fetch_materialized_views(settings, args.database)
    except ClickHouseError as err:
        print(f"ClickHouse query error: {err}", file=sys.stderr)
        return 2

    records = _to_lineage_records(rows)
    render_lineage_html(records, args.output)
    print(f"Generated lineage HTML: {args.output}")
    return 0


def main() -> None:
    raise SystemExit(run())

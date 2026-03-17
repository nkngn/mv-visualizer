from __future__ import annotations

from typing import Any

import clickhouse_connect

from mv_lineage.models import Settings


def build_mv_query() -> str:
    return (
        "SELECT database, name, create_table_query "
        "FROM system.tables "
        "WHERE database = %(db)s AND engine = 'MaterializedView'"
    )


def fetch_materialized_views(settings: Settings, database: str) -> list[dict[str, Any]]:
    client = clickhouse_connect.get_client(
        host=settings.host,
        port=settings.port,
        username=settings.user,
        password=settings.password,
        database=database,
    )
    result = client.query(build_mv_query(), parameters={"db": database})
    rows: list[dict[str, Any]] = []
    for row in result.result_rows:
        rows.append(
            {
                "database": row[0],
                "name": row[1],
                "create_table_query": row[2],
            }
        )
    return rows

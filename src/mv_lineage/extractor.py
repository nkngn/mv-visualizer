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


def build_query_log_sql() -> str:
    return (
        "SELECT event_time, query_id, query_duration_ms, exception_code, exception, query "
        "FROM system.query_log "
        "WHERE event_time > now() - INTERVAL %(hours)s HOUR "
        "AND has(%(node_names)s, arrayJoin(extractAllGroupsHorizontal(query, '([A-Za-z0-9_]+\\.[A-Za-z0-9_]+)')[1])) "
        "ORDER BY event_time DESC "
        "LIMIT %(limit)s"
    )


def fetch_node_ddls(client: Any, database: str, node_names: list[str]) -> dict[str, str]:
    if not node_names:
        return {}
    sql = (
        "SELECT concat(database, '.', name), create_table_query "
        "FROM system.tables "
        "WHERE database = %(db)s AND concat(database, '.', name) IN %(node_names)s"
    )
    result = client.query(sql, parameters={"db": database, "node_names": tuple(node_names)})
    return {row[0]: row[1] for row in result.result_rows}


def fetch_node_status_and_errors(
    client: Any, database: str, node_names: list[str], hours: int, limit: int
) -> dict[str, dict[str, Any]]:
    details: dict[str, dict[str, Any]] = {
        node: {
            "status": {
                "last_seen": None,
                "query_count": 0,
                "error_count": 0,
                "avg_latency_ms": None,
                "state": "ok",
            },
            "errors": [],
        }
        for node in node_names
    }
    if not node_names:
        return details

    try:
        sql = (
            "SELECT event_time, query_id, query_duration_ms, exception_code, exception, query "
            "FROM system.query_log "
            "WHERE event_time > now() - INTERVAL %(hours)s HOUR "
            "ORDER BY event_time DESC "
            "LIMIT %(limit)s"
        )
        result = client.query(sql, parameters={"hours": hours, "limit": max(limit * 20, 200)})
    except Exception:
        for node in details:
            details[node]["status"]["state"] = "unavailable"
        return details

    for row in result.result_rows:
        event_time, query_id, query_duration_ms, exception_code, exception, query_text = row
        query_text = query_text or ""
        for node in node_names:
            if node not in query_text:
                continue
            status = details[node]["status"]
            status["last_seen"] = str(event_time)
            status["query_count"] += 1
            if query_duration_ms is not None:
                prev = status["avg_latency_ms"]
                if prev is None:
                    status["avg_latency_ms"] = float(query_duration_ms)
                else:
                    count = status["query_count"]
                    status["avg_latency_ms"] = ((prev * (count - 1)) + float(query_duration_ms)) / count
            if exception_code not in (None, 0):
                status["error_count"] += 1
                status["state"] = "error"
                if len(details[node]["errors"]) < limit:
                    details[node]["errors"].append(
                        {
                            "timestamp": str(event_time),
                            "source": "query_log",
                            "message": str(exception or ""),
                            "query_id": str(query_id) if query_id else None,
                        }
                    )
    return details


def fetch_global_system_errors(client: Any, limit: int) -> list[dict[str, Any]]:
    sql = (
        "SELECT name, code, value, last_error_time, last_error_message, "
        "last_error_format_string, last_error_trace, remote, query_id "
        "FROM system.errors "
        "ORDER BY value DESC "
        "LIMIT %(limit)s"
    )
    try:
        result = client.query(sql, parameters={"limit": limit})
    except Exception:
        return []
    out: list[dict[str, Any]] = []
    for row in result.result_rows:
        out.append(
            {
                "name": row[0],
                "code": row[1],
                "value": row[2],
                "last_error_time": str(row[3]) if row[3] else None,
                "last_error_message": row[4],
                "last_error_format_string": row[5],
                "last_error_trace": row[6],
                "remote": row[7],
                "query_id": row[8],
            }
        )
    return out


def fetch_node_details(
    settings: Settings, database: str, node_names: list[str], hours: int, limit: int
) -> dict[str, dict[str, Any]]:
    client = clickhouse_connect.get_client(
        host=settings.host,
        port=settings.port,
        username=settings.user,
        password=settings.password,
        database=database,
    )
    ddls = fetch_node_ddls(client, database, node_names)
    status_map = fetch_node_status_and_errors(client, database, node_names, hours, limit)
    global_errors = fetch_global_system_errors(client, limit)

    details: dict[str, dict[str, Any]] = {}
    for node in node_names:
        node_status = status_map.get(
            node,
            {
                "status": {
                    "last_seen": None,
                    "query_count": 0,
                    "error_count": 0,
                    "avg_latency_ms": None,
                    "state": "unavailable",
                },
                "errors": [],
            },
        )
        details[node] = {
            "ddl": ddls.get(node, ""),
            "status": node_status["status"],
            "errors": node_status["errors"],
            "global_errors": global_errors,
        }
    return details

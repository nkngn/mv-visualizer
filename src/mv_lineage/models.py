from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class Settings:
    host: str
    port: int
    user: str
    password: str


@dataclass(frozen=True)
class NodeStatusSummary:
    last_seen: str | None
    query_count: int
    error_count: int
    avg_latency_ms: float | None
    state: str


@dataclass(frozen=True)
class NodeErrorEntry:
    timestamp: str | None
    source: str
    message: str
    query_id: str | None


@dataclass(frozen=True)
class NodeDetails:
    node: str
    node_type: str
    ddl: str
    status: NodeStatusSummary
    errors: list[NodeErrorEntry] = field(default_factory=list)
    global_errors: list[dict[str, Any]] = field(default_factory=list)

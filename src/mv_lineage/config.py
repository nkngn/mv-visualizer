import os

from mv_lineage.models import Settings


def load_settings() -> Settings:
    host = os.getenv("CLICKHOUSE_HOST")
    if not host:
        raise ValueError("Missing CLICKHOUSE_HOST")

    port = int(os.getenv("CLICKHOUSE_PORT", "8123"))
    user = os.getenv("CLICKHOUSE_USER", "default")
    password = os.getenv("CLICKHOUSE_PASSWORD", "")
    return Settings(host=host, port=port, user=user, password=password)

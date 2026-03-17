import re
from dataclasses import dataclass


@dataclass(frozen=True)
class ParsedLineage:
    source: str
    mv: str
    target: str


def parse_mv_ddl(database: str, mv_name: str, ddl: str) -> ParsedLineage:
    to_match = re.search(r"\bTO\s+([`\w\.]+)", ddl, flags=re.IGNORECASE)
    from_match = re.search(r"\bFROM\s+([`\w\.]+)", ddl, flags=re.IGNORECASE)
    if not to_match or not from_match:
        raise ValueError("Unable to parse MV DDL")

    target = to_match.group(1).strip("`")
    source = from_match.group(1).strip("`")
    mv_full = mv_name if "." in mv_name else f"{database}.{mv_name}"
    return ParsedLineage(source=source, mv=mv_full, target=target)

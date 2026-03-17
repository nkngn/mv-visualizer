from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    host: str
    port: int
    user: str
    password: str

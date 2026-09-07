from __future__ import annotations

import os
from dataclasses import dataclass

UDP_HOST = "0.0.0.0"
UDP_PORT = 9999
QUEUE_CAPACITY = 1000
RECENT_SEQUENCE_HISTORY = 50
STALE_AFTER_SECONDS = 5
OFFLINE_AFTER_SECONDS = 15
HEALTH_CHECK_INTERVAL_SECONDS = 5


def _parse_int_env(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None:
        return default
    try:
        return int(raw)
    except ValueError as exc:
        raise ValueError(f"invalid integer for {name}: {raw!r}") from exc


@dataclass(frozen=True)
class Settings:
    udp_host: str
    udp_port: int
    queue_capacity: int
    recent_sequence_history: int
    stale_after_seconds: int
    offline_after_seconds: int
    health_check_interval_seconds: int

    def __post_init__(self) -> None:
        if not 1 <= self.udp_port <= 65535:
            raise ValueError("udp_port must be between 1 and 65535")
        if self.queue_capacity < 1:
            raise ValueError("queue_capacity must be at least 1")
        if self.recent_sequence_history < 1:
            raise ValueError("recent_sequence_history must be at least 1")
        if self.stale_after_seconds >= self.offline_after_seconds:
            raise ValueError(
                "stale_after_seconds must be less than offline_after_seconds"
            )

    @classmethod
    def from_env(cls) -> Settings:
        return cls(
            udp_host=os.environ.get("UDP_HOST", UDP_HOST),
            udp_port=_parse_int_env("UDP_PORT", UDP_PORT),
            queue_capacity=_parse_int_env("QUEUE_CAPACITY", QUEUE_CAPACITY),
            recent_sequence_history=_parse_int_env(
                "RECENT_SEQUENCE_HISTORY", RECENT_SEQUENCE_HISTORY
            ),
            stale_after_seconds=_parse_int_env(
                "STALE_AFTER_SECONDS", STALE_AFTER_SECONDS
            ),
            offline_after_seconds=_parse_int_env(
                "OFFLINE_AFTER_SECONDS", OFFLINE_AFTER_SECONDS
            ),
            health_check_interval_seconds=_parse_int_env(
                "HEALTH_CHECK_INTERVAL_SECONDS", HEALTH_CHECK_INTERVAL_SECONDS
            ),
        )

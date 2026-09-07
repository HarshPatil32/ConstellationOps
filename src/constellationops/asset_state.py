from collections import deque
from dataclasses import dataclass, field
from enum import Enum

from constellationops.telemetry import TelemetryPacket


class SequenceClass(str, Enum):
    FIRST = "FIRST"
    NORMAL = "NORMAL"
    FORWARD_GAP = "FORWARD_GAP"
    DUPLICATE = "DUPLICATE"
    OUT_OF_ORDER = "OUT_OF_ORDER"


class AssetHealth(str, Enum):
    ONLINE = "ONLINE"
    STALE = "STALE"
    OFFLINE = "OFFLINE"


@dataclass
class AssetState:
    asset_id: str
    recent_sequence_capacity: int
    highest_sequence: int | None = None
    recent_sequence_order: deque[int] = field(default_factory=deque)
    recent_sequence_set: set[int] = field(default_factory=set)
    accepted_count: int = 0
    gap_count: int = 0
    duplicate_count: int = 0
    out_of_order_count: int = 0
    latest_telemetry: TelemetryPacket | None = None
    last_seen_monotonic: float | None = None
    health: AssetHealth = AssetHealth.ONLINE

    def __post_init__(self) -> None:
        if self.recent_sequence_capacity < 1:
            raise ValueError("recent_sequence_capacity must be at least 1")

    def record_sequence(self, sequence_number: int) -> None:
        if sequence_number in self.recent_sequence_set:
            return
        self.recent_sequence_order.append(sequence_number)
        self.recent_sequence_set.add(sequence_number)
        if len(self.recent_sequence_order) > self.recent_sequence_capacity:
            oldest = self.recent_sequence_order.popleft()
            self.recent_sequence_set.discard(oldest)

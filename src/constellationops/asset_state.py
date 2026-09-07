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
    recent_sequence_order: deque[int] = field(default_factory=deque) # Used for O(1) oldest value lookup and eviction
    recent_sequence_set: set[int] = field(default_factory=set) # Used for O(1) lookups
    accepted_count: int = 0
    gap_count: int = 0
    duplicate_count: int = 0
    out_of_order_count: int = 0
    latest_telemetry: TelemetryPacket | None = None
    last_seen_monotonic: float | None = None # For any valid packet
    last_progress_monotonic: float | None = None # For only forward progress packets
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

    def observe(self, packet: TelemetryPacket, now: float) -> tuple[SequenceClass, int]:
        seq = packet.sequence_number
        self.last_seen_monotonic = now # The packet.sent_at is not trusted or guaranteed to be monotonic

        if self.highest_sequence is None:
            seq_class = SequenceClass.FIRST
            gap_size = 0
        elif seq in self.recent_sequence_set:
            seq_class = SequenceClass.DUPLICATE
            gap_size = 0
        elif seq == self.highest_sequence:
            seq_class = SequenceClass.DUPLICATE
            gap_size = 0
        elif seq == self.highest_sequence + 1:
            seq_class = SequenceClass.NORMAL
            gap_size = 0
        elif seq > self.highest_sequence + 1:
            seq_class = SequenceClass.FORWARD_GAP
            gap_size = seq - self.highest_sequence - 1
        else:
            seq_class = SequenceClass.OUT_OF_ORDER
            gap_size = 0

        if seq_class in (
            SequenceClass.FIRST,
            SequenceClass.NORMAL,
            SequenceClass.FORWARD_GAP,
        ):
            self.highest_sequence = seq
            self.latest_telemetry = packet
            self.last_progress_monotonic = now
            self.accepted_count += 1
            self.record_sequence(seq)
            if seq_class is SequenceClass.FORWARD_GAP:
                self.gap_count += 1
        elif seq_class is SequenceClass.DUPLICATE:
            self.duplicate_count += 1
        else:
            self.out_of_order_count += 1
            self.record_sequence(seq)

        return seq_class, gap_size

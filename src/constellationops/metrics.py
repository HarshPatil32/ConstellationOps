from dataclasses import dataclass, fields


@dataclass
class Metrics:
    datagrams_received_total: int = 0
    packets_invalid_total: int = 0
    packets_enqueued_total: int = 0
    packets_queue_dropped_total: int = 0
    packets_processed_total: int = 0
    sequence_gaps_total: int = 0
    duplicate_packets_total: int = 0
    out_of_order_packets_total: int = 0

    def increment(self, name: str, n: int = 1) -> None:
        counter_names = {field.name for field in fields(self)}
        if name not in counter_names:
            raise ValueError(f"unknown counter: {name!r}")
        if n < 1:
            raise ValueError("n must be at least 1")
        setattr(self, name, getattr(self, name) + n)

    def snapshot(self, known_assets: int) -> dict[str, int]:
        return {
            "datagrams_received_total": self.datagrams_received_total,
            "packets_invalid_total": self.packets_invalid_total,
            "packets_enqueued_total": self.packets_enqueued_total,
            "packets_queue_dropped_total": self.packets_queue_dropped_total,
            "packets_processed_total": self.packets_processed_total,
            "sequence_gaps_total": self.sequence_gaps_total,
            "duplicate_packets_total": self.duplicate_packets_total,
            "out_of_order_packets_total": self.out_of_order_packets_total,
            "known_assets": known_assets,
        }

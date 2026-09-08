import asyncio
import time
from collections.abc import Callable
from dataclasses import dataclass, field, fields


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
    clock: Callable[[], float] = field(default=time.monotonic, compare=False)

    _last_processed: int | None = field(default=None, init=False, repr=False, compare=False)
    _last_time: float | None = field(default=None, init=False, repr=False, compare=False)
    packets_per_second: float = field(default=0.0, init=False, repr=False, compare=False)

    def increment(self, name: str, n: int = 1) -> None:
        counter_names = {field.name for field in fields(self) if field.name.endswith("_total")}
        if name not in counter_names:
            raise ValueError(f"unknown counter: {name!r}")
        if n < 1:
            raise ValueError("n must be at least 1")
        setattr(self, name, getattr(self, name) + n)

    def sample_rate(self) -> float:
        now = self.clock()
        current = self.packets_processed_total

        if self._last_time is None:
            rate = 0.0
        else:
            elapsed = now - self._last_time
            if elapsed <= 0:
                rate = 0.0
            else:
                assert self._last_processed is not None
                delta = current - self._last_processed
                rate = delta / elapsed

        self._last_processed = current
        self._last_time = now
        self.packets_per_second = round(rate, 2)
        return self.packets_per_second

    def snapshot(self, known_assets: int) -> dict[str, int | float]:
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
            "packets_per_second": self.packets_per_second,
        }


async def run_metrics_sampler(
    metrics: Metrics,
    interval_seconds: float,
) -> None:
    while True:
        metrics.sample_rate()
        await asyncio.sleep(interval_seconds)

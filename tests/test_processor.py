import asyncio
import logging
from collections.abc import Callable
from datetime import datetime

import pytest

from constellationops.asset_state import AssetState
from constellationops.metrics import Metrics
from constellationops.processor import run_telemetry_processor
from constellationops.registry import AssetRegistry
from constellationops.telemetry import TelemetryPacket


def _registry(**overrides: object) -> AssetRegistry:
    kwargs: dict[str, object] = {
        "recent_sequence_capacity": 50,
    }
    kwargs.update(overrides)
    return AssetRegistry(**kwargs)


def _packet(**overrides: object) -> TelemetryPacket:
    kwargs: dict[str, object] = {
        "asset_id": "sat-001",
        "sequence_number": 1,
        "sent_at": datetime(2026, 9, 6, 12, 0, 0),
        "temperature_c": 22.5,
        "battery_pct": 87.0,
        "signal_dbm": -72.0,
    }
    kwargs.update(overrides)
    return TelemetryPacket(**kwargs)


async def _run_processor(
    queue: asyncio.Queue[TelemetryPacket],
    registry: AssetRegistry,
    metrics: Metrics,
    clock: Callable[[], float] | None = None,
) -> asyncio.Task[None]:
    kwargs: dict[str, object] = {
        "queue": queue,
        "registry": registry,
        "metrics": metrics,
    }
    if clock is not None:
        kwargs["clock"] = clock
    return asyncio.create_task(run_telemetry_processor(**kwargs))


async def test_processor_first_packet_increments_processed_only() -> None:
    queue: asyncio.Queue[TelemetryPacket] = asyncio.Queue()
    registry = _registry()
    metrics = Metrics()

    task = await _run_processor(queue, registry, metrics)
    try:
        await queue.put(_packet(sequence_number=1))
        await queue.join()

        assert metrics.packets_processed_total == 1
        assert metrics.sequence_gaps_total == 0
        assert metrics.duplicate_packets_total == 0
        assert metrics.out_of_order_packets_total == 0
        assert registry.get_or_create("sat-001").accepted_count == 1
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task


async def test_processor_normal_packet_increments_processed_only() -> None:
    queue: asyncio.Queue[TelemetryPacket] = asyncio.Queue()
    registry = _registry()
    metrics = Metrics()

    task = await _run_processor(queue, registry, metrics)
    try:
        await queue.put(_packet(sequence_number=1))
        await queue.put(_packet(sequence_number=2))
        await queue.join()

        assert metrics.packets_processed_total == 2
        assert metrics.sequence_gaps_total == 0
        assert metrics.duplicate_packets_total == 0
        assert metrics.out_of_order_packets_total == 0
        assert registry.get_or_create("sat-001").accepted_count == 2
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task


async def test_processor_forward_gap_increments_sequence_gaps_by_gap_size() -> None:
    queue: asyncio.Queue[TelemetryPacket] = asyncio.Queue()
    registry = _registry()
    metrics = Metrics()

    task = await _run_processor(queue, registry, metrics)
    try:
        await queue.put(_packet(sequence_number=1))
        await queue.put(_packet(sequence_number=5))
        await queue.join()

        assert metrics.packets_processed_total == 2
        assert metrics.sequence_gaps_total == 3
        assert metrics.duplicate_packets_total == 0
        assert metrics.out_of_order_packets_total == 0
        assert registry.get_or_create("sat-001").forward_gap_event_count == 1
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task


async def test_processor_duplicate_increments_duplicate_counter() -> None:
    queue: asyncio.Queue[TelemetryPacket] = asyncio.Queue()
    registry = _registry()
    metrics = Metrics()

    task = await _run_processor(queue, registry, metrics)
    try:
        await queue.put(_packet(sequence_number=1))
        await queue.put(_packet(sequence_number=1))
        await queue.join()

        assert metrics.packets_processed_total == 2
        assert metrics.duplicate_packets_total == 1
        assert metrics.sequence_gaps_total == 0
        assert metrics.out_of_order_packets_total == 0
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task


async def test_processor_out_of_order_increments_out_of_order_counter() -> None:
    queue: asyncio.Queue[TelemetryPacket] = asyncio.Queue()
    registry = _registry()
    metrics = Metrics()

    task = await _run_processor(queue, registry, metrics)
    try:
        await queue.put(_packet(sequence_number=3))
        await queue.put(_packet(sequence_number=1))
        await queue.join()

        assert metrics.packets_processed_total == 2
        assert metrics.out_of_order_packets_total == 1
        assert metrics.sequence_gaps_total == 0
        assert metrics.duplicate_packets_total == 0
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task


async def test_processor_logs_exception_and_continues(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    queue: asyncio.Queue[TelemetryPacket] = asyncio.Queue()
    registry = _registry()
    metrics = Metrics()

    original_observe = AssetState.observe

    def raise_on_bad(
        self: AssetState, packet: TelemetryPacket, now: float
    ) -> tuple[object, int]:
        if packet.asset_id == "sat-bad":
            raise RuntimeError("observe failed")
        return original_observe(self, packet, now)

    monkeypatch.setattr(AssetState, "observe", raise_on_bad)
    caplog.set_level(logging.ERROR)

    task = await _run_processor(queue, registry, metrics)
    try:
        await queue.put(_packet(asset_id="sat-bad", sequence_number=1))
        await queue.put(_packet(asset_id="sat-good", sequence_number=1))
        await queue.join()

        assert metrics.packets_processed_total == 1
        assert registry.get_or_create("sat-good").accepted_count == 1
        assert not task.done()
        assert "sat-bad" in caplog.text
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task


async def test_processor_logs_exception_and_recovers_same_asset(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    queue: asyncio.Queue[TelemetryPacket] = asyncio.Queue()
    registry = _registry()
    metrics = Metrics()

    original_observe = AssetState.observe
    fail_remaining = [1]

    def raise_once_then_observe(
        self: AssetState, packet: TelemetryPacket, now: float
    ) -> tuple[object, int]:
        if fail_remaining[0] > 0:
            fail_remaining[0] -= 1
            raise RuntimeError("observe failed")
        return original_observe(self, packet, now)

    monkeypatch.setattr(AssetState, "observe", raise_once_then_observe)
    caplog.set_level(logging.ERROR)

    task = await _run_processor(queue, registry, metrics)
    try:
        await queue.put(_packet(sequence_number=1))
        await queue.put(_packet(sequence_number=1))
        await queue.put(_packet(sequence_number=2))
        await queue.join()

        assert metrics.packets_processed_total == 2
        state = registry.get_or_create("sat-001")
        assert state.accepted_count == 2
        assert metrics.duplicate_packets_total == 0
        assert not task.done()
        assert "sat-001" in caplog.text
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task


async def test_processor_interleaved_multi_asset_tracks_independent_state() -> None:
    queue: asyncio.Queue[TelemetryPacket] = asyncio.Queue()
    registry = _registry()
    metrics = Metrics()

    task = await _run_processor(queue, registry, metrics)
    try:
        await queue.put(_packet(asset_id="sat-a", sequence_number=1))
        await queue.put(_packet(asset_id="sat-b", sequence_number=1))
        await queue.put(_packet(asset_id="sat-c", sequence_number=1))
        await queue.put(_packet(asset_id="sat-a", sequence_number=2))
        await queue.put(_packet(asset_id="sat-b", sequence_number=5))
        await queue.put(_packet(asset_id="sat-c", sequence_number=1))
        await queue.put(_packet(asset_id="sat-a", sequence_number=2))
        await queue.put(_packet(asset_id="sat-b", sequence_number=3))
        await queue.put(_packet(asset_id="sat-c", sequence_number=3))
        await queue.put(_packet(asset_id="sat-a", sequence_number=3))
        await queue.join()

        assert metrics.packets_processed_total == 10
        assert metrics.sequence_gaps_total == 4
        assert metrics.duplicate_packets_total == 2
        assert metrics.out_of_order_packets_total == 1

        a = registry.get_or_create("sat-a")
        assert a.accepted_count == 3
        assert a.duplicate_count == 1
        assert a.forward_gap_event_count == 0
        assert a.out_of_order_count == 0
        assert a.highest_sequence == 3

        b = registry.get_or_create("sat-b")
        assert b.accepted_count == 2
        assert b.forward_gap_event_count == 1
        assert b.out_of_order_count == 1
        assert b.duplicate_count == 0
        assert b.highest_sequence == 5

        c = registry.get_or_create("sat-c")
        assert c.accepted_count == 2
        assert c.forward_gap_event_count == 1
        assert c.duplicate_count == 1
        assert c.out_of_order_count == 0
        assert c.highest_sequence == 3
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task


async def test_processor_logs_exception_mid_interleave_and_continues_other_assets(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    queue: asyncio.Queue[TelemetryPacket] = asyncio.Queue()
    registry = _registry()
    metrics = Metrics()

    original_observe = AssetState.observe

    def raise_on_bad(
        self: AssetState, packet: TelemetryPacket, now: float
    ) -> tuple[object, int]:
        if packet.asset_id == "sat-bad":
            raise RuntimeError("observe failed")
        return original_observe(self, packet, now)

    monkeypatch.setattr(AssetState, "observe", raise_on_bad)
    caplog.set_level(logging.ERROR)

    task = await _run_processor(queue, registry, metrics)
    try:
        await queue.put(_packet(asset_id="sat-a", sequence_number=1))
        await queue.put(_packet(asset_id="sat-bad", sequence_number=1))
        await queue.put(_packet(asset_id="sat-c", sequence_number=1))
        await queue.put(_packet(asset_id="sat-a", sequence_number=2))
        await queue.put(_packet(asset_id="sat-bad", sequence_number=2))
        await queue.put(_packet(asset_id="sat-c", sequence_number=5))
        await queue.put(_packet(asset_id="sat-a", sequence_number=2))
        await queue.put(_packet(asset_id="sat-bad", sequence_number=3))
        await queue.put(_packet(asset_id="sat-c", sequence_number=3))
        await queue.put(_packet(asset_id="sat-a", sequence_number=3))
        await queue.join()

        assert metrics.packets_processed_total == 7
        assert metrics.sequence_gaps_total == 3
        assert metrics.duplicate_packets_total == 1
        assert metrics.out_of_order_packets_total == 1

        a = registry.get_or_create("sat-a")
        assert a.accepted_count == 3
        assert a.duplicate_count == 1

        c = registry.get_or_create("sat-c")
        assert c.accepted_count == 2
        assert c.forward_gap_event_count == 1
        assert c.out_of_order_count == 1

        bad = registry.get_or_create("sat-bad")
        assert bad.accepted_count == 0
        assert bad.highest_sequence is None

        assert not task.done()
        assert "sat-bad" in caplog.text
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task


async def test_processor_cancels_cleanly_while_waiting_on_empty_queue() -> None:
    queue: asyncio.Queue[TelemetryPacket] = asyncio.Queue()
    registry = _registry()
    metrics = Metrics()

    task = await _run_processor(queue, registry, metrics)
    await asyncio.sleep(0.05)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


async def test_processor_passes_injected_clock_to_observe() -> None:
    queue: asyncio.Queue[TelemetryPacket] = asyncio.Queue()
    registry = _registry()
    metrics = Metrics()
    now = [42.0]

    def clock() -> float:
        return now[0]

    task = await _run_processor(queue, registry, metrics, clock=clock)
    try:
        await queue.put(_packet(sequence_number=1))
        await queue.join()

        state = registry.get_or_create("sat-001")
        assert state.last_seen_monotonic == 42.0
        assert state.last_progress_monotonic == 42.0
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

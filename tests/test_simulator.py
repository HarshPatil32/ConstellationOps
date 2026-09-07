import asyncio
import random
import time
from collections import Counter

import pytest

from constellationops.ingest import TelemetryProtocol
from constellationops.metrics import Metrics
from constellationops.simulator import (
    SimulatorStats,
    _DEFAULT_ASSETS,
    _DEFAULT_DROP_PROB,
    _DEFAULT_DUP_PROB,
    _DEFAULT_HOST,
    _DEFAULT_PORT,
    _DEFAULT_RATE,
    _DEFAULT_REORDER_DELAY_MS,
    _DEFAULT_REORDER_PROB,
    _asset_rng,
    _build_parser,
    _delayed_send,
    _format_summary,
    _generate_telemetry_values,
    _prune_pending,
    _run_asset,
    _validate_args,
    run_simulator,
)
from constellationops.telemetry import TelemetryPacket, decode_packet, encode_packet


async def _bind_receiver(
    queue: asyncio.Queue[TelemetryPacket],
    metrics: Metrics,
) -> tuple[asyncio.DatagramTransport, tuple[str, int]]:
    loop = asyncio.get_running_loop()
    transport, _protocol = await loop.create_datagram_endpoint(
        lambda: TelemetryProtocol(queue, metrics),
        local_addr=("127.0.0.1", 0),
    )
    sockname = transport.get_extra_info("sockname")
    assert sockname is not None
    return transport, sockname


async def _collect_packets(
    queue: asyncio.Queue[TelemetryPacket],
    timeout: float,
) -> list[TelemetryPacket]:
    packets: list[TelemetryPacket] = []
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        try:
            packets.append(
                await asyncio.wait_for(
                    queue.get(),
                    timeout=deadline - asyncio.get_running_loop().time(),
                )
            )
        except TimeoutError:
            break
    return packets


def test_seeded_values_are_deterministic() -> None:
    rng_a = _asset_rng(42, 1)
    rng_b = _asset_rng(42, 1)
    assert _generate_telemetry_values(rng_a) == _generate_telemetry_values(rng_b)

    rng_c = _asset_rng(99, 1)
    assert _generate_telemetry_values(rng_a) != _generate_telemetry_values(rng_c)


def test_asset_rng_is_independent_per_asset() -> None:
    seed = 42
    rng_first = _asset_rng(seed, 1)
    rng_second = _asset_rng(seed, 2)

    assert rng_first is not rng_second
    assert _generate_telemetry_values(rng_first) != _generate_telemetry_values(rng_second)


def test_asset_rngs_do_not_interfere() -> None:
    expected_b = _generate_telemetry_values(_asset_rng(42, 2))

    rng_a = _asset_rng(42, 1)
    rng_b = _asset_rng(42, 2)
    for _ in range(10):
        _generate_telemetry_values(rng_a)

    assert _generate_telemetry_values(rng_b) == expected_b


def test_seeded_values_respect_bounds() -> None:
    rng = random.Random(123)
    for _ in range(100):
        temperature_c, battery_pct, signal_dbm = _generate_telemetry_values(rng)
        assert -20.0 <= temperature_c <= 60.0
        assert 0.0 <= battery_pct <= 100.0
        assert -120.0 <= signal_dbm <= -30.0


def test_cli_defaults() -> None:
    args = _build_parser().parse_args([])
    assert args.assets == _DEFAULT_ASSETS
    assert args.rate == _DEFAULT_RATE
    assert args.host == _DEFAULT_HOST
    assert args.port == _DEFAULT_PORT
    assert args.duration is None
    assert args.seed is None
    assert args.drop_prob == _DEFAULT_DROP_PROB
    assert args.dup_prob == _DEFAULT_DUP_PROB
    assert args.reorder_prob == _DEFAULT_REORDER_PROB
    assert args.reorder_delay_ms == _DEFAULT_REORDER_DELAY_MS


def test_cli_overrides() -> None:
    args = _build_parser().parse_args(
        [
            "--assets",
            "3",
            "--rate",
            "2.5",
            "--host",
            "10.0.0.1",
            "--port",
            "1234",
            "--duration",
            "10",
            "--seed",
            "7",
            "--drop-prob",
            "0.1",
            "--dup-prob",
            "0.2",
            "--reorder-prob",
            "0.3",
            "--reorder-delay-ms",
            "50",
        ]
    )
    assert args.assets == 3
    assert args.rate == 2.5
    assert args.host == "10.0.0.1"
    assert args.port == 1234
    assert args.duration == 10.0
    assert args.seed == 7
    assert args.drop_prob == 0.1
    assert args.dup_prob == 0.2
    assert args.reorder_prob == 0.3
    assert args.reorder_delay_ms == 50.0


def test_cli_rejects_invalid_values(capsys: pytest.CaptureFixture[str]) -> None:
    parser = _build_parser()
    with pytest.raises(SystemExit):
        _validate_args(parser.parse_args(["--rate", "0"]), parser)
    assert "rate" in capsys.readouterr().err

    with pytest.raises(SystemExit):
        _validate_args(parser.parse_args(["--port", "0"]), parser)
    assert "port" in capsys.readouterr().err

    with pytest.raises(SystemExit):
        _validate_args(parser.parse_args(["--assets", "0"]), parser)
    assert "assets" in capsys.readouterr().err

    with pytest.raises(SystemExit):
        _validate_args(parser.parse_args(["--duration", "0"]), parser)
    assert "duration" in capsys.readouterr().err

    with pytest.raises(SystemExit):
        _validate_args(parser.parse_args(["--drop-prob", "-0.1"]), parser)
    assert "drop-prob" in capsys.readouterr().err

    with pytest.raises(SystemExit):
        _validate_args(parser.parse_args(["--dup-prob", "1.5"]), parser)
    assert "dup-prob" in capsys.readouterr().err

    with pytest.raises(SystemExit):
        _validate_args(parser.parse_args(["--reorder-delay-ms", "-1"]), parser)
    assert "reorder-delay-ms" in capsys.readouterr().err

    with pytest.raises(SystemExit):
        _validate_args(
            parser.parse_args(["--reorder-prob", "1", "--reorder-delay-ms", "0"]),
            parser,
        )
    assert "reorder-delay-ms" in capsys.readouterr().err


def test_format_summary() -> None:
    stats = SimulatorStats(generated=10, dropped=2, duplicated=3, delayed=1)
    assert _format_summary(stats) == "generated=10 dropped=2 duplicated=3 delayed=1"


class _RecordingTransport:
    def __init__(self, fail: bool = False) -> None:
        self.sent: list[bytes] = []
        self.fail = fail

    def sendto(self, data: bytes, addr: object = None) -> None:
        if self.fail:
            raise OSError("simulated send failure")
        self.sent.append(data)


class _ScriptedRandom:
    def __init__(self, values: list[float]) -> None:
        self._values = values
        self._index = 0

    def uniform(self, _a: float, _b: float) -> float:
        return 0.0

    def random(self) -> float:
        if self._index >= len(self._values):
            return 1.0
        value = self._values[self._index]
        self._index += 1
        return value


async def test_drop_advances_sequence_without_sending() -> None:
    transport = _RecordingTransport()
    loop = asyncio.get_running_loop()
    deadline = loop.time() + 0.05
    rng = _ScriptedRandom(
        [
            0.0,  # drop first packet
            1.0,
            1.0,
            1.0,
            1.0,
            1.0,
        ]
    )

    stats = await _run_asset(
        "sat-001",
        transport,  # type: ignore[arg-type]
        rate_hz=100.0,
        deadline=deadline,
        rng=rng,  # type: ignore[arg-type]
        drop_prob=0.5,
        dup_prob=0.0,
        reorder_prob=0.0,
        reorder_delay_ms=0.0,
    )

    assert stats.generated >= 1
    assert stats.dropped >= 1
    sequences = [decode_packet(raw).sequence_number for raw in transport.sent]
    assert sequences == sorted(sequences)
    assert len(set(sequences)) == len(sequences)
    assert sequences[0] >= 1
    assert stats.dropped + len(transport.sent) == stats.generated


def test_prune_pending_removes_completed_tasks() -> None:
    async def noop() -> None:
        return None

    loop = asyncio.new_event_loop()
    try:
        done = loop.create_task(noop())
        loop.run_until_complete(done)
        pending: list[asyncio.Task[None]] = [done, loop.create_task(noop())]
        _prune_pending(pending)
        assert len(pending) == 1
        assert not pending[0].done()
        loop.run_until_complete(asyncio.gather(*pending))
    finally:
        loop.close()


async def test_drop_takes_precedence_over_dup_and_reorder() -> None:
    transport = _RecordingTransport()
    loop = asyncio.get_running_loop()
    rng = _ScriptedRandom([0.0, 0.0, 0.0])

    stats = await _run_asset(
        "sat-001",
        transport,  # type: ignore[arg-type]
        rate_hz=100.0,
        deadline=loop.time() + 0.01,
        rng=rng,  # type: ignore[arg-type]
        drop_prob=0.5,
        dup_prob=0.5,
        reorder_prob=0.5,
        reorder_delay_ms=50.0,
    )

    assert stats.generated == 1
    assert stats.dropped == 1
    assert stats.duplicated == 0
    assert stats.delayed == 0
    assert transport.sent == []


async def test_delayed_send_logs_send_failure(
    caplog: pytest.LogCaptureFixture,
) -> None:
    transport = _RecordingTransport(fail=True)
    payload = b"test-payload"

    await _delayed_send(transport, payload, 0.0, duplicate=False)  # type: ignore[arg-type]

    assert transport.sent == []
    assert any("delayed send failed" in record.message for record in caplog.records)


async def test_cancelled_asset_returns_partial_stats() -> None:
    transport = _RecordingTransport()
    rng = _ScriptedRandom([1.0, 1.0, 1.0])
    task = asyncio.create_task(
        _run_asset(
            "sat-001",
            transport,  # type: ignore[arg-type]
            rate_hz=1.0,
            deadline=None,
            rng=rng,  # type: ignore[arg-type]
            drop_prob=0.0,
            dup_prob=0.0,
            reorder_prob=0.0,
            reorder_delay_ms=0.0,
        )
    )
    await asyncio.sleep(0)
    task.cancel()
    stats = await task

    assert isinstance(stats, SimulatorStats)
    assert stats.generated >= 1


async def test_duplicate_sends_identical_bytes_twice() -> None:
    queue: asyncio.Queue[TelemetryPacket] = asyncio.Queue(maxsize=1000)
    metrics = Metrics()
    transport, (host, port) = await _bind_receiver(queue, metrics)

    try:
        stats = await run_simulator(
            assets=1,
            rate=20.0,
            host=host,
            port=port,
            duration=0.15,
            seed=5,
            dup_prob=1.0,
        )
        packets = await _collect_packets(queue, timeout=0.5)

        assert stats.duplicated == stats.generated
        by_sequence: dict[int, list[TelemetryPacket]] = {}
        for packet in packets:
            by_sequence.setdefault(packet.sequence_number, []).append(packet)
        for sequence_packets in by_sequence.values():
            assert len(sequence_packets) == 2
            first, second = sequence_packets
            assert encode_packet(first) == encode_packet(second)
    finally:
        transport.close()


async def test_reorder_causes_later_packet_to_arrive_first() -> None:
    queue: asyncio.Queue[TelemetryPacket] = asyncio.Queue(maxsize=1000)
    metrics = Metrics()
    receiver, (host, port) = await _bind_receiver(queue, metrics)
    sender_loop = asyncio.get_running_loop()
    sender_transport, _ = await sender_loop.create_datagram_endpoint(
        asyncio.DatagramProtocol,
        remote_addr=(host, port),
    )

    rng = _ScriptedRandom(
        [
            1.0,  # tick 0: drop no
            1.0,  # tick 0: dup no
            0.0,  # tick 0: reorder yes
            1.0,  # tick 1: drop no
            1.0,  # tick 1: dup no
            1.0,  # tick 1: reorder no
        ]
    )

    try:
        stats = await _run_asset(
            "sat-001",
            sender_transport,
            rate_hz=100.0,
            deadline=sender_loop.time() + 0.015,
            rng=rng,  # type: ignore[arg-type]
            drop_prob=0.0,
            dup_prob=0.0,
            reorder_prob=0.5,
            reorder_delay_ms=100.0,
        )
        packets = await _collect_packets(queue, timeout=0.5)

        assert stats.delayed >= 1
        assert stats.generated == 2
        sat_packets = [packet for packet in packets if packet.asset_id == "sat-001"]
        arrival_order = [packet.sequence_number for packet in sat_packets]
        assert arrival_order.index(1) < arrival_order.index(0)
    finally:
        sender_transport.close()
        receiver.close()


async def test_run_simulator_waits_for_delayed_sends() -> None:
    queue: asyncio.Queue[TelemetryPacket] = asyncio.Queue(maxsize=1000)
    metrics = Metrics()
    transport, (host, port) = await _bind_receiver(queue, metrics)
    delay_ms = 80.0

    try:
        started = time.monotonic()
        stats = await run_simulator(
            assets=1,
            rate=10.0,
            host=host,
            port=port,
            duration=0.05,
            seed=4,
            reorder_prob=1.0,
            reorder_delay_ms=delay_ms,
        )
        elapsed_ms = (time.monotonic() - started) * 1000

        packets = await _collect_packets(queue, timeout=0.5)
        assert stats.delayed == stats.generated
        assert stats.generated >= 1
        assert elapsed_ms >= delay_ms
        assert packets
    finally:
        transport.close()


async def test_fault_injection_is_deterministic_with_seed() -> None:
    async def run_once() -> tuple[SimulatorStats, list[tuple[str, int, float, float, float]]]:
        queue: asyncio.Queue[TelemetryPacket] = asyncio.Queue(maxsize=1000)
        metrics = Metrics()
        transport, (host, port) = await _bind_receiver(queue, metrics)
        try:
            stats = await run_simulator(
                assets=2,
                rate=50.0,
                host=host,
                port=port,
                duration=0.05,
                seed=42,
                drop_prob=0.2,
                dup_prob=0.2,
                reorder_prob=0.2,
                reorder_delay_ms=100.0,
            )
            packets = await _collect_packets(queue, timeout=0.5)
            received = [
                (
                    packet.asset_id,
                    packet.sequence_number,
                    packet.temperature_c,
                    packet.battery_pct,
                    packet.signal_dbm,
                )
                for packet in packets
            ]
            return stats, received
        finally:
            transport.close()

    first_stats, first_received = await run_once()
    second_stats, second_received = await run_once()
    assert first_stats == second_stats
    assert Counter(first_received) == Counter(second_received)


async def test_run_simulator_produces_deterministic_values_with_seed() -> None:
    async def collect_packets_for_seed(seed: int) -> dict[str, TelemetryPacket]:
        queue: asyncio.Queue[TelemetryPacket] = asyncio.Queue(maxsize=1000)
        metrics = Metrics()
        transport, (host, port) = await _bind_receiver(queue, metrics)
        try:
            await run_simulator(
                assets=2,
                rate=50.0,
                host=host,
                port=port,
                duration=0.05,
                seed=seed,
            )
            packets = await _collect_packets(queue, timeout=0.5)
        finally:
            transport.close()

        first_by_asset: dict[str, TelemetryPacket] = {}
        for packet in packets:
            if packet.asset_id not in first_by_asset:
                first_by_asset[packet.asset_id] = packet
        return first_by_asset

    first_run = await collect_packets_for_seed(42)
    second_run = await collect_packets_for_seed(42)

    assert set(first_run) == {"sat-001", "sat-002"}
    for asset_id in ("sat-001", "sat-002"):
        first_packet = first_run[asset_id]
        second_packet = second_run[asset_id]
        assert first_packet.temperature_c == second_packet.temperature_c
        assert first_packet.battery_pct == second_packet.battery_pct
        assert first_packet.signal_dbm == second_packet.signal_dbm

    sat_001 = first_run["sat-001"]
    sat_002 = first_run["sat-002"]
    assert (
        sat_001.temperature_c,
        sat_001.battery_pct,
        sat_001.signal_dbm,
    ) != (
        sat_002.temperature_c,
        sat_002.battery_pct,
        sat_002.signal_dbm,
    )


async def test_inner_asset_failure_does_not_cancel_others(monkeypatch: pytest.MonkeyPatch) -> None:
    def failing_encode_packet(packet: TelemetryPacket) -> bytes:
        if packet.asset_id == "sat-001":
            raise RuntimeError("simulated encode failure")
        return encode_packet(packet)

    monkeypatch.setattr("constellationops.simulator.encode_packet", failing_encode_packet)

    queue: asyncio.Queue[TelemetryPacket] = asyncio.Queue(maxsize=1000)
    metrics = Metrics()
    transport, (host, port) = await _bind_receiver(queue, metrics)

    try:
        await run_simulator(
            assets=2,
            rate=20.0,
            host=host,
            port=port,
            duration=0.3,
            seed=2,
        )
        packets = await _collect_packets(queue, timeout=0.5)
        asset_ids = {packet.asset_id for packet in packets}
        assert "sat-002" in asset_ids
        assert "sat-001" not in asset_ids
    finally:
        transport.close()


async def test_end_to_end_over_real_socket() -> None:
    queue: asyncio.Queue[TelemetryPacket] = asyncio.Queue(maxsize=1000)
    metrics = Metrics()
    transport, (host, port) = await _bind_receiver(queue, metrics)

    try:
        await run_simulator(
            assets=2,
            rate=20.0,
            host=host,
            port=port,
            duration=0.3,
            seed=1,
        )
        packets = await _collect_packets(queue, timeout=0.5)

        assert packets
        by_asset: dict[str, list[TelemetryPacket]] = {}
        for packet in packets:
            by_asset.setdefault(packet.asset_id, []).append(packet)

        assert set(by_asset) == {"sat-001", "sat-002"}
        for asset_packets in by_asset.values():
            sequences = [packet.sequence_number for packet in asset_packets]
            assert sequences == list(range(len(sequences)))
    finally:
        transport.close()


async def test_one_asset_failure_does_not_cancel_others(monkeypatch: pytest.MonkeyPatch) -> None:
    original_run_asset = _run_asset

    async def failing_run_asset(asset_id: str, *args: object, **kwargs: object) -> None:
        if asset_id == "sat-001":
            raise RuntimeError("simulated failure")
        await original_run_asset(asset_id, *args, **kwargs)

    monkeypatch.setattr("constellationops.simulator._run_asset", failing_run_asset)

    queue: asyncio.Queue[TelemetryPacket] = asyncio.Queue(maxsize=1000)
    metrics = Metrics()
    transport, (host, port) = await _bind_receiver(queue, metrics)

    try:
        await run_simulator(
            assets=2,
            rate=20.0,
            host=host,
            port=port,
            duration=0.3,
            seed=2,
        )
        packets = await _collect_packets(queue, timeout=0.5)
        asset_ids = {packet.asset_id for packet in packets}
        assert "sat-002" in asset_ids
        assert "sat-001" not in asset_ids
    finally:
        transport.close()


async def test_duration_bounds_runtime() -> None:
    queue: asyncio.Queue[TelemetryPacket] = asyncio.Queue(maxsize=1000)
    metrics = Metrics()
    transport, (host, port) = await _bind_receiver(queue, metrics)
    duration = 0.2

    try:
        started = time.monotonic()
        await run_simulator(
            assets=1,
            rate=10.0,
            host=host,
            port=port,
            duration=duration,
            seed=3,
        )
        elapsed = time.monotonic() - started
        assert elapsed < duration * 3
    finally:
        transport.close()

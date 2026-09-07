import asyncio
import random
import time

import pytest

from constellationops.ingest import TelemetryProtocol
from constellationops.metrics import Metrics
from constellationops.simulator import (
    _DEFAULT_ASSETS,
    _DEFAULT_HOST,
    _DEFAULT_PORT,
    _DEFAULT_RATE,
    _build_parser,
    _generate_telemetry_values,
    _run_asset,
    _validate_args,
    run_simulator,
)
from constellationops.telemetry import TelemetryPacket, encode_packet


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
    rng_a = random.Random(42 + 1)
    rng_b = random.Random(42 + 1)
    assert _generate_telemetry_values(rng_a) == _generate_telemetry_values(rng_b)

    rng_c = random.Random(99 + 1)
    assert _generate_telemetry_values(rng_a) != _generate_telemetry_values(rng_c)


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


def test_cli_overrides() -> None:
    args = _build_parser().parse_args(
        ["--assets", "3", "--rate", "2.5", "--host", "10.0.0.1", "--port", "1234", "--duration", "10", "--seed", "7"]
    )
    assert args.assets == 3
    assert args.rate == 2.5
    assert args.host == "10.0.0.1"
    assert args.port == 1234
    assert args.duration == 10.0
    assert args.seed == 7


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

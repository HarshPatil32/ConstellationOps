import asyncio
import socket
from collections.abc import Callable
from datetime import datetime

from constellationops.ingest import TelemetryProtocol
from constellationops.metrics import Metrics
from constellationops.telemetry import TelemetryPacket, encode_packet


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


async def _bind_server(
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


def _send_datagram(data: bytes, addr: tuple[str, int]) -> None:
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
        sock.sendto(data, addr)


async def _wait_until(condition: Callable[[], bool], timeout: float = 1.0) -> None:
    deadline = asyncio.get_running_loop().time() + timeout
    while not condition():
        if asyncio.get_running_loop().time() >= deadline:
            raise AssertionError("timed out waiting for condition")
        await asyncio.sleep(0)


async def test_valid_packet_over_real_socket_enqueues_and_increments_counters() -> None:
    queue: asyncio.Queue[TelemetryPacket] = asyncio.Queue(maxsize=10)
    metrics = Metrics()
    packet = _packet()
    transport, addr = await _bind_server(queue, metrics)

    try:
        _send_datagram(encode_packet(packet), addr)
        received = await asyncio.wait_for(queue.get(), timeout=1.0)

        assert received == packet
        assert metrics.datagrams_received_total == 1
        assert metrics.packets_enqueued_total == 1
        assert metrics.packets_invalid_total == 0
        assert metrics.packets_queue_dropped_total == 0
    finally:
        transport.close()


async def test_garbage_datagram_increments_invalid_and_receiver_survives() -> None:
    queue: asyncio.Queue[TelemetryPacket] = asyncio.Queue(maxsize=10)
    metrics = Metrics()
    packet = _packet(sequence_number=2)
    transport, addr = await _bind_server(queue, metrics)

    try:
        _send_datagram(b"not a valid packet", addr)
        _send_datagram(encode_packet(packet), addr)
        received = await asyncio.wait_for(queue.get(), timeout=1.0)

        assert received == packet
        assert metrics.datagrams_received_total == 2
        assert metrics.packets_invalid_total == 1
        assert metrics.packets_enqueued_total == 1
        assert metrics.packets_queue_dropped_total == 0
    finally:
        transport.close()


async def test_full_queue_drops_packet_and_services_subsequent_traffic() -> None:
    queue: asyncio.Queue[TelemetryPacket] = asyncio.Queue(maxsize=2)
    queue.put_nowait(_packet(sequence_number=0))
    queue.put_nowait(_packet(sequence_number=1))
    metrics = Metrics()
    follow_up = _packet(sequence_number=3)
    transport, addr = await _bind_server(queue, metrics)

    try:
        _send_datagram(encode_packet(_packet(sequence_number=2)), addr)
        await _wait_until(lambda: metrics.packets_queue_dropped_total == 1)

        assert queue.qsize() == 2
        assert metrics.datagrams_received_total == 1
        assert metrics.packets_enqueued_total == 0
        assert metrics.packets_invalid_total == 0

        await queue.get()
        _send_datagram(encode_packet(follow_up), addr)
        await _wait_until(lambda: metrics.packets_enqueued_total == 1)

        remaining = await asyncio.wait_for(queue.get(), timeout=1.0)
        assert remaining.sequence_number == 1
        received = await asyncio.wait_for(queue.get(), timeout=1.0)
        assert received == follow_up
        assert metrics.packets_queue_dropped_total == 1
        assert queue.empty()
    finally:
        transport.close()

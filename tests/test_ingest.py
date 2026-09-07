import asyncio
import json
import logging
from datetime import datetime

import pytest

from constellationops.ingest import TelemetryProtocol
from constellationops.metrics import Metrics
from constellationops.telemetry import TelemetryPacket, encode_packet

_ADDR = ("127.0.0.1", 5000)


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


def _protocol(
    queue: asyncio.Queue[TelemetryPacket] | None = None,
    metrics: Metrics | None = None,
) -> TelemetryProtocol:
    return TelemetryProtocol(
        queue=queue or asyncio.Queue(),
        metrics=metrics or Metrics(),
    )


def test_valid_packet_enqueues_and_increments_counters() -> None:
    queue: asyncio.Queue[TelemetryPacket] = asyncio.Queue(maxsize=10)
    metrics = Metrics()
    protocol = _protocol(queue, metrics)
    packet = _packet()

    protocol.datagram_received(encode_packet(packet), _ADDR)

    assert metrics.datagrams_received_total == 1
    assert metrics.packets_enqueued_total == 1
    assert metrics.packets_invalid_total == 0
    assert metrics.packets_queue_dropped_total == 0
    assert queue.get_nowait() == packet


def test_invalid_utf8_increments_invalid_and_does_not_enqueue() -> None:
    queue: asyncio.Queue[TelemetryPacket] = asyncio.Queue()
    metrics = Metrics()
    protocol = _protocol(queue, metrics)

    protocol.datagram_received(b"\xff\xfe", _ADDR)

    assert metrics.datagrams_received_total == 1
    assert metrics.packets_invalid_total == 1
    assert metrics.packets_enqueued_total == 0
    assert metrics.packets_queue_dropped_total == 0
    assert queue.empty()


def test_invalid_json_increments_invalid_and_does_not_enqueue() -> None:
    queue: asyncio.Queue[TelemetryPacket] = asyncio.Queue()
    metrics = Metrics()
    protocol = _protocol(queue, metrics)

    protocol.datagram_received(b"not json", _ADDR)

    assert metrics.datagrams_received_total == 1
    assert metrics.packets_invalid_total == 1
    assert metrics.packets_enqueued_total == 0
    assert queue.empty()


def test_empty_datagram_increments_invalid_and_does_not_enqueue() -> None:
    queue: asyncio.Queue[TelemetryPacket] = asyncio.Queue()
    metrics = Metrics()
    protocol = _protocol(queue, metrics)

    protocol.datagram_received(b"", _ADDR)

    assert metrics.datagrams_received_total == 1
    assert metrics.packets_invalid_total == 1
    assert metrics.packets_enqueued_total == 0
    assert queue.empty()


def test_invalid_schema_increments_invalid_and_does_not_enqueue() -> None:
    queue: asyncio.Queue[TelemetryPacket] = asyncio.Queue()
    metrics = Metrics()
    protocol = _protocol(queue, metrics)
    payload = json.dumps(
        {
            "asset_id": "sat-001",
            "sequence_number": 1,
            "sent_at": "2026-09-06T12:00:00",
            "temperature_c": 22.5,
            "battery_pct": 150.0,
            "signal_dbm": -72.0,
        }
    ).encode()

    protocol.datagram_received(payload, _ADDR)

    assert metrics.datagrams_received_total == 1
    assert metrics.packets_invalid_total == 1
    assert metrics.packets_enqueued_total == 0
    assert queue.empty()


def test_full_queue_increments_dropped_and_does_not_enqueue() -> None:
    queue: asyncio.Queue[TelemetryPacket] = asyncio.Queue(maxsize=1)
    queue.put_nowait(_packet(sequence_number=0))
    metrics = Metrics()
    protocol = _protocol(queue, metrics)

    protocol.datagram_received(encode_packet(_packet(sequence_number=1)), _ADDR)

    assert metrics.datagrams_received_total == 1
    assert metrics.packets_queue_dropped_total == 1
    assert metrics.packets_enqueued_total == 0
    assert metrics.packets_invalid_total == 0
    assert queue.qsize() == 1
    assert queue.get_nowait().sequence_number == 0


def test_unexpected_exception_is_logged_and_does_not_propagate(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    queue: asyncio.Queue[TelemetryPacket] = asyncio.Queue()
    metrics = Metrics()
    protocol = _protocol(queue, metrics)

    def boom(_raw: bytes) -> TelemetryPacket:
        raise RuntimeError("boom")

    monkeypatch.setattr("constellationops.ingest.decode_packet", boom)
    caplog.set_level(logging.ERROR)

    protocol.datagram_received(encode_packet(_packet()), _ADDR)

    assert metrics.datagrams_received_total == 1
    assert metrics.packets_invalid_total == 0
    assert metrics.packets_enqueued_total == 0
    assert metrics.packets_queue_dropped_total == 0
    assert queue.empty()
    assert "unexpected error" in caplog.text
    assert "sat-001" not in caplog.text


def test_receiver_survives_after_unexpected_exception(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    queue: asyncio.Queue[TelemetryPacket] = asyncio.Queue()
    metrics = Metrics()
    protocol = _protocol(queue, metrics)
    packet = _packet(sequence_number=2)

    def boom(_raw: bytes) -> TelemetryPacket:
        raise RuntimeError("boom")

    monkeypatch.setattr("constellationops.ingest.decode_packet", boom)
    protocol.datagram_received(encode_packet(_packet(sequence_number=1)), _ADDR)

    monkeypatch.undo()
    protocol.datagram_received(encode_packet(packet), _ADDR)

    assert metrics.datagrams_received_total == 2
    assert metrics.packets_enqueued_total == 1
    assert queue.get_nowait() == packet


def test_error_received_logs_and_does_not_raise(
    caplog: pytest.LogCaptureFixture,
) -> None:
    metrics = Metrics()
    protocol = _protocol(metrics=metrics)
    caplog.set_level(logging.ERROR)

    protocol.error_received(OSError("network unreachable"))

    assert metrics.datagrams_received_total == 0
    assert "network unreachable" in caplog.text

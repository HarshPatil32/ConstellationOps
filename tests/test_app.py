import asyncio
import socket
import time
from datetime import datetime

import pytest
from fastapi.testclient import TestClient

from constellationops.app import app
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


def _send_datagram(data: bytes, addr: tuple[str, int]) -> None:
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
        sock.sendto(data, addr)


def _wait_until(condition, timeout: float = 1.0) -> None:
    deadline = time.monotonic() + timeout
    while not condition():
        if time.monotonic() >= deadline:
            raise AssertionError("timed out waiting for condition")
        time.sleep(0.01)


@pytest.fixture
def app_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("UDP_HOST", "127.0.0.1")
    monkeypatch.setenv("UDP_PORT", "0")


def test_lifespan_populates_app_state(app_env: None) -> None:
    with TestClient(app) as client:
        assert client.app.state.settings is not None
        assert client.app.state.metrics is not None
        assert client.app.state.registry is not None
        assert client.app.state.queue is not None
        assert client.app.state.transport is not None
        assert client.app.state.processor_task is not None
        assert client.app.state.health_task is not None
        assert not client.app.state.processor_task.done()
        assert not client.app.state.health_task.done()


def test_lifespan_processes_udp_packet_end_to_end(app_env: None) -> None:
    with TestClient(app) as client:
        sockname = client.app.state.transport.get_extra_info("sockname")
        assert sockname is not None

        _send_datagram(encode_packet(_packet()), sockname)

        _wait_until(lambda: client.app.state.registry.known_assets == 1)
        _wait_until(lambda: client.app.state.metrics.packets_processed_total == 1)


def test_lifespan_shuts_down_cleanly(app_env: None) -> None:
    processor_task = None
    health_task = None
    transport = None

    with TestClient(app) as client:
        processor_task = client.app.state.processor_task
        health_task = client.app.state.health_task
        transport = client.app.state.transport

    assert processor_task is not None
    assert health_task is not None
    assert transport is not None
    assert processor_task.cancelled() or processor_task.done()
    assert health_task.cancelled() or health_task.done()
    assert transport.is_closing()


async def _failing_create_datagram_endpoint(self, *args, **kwargs):  # noqa: ANN001, ANN002, ANN003
    raise OSError("Address already in use")


def test_lifespan_startup_failure_does_not_leak_resources(
    app_env: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        asyncio.BaseEventLoop,
        "create_datagram_endpoint",
        _failing_create_datagram_endpoint,
    )

    with pytest.raises(OSError, match="Address already in use"):
        with TestClient(app):
            pass


def test_main_runs_uvicorn_with_settings(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[tuple[object, str, int]] = []

    def fake_run(app_obj: object, *, host: str, port: int) -> None:
        calls.append((app_obj, host, port))

    monkeypatch.setenv("HTTP_HOST", "127.0.0.1")
    monkeypatch.setenv("HTTP_PORT", "8080")
    monkeypatch.setattr("constellationops.__main__.uvicorn.run", fake_run)

    from constellationops.__main__ import main

    main()

    assert len(calls) == 1
    assert calls[0][0] is app
    assert calls[0][1] == "127.0.0.1"
    assert calls[0][2] == 8080

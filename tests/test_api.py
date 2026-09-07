import socket
import time
from datetime import datetime

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


def test_get_health(app_env: None) -> None:
    with TestClient(app) as client:
        response = client.get("/health")
        queue_capacity = client.app.state.settings.queue_capacity

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "ok"
    assert body["uptime_seconds"] >= 0
    assert body["queue_size"] == 0
    assert body["queue_capacity"] == queue_capacity


def test_get_assets_empty(app_env: None) -> None:
    with TestClient(app) as client:
        response = client.get("/assets")

    assert response.status_code == 200
    assert response.json() == []


def test_get_assets_after_processing(app_env: None) -> None:
    with TestClient(app) as client:
        sockname = client.app.state.transport.get_extra_info("sockname")
        assert sockname is not None

        _send_datagram(encode_packet(_packet()), sockname)
        _wait_until(lambda: client.app.state.registry.known_assets == 1)

        response = client.get("/assets")

    assert response.status_code == 200
    assets = response.json()
    assert len(assets) == 1
    asset = assets[0]
    assert asset["asset_id"] == "sat-001"
    assert asset["health"] == "ONLINE"
    assert asset["highest_sequence"] == 1
    assert asset["accepted_count"] == 1
    assert asset["forward_gap_event_count"] == 0
    assert asset["duplicate_count"] == 0
    assert asset["out_of_order_count"] == 0
    assert asset["last_seen_age_seconds"] is not None
    assert asset["last_seen_age_seconds"] >= 0


def test_get_asset_unknown_returns_404(app_env: None) -> None:
    with TestClient(app) as client:
        response = client.get("/assets/unknown-sat")

    assert response.status_code == 404
    assert response.json() == {"detail": "unknown asset: unknown-sat"}


def test_get_asset_known_returns_full_state(app_env: None) -> None:
    sent = _packet(sequence_number=42, temperature_c=19.0)

    with TestClient(app) as client:
        sockname = client.app.state.transport.get_extra_info("sockname")
        assert sockname is not None

        _send_datagram(encode_packet(sent), sockname)
        _wait_until(lambda: client.app.state.registry.known_assets == 1)

        response = client.get("/assets/sat-001")

    assert response.status_code == 200
    body = response.json()
    assert body["asset_id"] == "sat-001"
    assert body["highest_sequence"] == 42
    assert body["latest_telemetry"] == sent.model_dump(mode="json")


def test_get_metrics_returns_counter_totals_and_known_assets(app_env: None) -> None:
    with TestClient(app) as client:
        sockname = client.app.state.transport.get_extra_info("sockname")
        assert sockname is not None

        _send_datagram(encode_packet(_packet()), sockname)
        _wait_until(lambda: client.app.state.metrics.packets_processed_total == 1)

        response = client.get("/metrics")
        metrics = client.app.state.metrics
        known_assets = client.app.state.registry.known_assets

    assert response.status_code == 200
    body = response.json()
    assert body["datagrams_received_total"] == metrics.datagrams_received_total
    assert body["packets_invalid_total"] == metrics.packets_invalid_total
    assert body["packets_enqueued_total"] == metrics.packets_enqueued_total
    assert body["packets_queue_dropped_total"] == metrics.packets_queue_dropped_total
    assert body["packets_processed_total"] == metrics.packets_processed_total
    assert body["sequence_gaps_total"] == metrics.sequence_gaps_total
    assert body["duplicate_packets_total"] == metrics.duplicate_packets_total
    assert body["out_of_order_packets_total"] == metrics.out_of_order_packets_total
    assert body["known_assets"] == known_assets
    assert isinstance(body["packets_per_second"], float)
    assert body["packets_per_second"] >= 0

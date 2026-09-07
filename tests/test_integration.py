import socket
import time
from datetime import datetime

from fastapi.testclient import TestClient

from constellationops.app import app
from constellationops.telemetry import TelemetryPacket, encode_packet


def _packet(**overrides: object) -> TelemetryPacket:
    kwargs: dict[str, object] = {
        "asset_id": "sat-101",
        "sequence_number": 100,
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


def _send_packet_and_wait(
    client: TestClient,
    packet: TelemetryPacket,
    addr: tuple[str, int],
    expected_processed: int,
) -> None:
    _send_datagram(encode_packet(packet), addr)
    _wait_until(
        lambda: client.app.state.metrics.packets_processed_total == expected_processed
    )


def test_full_lifecycle_two_assets_over_udp_reports_state_and_metrics(
    app_env: None,
) -> None:
    sat_101_latest = _packet(
        asset_id="sat-101",
        sequence_number=105,
        temperature_c=18.0,
    )
    sat_202_latest = _packet(
        asset_id="sat-202",
        sequence_number=51,
        temperature_c=25.0,
    )

    with TestClient(app) as client:
        sockname = client.app.state.transport.get_extra_info("sockname")
        assert sockname is not None

        _send_packet_and_wait(
            client, _packet(asset_id="sat-101", sequence_number=100), sockname, 1
        )
        _send_packet_and_wait(
            client, _packet(asset_id="sat-101", sequence_number=101), sockname, 2
        )
        _send_packet_and_wait(client, sat_101_latest, sockname, 3)
        _send_packet_and_wait(client, sat_101_latest, sockname, 4)
        _send_packet_and_wait(
            client,
            _packet(asset_id="sat-101", sequence_number=103, temperature_c=17.0),
            sockname,
            5,
        )

        _send_packet_and_wait(
            client, _packet(asset_id="sat-202", sequence_number=50), sockname, 6
        )
        _send_packet_and_wait(client, sat_202_latest, sockname, 7)
        _send_packet_and_wait(client, sat_202_latest, sockname, 8)

        _send_datagram(b"not a valid packet", sockname)

        sat_101 = client.get("/assets/sat-101")
        sat_202 = client.get("/assets/sat-202")
        metrics = client.get("/metrics")

    assert sat_101.status_code == 200
    sat_101_body = sat_101.json()
    assert sat_101_body["asset_id"] == "sat-101"
    assert sat_101_body["health"] == "ONLINE"
    assert sat_101_body["highest_sequence"] == 105
    assert sat_101_body["accepted_count"] == 3
    assert sat_101_body["forward_gap_event_count"] == 1
    assert sat_101_body["duplicate_count"] == 1
    assert sat_101_body["out_of_order_count"] == 1
    assert sat_101_body["latest_telemetry"] == sat_101_latest.model_dump(mode="json")
    assert sat_101_body["last_seen_age_seconds"] is not None
    assert sat_101_body["last_seen_age_seconds"] >= 0

    assert sat_202.status_code == 200
    sat_202_body = sat_202.json()
    assert sat_202_body["asset_id"] == "sat-202"
    assert sat_202_body["health"] == "ONLINE"
    assert sat_202_body["highest_sequence"] == 51
    assert sat_202_body["accepted_count"] == 2
    assert sat_202_body["forward_gap_event_count"] == 0
    assert sat_202_body["duplicate_count"] == 1
    assert sat_202_body["out_of_order_count"] == 0
    assert sat_202_body["latest_telemetry"] == sat_202_latest.model_dump(mode="json")
    assert sat_202_body["last_seen_age_seconds"] is not None
    assert sat_202_body["last_seen_age_seconds"] >= 0

    assert metrics.status_code == 200
    metrics_body = metrics.json()
    assert metrics_body["datagrams_received_total"] == 9
    assert metrics_body["packets_invalid_total"] == 1
    assert metrics_body["packets_enqueued_total"] == 8
    assert metrics_body["packets_queue_dropped_total"] == 0
    assert metrics_body["packets_processed_total"] == 8
    assert metrics_body["sequence_gaps_total"] == 3
    assert metrics_body["duplicate_packets_total"] == 2
    assert metrics_body["out_of_order_packets_total"] == 1
    assert metrics_body["known_assets"] == 2
    assert isinstance(metrics_body["packets_per_second"], float)
    assert metrics_body["packets_per_second"] >= 0

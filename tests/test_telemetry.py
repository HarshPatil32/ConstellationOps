import json
from datetime import datetime

import pytest

from constellationops.telemetry import (
    InvalidPacketError,
    TelemetryPacket,
    decode_packet,
    encode_packet,
)


def _valid_packet_kwargs(**overrides: object) -> dict[str, object]:
    kwargs: dict[str, object] = {
        "asset_id": "sat-001",
        "sequence_number": 42,
        "sent_at": datetime(2026, 9, 6, 12, 0, 0),
        "temperature_c": 22.5,
        "battery_pct": 87.0,
        "signal_dbm": -72.0,
    }
    kwargs.update(overrides)
    return kwargs


def _valid_packet(**overrides: object) -> TelemetryPacket:
    return TelemetryPacket(**_valid_packet_kwargs(**overrides))


def test_encode_decode_roundtrip() -> None:
    packet = _valid_packet()

    decoded = decode_packet(encode_packet(packet))

    assert decoded == packet


def test_decode_rejects_invalid_utf8() -> None:
    with pytest.raises(InvalidPacketError, match="packet is not valid utf-8"):
        decode_packet(b"\xff\xfe\x00")


def test_decode_rejects_invalid_json() -> None:
    with pytest.raises(InvalidPacketError, match="packet is not valid json"):
        decode_packet(b"{not json")


@pytest.mark.parametrize(
    "raw",
    [
        b'{"asset_id":"sat-001","sequence_number":1,"sent_at":"2026-09-06T12:00:00","temperature_c":NaN,"battery_pct":50.0,"signal_dbm":-70.0}',
        b'{"asset_id":"sat-001","sequence_number":1,"sent_at":"2026-09-06T12:00:00","temperature_c":1.0,"battery_pct":50.0,"signal_dbm":Infinity}',
        b'{"asset_id":"sat-001","sequence_number":1,"sent_at":"2026-09-06T12:00:00","temperature_c":1.0,"battery_pct":50.0,"signal_dbm":-Infinity}',
    ],
)
def test_decode_rejects_non_standard_json_constants(raw: bytes) -> None:
    with pytest.raises(InvalidPacketError, match="packet is not valid json") as exc_info:
        decode_packet(raw)

    assert type(exc_info.value) is InvalidPacketError
    assert exc_info.value.__cause__ is not None


@pytest.mark.parametrize(
    ("payload", "match"),
    [
        ({"sequence_number": 1, "sent_at": "2026-09-06T12:00:00", "temperature_c": 1.0, "battery_pct": 50.0, "signal_dbm": -70.0}, "asset_id"),
        ({"asset_id": "sat-001", "sent_at": "2026-09-06T12:00:00", "temperature_c": 1.0, "battery_pct": 50.0, "signal_dbm": -70.0}, "sequence_number"),
        ({"asset_id": "sat-001", "sequence_number": "not-int", "sent_at": "2026-09-06T12:00:00", "temperature_c": 1.0, "battery_pct": 50.0, "signal_dbm": -70.0}, "sequence_number"),
        ({"asset_id": "sat-001", "sequence_number": 1, "sent_at": "2026-09-06T12:00:00", "temperature_c": 1.0, "battery_pct": 50.0, "signal_dbm": -70.0, "extra_field": "bad"}, "extra"),
        ({"asset_id": "sat-001", "sequence_number": -1, "sent_at": "2026-09-06T12:00:00", "temperature_c": 1.0, "battery_pct": 50.0, "signal_dbm": -70.0}, "sequence_number"),
        ({"asset_id": "sat-001", "sequence_number": 1, "sent_at": "2026-09-06T12:00:00", "temperature_c": 1.0, "battery_pct": -0.1, "signal_dbm": -70.0}, "battery_pct"),
        ({"asset_id": "sat-001", "sequence_number": 1, "sent_at": "2026-09-06T12:00:00", "temperature_c": 1.0, "battery_pct": 100.1, "signal_dbm": -70.0}, "battery_pct"),
        ({"asset_id": "", "sequence_number": 1, "sent_at": "2026-09-06T12:00:00", "temperature_c": 1.0, "battery_pct": 50.0, "signal_dbm": -70.0}, "asset_id"),
    ],
)
def test_decode_rejects_invalid_schema(payload: dict[str, object], match: str) -> None:
    with pytest.raises(InvalidPacketError, match="packet failed schema validation") as exc_info:
        decode_packet(json.dumps(payload).encode("utf-8"))

    assert type(exc_info.value) is InvalidPacketError
    assert exc_info.value.__cause__ is not None
    assert match in str(exc_info.value)


def test_decode_rejects_non_object_json() -> None:
    with pytest.raises(InvalidPacketError, match="packet failed schema validation") as exc_info:
        decode_packet(b"[1, 2, 3]")

    assert type(exc_info.value) is InvalidPacketError

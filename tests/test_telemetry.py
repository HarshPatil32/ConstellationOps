import json
import math
from datetime import datetime

import pytest
from pydantic import ValidationError

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


def _valid_payload(**overrides: object) -> dict[str, object]:
    payload: dict[str, object] = {
        "asset_id": "sat-001",
        "sequence_number": 1,
        "sent_at": "2026-09-06T12:00:00",
        "temperature_c": 1.0,
        "battery_pct": 50.0,
        "signal_dbm": -70.0,
    }
    payload.update(overrides)
    return payload


def _payload_missing(field: str) -> dict[str, object]:
    payload = _valid_payload()
    del payload[field]
    return payload


def test_encode_decode_roundtrip() -> None:
    packet = _valid_packet()

    decoded = decode_packet(encode_packet(packet))

    assert decoded == packet


def test_decode_accepts_iso8601_sent_at() -> None:
    decoded = decode_packet(json.dumps(_valid_payload()).encode("utf-8"))

    assert decoded.sent_at == datetime(2026, 9, 6, 12, 0, 0)


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
def test_decode_rejects_non_finite_floats(raw: bytes) -> None:
    with pytest.raises(InvalidPacketError, match="packet failed schema validation"):
        decode_packet(raw)


@pytest.mark.parametrize(
    "sequence_number",
    [True, False, "42"],
)
def test_decode_rejects_invalid_sequence_number_coercion(
    sequence_number: object,
) -> None:
    with pytest.raises(InvalidPacketError, match="packet failed schema validation") as exc_info:
        decode_packet(
            json.dumps(_valid_payload(sequence_number=sequence_number)).encode("utf-8")
        )

    assert "sequence_number" in str(exc_info.value)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("temperature_c", math.nan),
        ("temperature_c", math.inf),
        ("temperature_c", -math.inf),
        ("battery_pct", math.nan),
        ("signal_dbm", math.inf),
    ],
)
def test_telemetry_packet_rejects_non_finite_floats(field: str, value: float) -> None:
    with pytest.raises(ValidationError):
        _valid_packet(**{field: value})


@pytest.mark.parametrize(
    ("payload", "match"),
    [
        (_payload_missing("asset_id"), "asset_id"),
        (_payload_missing("sequence_number"), "sequence_number"),
        (_payload_missing("sent_at"), "sent_at"),
        (_payload_missing("temperature_c"), "temperature_c"),
        (_payload_missing("battery_pct"), "battery_pct"),
        (_payload_missing("signal_dbm"), "signal_dbm"),
        (_valid_payload(sequence_number="not-int"), "sequence_number"),
        (_valid_payload(asset_id=123), "asset_id"),
        (_valid_payload(extra_field="bad"), "extra"),
        (_valid_payload(sequence_number=-1), "sequence_number"),
        (_valid_payload(battery_pct=-0.1), "battery_pct"),
        (_valid_payload(battery_pct=100.1), "battery_pct"),
        (_valid_payload(asset_id=""), "asset_id"),
    ],
)
def test_decode_rejects_invalid_schema(payload: dict[str, object], match: str) -> None:
    with pytest.raises(InvalidPacketError, match="packet failed schema validation") as exc_info:
        decode_packet(json.dumps(payload).encode("utf-8"))

    assert match in str(exc_info.value)


def test_decode_rejects_non_object_json() -> None:
    with pytest.raises(InvalidPacketError, match="packet failed schema validation"):
        decode_packet(b"[1, 2, 3]")

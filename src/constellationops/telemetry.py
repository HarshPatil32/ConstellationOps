import json
from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field, ValidationError


class InvalidPacketError(Exception):
    """Raised when any of the three stages in decode_packet fail."""


class TelemetryPacket(BaseModel):
    model_config = ConfigDict(extra="forbid", allow_inf_nan=False, frozen=True)

    asset_id: str = Field(min_length=1)
    sequence_number: int = Field(ge=0, strict=True)
    sent_at: datetime
    temperature_c: float
    battery_pct: float = Field(ge=0, le=100)
    signal_dbm: float


def decode_packet(raw: bytes) -> TelemetryPacket:
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise InvalidPacketError(f"packet is not valid utf-8: {exc}") from exc

    try:
        data = json.loads(text)
    except json.JSONDecodeError as exc:
        raise InvalidPacketError(f"packet is not valid json: {exc}") from exc

    try:
        return TelemetryPacket.model_validate(data)
    except ValidationError as exc:
        raise InvalidPacketError(f"packet failed schema validation: {exc}") from exc


def encode_packet(packet: TelemetryPacket) -> bytes:
    return packet.model_dump_json().encode("utf-8")

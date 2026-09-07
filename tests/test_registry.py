from datetime import datetime

import pytest

from constellationops.asset_state import AssetState
from constellationops.registry import AssetRegistry
from constellationops.telemetry import TelemetryPacket


def _registry(**overrides: object) -> AssetRegistry:
    kwargs: dict[str, object] = {
        "recent_sequence_capacity": 50,
    }
    kwargs.update(overrides)
    return AssetRegistry(**kwargs)


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


def test_get_or_create_creates_new_asset_state() -> None:
    registry = _registry(recent_sequence_capacity=50)

    state = registry.get_or_create("sat-001")

    assert isinstance(state, AssetState)
    assert state.asset_id == "sat-001"
    assert state.recent_sequence_capacity == 50


def test_get_or_create_returns_same_instance_on_repeat_calls() -> None:
    registry = _registry()

    first = registry.get_or_create("sat-001")
    second = registry.get_or_create("sat-001")

    assert first is second


def test_get_or_create_tracks_multiple_assets_independently() -> None:
    registry = _registry()

    state_a = registry.get_or_create("sat-001")
    state_b = registry.get_or_create("sat-002")

    state_a.observe(_packet(asset_id="sat-001", sequence_number=1), now=100.0)

    assert state_a.accepted_count == 1
    assert state_b.accepted_count == 0


def test_get_returns_none_for_unknown_asset() -> None:
    registry = _registry()

    assert registry.get("missing") is None


def test_get_returns_existing_after_get_or_create() -> None:
    registry = _registry()

    created = registry.get_or_create("sat-001")

    assert registry.get("sat-001") is created


def test_all_returns_empty_list_when_no_assets() -> None:
    registry = _registry()

    assert registry.all() == []


def test_all_returns_all_created_assets() -> None:
    registry = _registry()

    state_a = registry.get_or_create("sat-001")
    state_b = registry.get_or_create("sat-002")
    state_c = registry.get_or_create("sat-003")

    all_states = registry.all()
    assert len(all_states) == 3
    for expected in (state_a, state_b, state_c):
        assert any(state is expected for state in all_states)


def test_known_assets_reflects_count() -> None:
    registry = _registry()

    assert registry.known_assets == 0

    registry.get_or_create("sat-001")
    assert registry.known_assets == 1

    registry.get_or_create("sat-001")
    assert registry.known_assets == 1

    registry.get_or_create("sat-002")
    assert registry.known_assets == 2

    assert registry.get("sat-001") is not None
    assert registry.known_assets == 2


@pytest.mark.parametrize("recent_sequence_capacity", [0, -1])
def test_registry_rejects_invalid_recent_sequence_capacity(
    recent_sequence_capacity: int,
) -> None:
    with pytest.raises(ValueError, match="recent_sequence_capacity must be at least 1"):
        AssetRegistry(recent_sequence_capacity=recent_sequence_capacity)


def test_registry_instances_do_not_share_state() -> None:
    registry_a = _registry()
    registry_b = _registry()

    state_a = registry_a.get_or_create("sat-001")
    state_b = registry_b.get_or_create("sat-001")

    assert registry_a.known_assets == 1
    assert registry_b.known_assets == 1
    assert registry_a.get("sat-001") is state_a
    assert registry_b.get("sat-001") is state_b
    assert state_a is not state_b

from datetime import datetime

import pytest
from pydantic import ValidationError

from constellationops.asset_state import AssetState, SequenceClass
from constellationops.config import Settings
from constellationops.health import HealthState
from constellationops.telemetry import TelemetryPacket


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


def _asset_state(**overrides: object) -> AssetState:
    kwargs: dict[str, object] = {
        "asset_id": "sat-001",
        "recent_sequence_capacity": 50,
    }
    kwargs.update(overrides)
    return AssetState(**kwargs)


def test_sequence_class_members() -> None:
    assert set(SequenceClass) == {
        SequenceClass.FIRST,
        SequenceClass.NORMAL,
        SequenceClass.FORWARD_GAP,
        SequenceClass.DUPLICATE,
        SequenceClass.OUT_OF_ORDER,
    }


def _settings(**overrides: object) -> Settings:
    kwargs: dict[str, object] = {
        "udp_host": "0.0.0.0",
        "udp_port": 9999,
        "queue_capacity": 1000,
        "recent_sequence_history": 50,
        "stale_after_seconds": 5,
        "offline_after_seconds": 15,
        "health_check_interval_seconds": 5,
    }
    kwargs.update(overrides)
    return Settings(**kwargs)


def test_asset_state_defaults() -> None:
    state = _asset_state()

    assert state.asset_id == "sat-001"
    assert state.recent_sequence_capacity == 50
    assert state.highest_sequence is None
    assert len(state.recent_sequence_order) == 0
    assert len(state.recent_sequence_set) == 0
    assert state.accepted_count == 0
    assert state.forward_gap_event_count == 0
    assert state.duplicate_count == 0
    assert state.out_of_order_count == 0
    assert state.latest_telemetry is None
    assert state.last_seen_monotonic is None
    assert state.last_progress_monotonic is None
    assert state.health is HealthState.ONLINE


@pytest.mark.parametrize("recent_sequence_capacity", [0, -1])
def test_asset_state_rejects_invalid_capacity(recent_sequence_capacity: int) -> None:
    with pytest.raises(ValueError, match="recent_sequence_capacity must be at least 1"):
        _asset_state(recent_sequence_capacity=recent_sequence_capacity)


def test_asset_states_do_not_share_mutable_defaults() -> None:
    first = _asset_state(asset_id="sat-001")
    second = _asset_state(asset_id="sat-002")

    first.observe(_packet(sequence_number=1), 100.0)

    assert list(first.recent_sequence_order) == [1]
    assert first.recent_sequence_set == {1}
    assert len(second.recent_sequence_order) == 0
    assert len(second.recent_sequence_set) == 0


def test_observe_first_packet_is_classified_first_and_sets_state() -> None:
    state = _asset_state()
    packet = _packet(sequence_number=10)
    now = 100.0

    seq_class, gap_size = state.observe(packet, now)

    assert seq_class is SequenceClass.FIRST
    assert gap_size == 0
    assert state.highest_sequence == 10
    assert state.latest_telemetry is packet
    assert state.last_seen_monotonic == now
    assert state.last_progress_monotonic == now
    assert state.accepted_count == 1
    assert state.forward_gap_event_count == 0
    assert state.duplicate_count == 0
    assert state.out_of_order_count == 0
    assert 10 in state.recent_sequence_set


def test_observe_normal_packet_advances_state() -> None:
    state = _asset_state()
    state.observe(_packet(sequence_number=10), 100.0)

    packet = _packet(sequence_number=11)
    seq_class, gap_size = state.observe(packet, 101.0)

    assert seq_class is SequenceClass.NORMAL
    assert gap_size == 0
    assert state.highest_sequence == 11
    assert state.latest_telemetry is packet
    assert state.last_progress_monotonic == 101.0
    assert state.accepted_count == 2
    assert state.forward_gap_event_count == 0


@pytest.mark.parametrize(
    ("first_sequence", "second_sequence", "expected_gap"),
    [
        (10, 15, 4),
        (7, 10, 2),
    ],
)
def test_observe_forward_gap_returns_correct_gap_size_and_advances_state(
    first_sequence: int, second_sequence: int, expected_gap: int
) -> None:
    state = _asset_state()
    state.observe(_packet(sequence_number=first_sequence), 100.0)

    packet = _packet(sequence_number=second_sequence)
    seq_class, gap_size = state.observe(packet, 102.0)

    assert seq_class is SequenceClass.FORWARD_GAP
    assert gap_size == expected_gap
    assert state.highest_sequence == second_sequence
    assert state.latest_telemetry is packet
    assert state.last_progress_monotonic == 102.0
    assert state.accepted_count == 2
    assert state.forward_gap_event_count == 1
    assert second_sequence in state.recent_sequence_set


def test_observe_duplicate_for_recently_seen_sequence_does_not_advance_state() -> None:
    state = _asset_state()
    state.observe(_packet(sequence_number=10), 100.0)
    latest = _packet(sequence_number=11)
    state.observe(latest, 101.0)

    packet = _packet(sequence_number=10)
    seq_class, gap_size = state.observe(packet, 102.0)

    assert seq_class is SequenceClass.DUPLICATE
    assert gap_size == 0
    assert state.highest_sequence == 11
    assert state.latest_telemetry is latest
    assert state.last_progress_monotonic == 101.0
    assert state.last_seen_monotonic == 102.0
    assert state.accepted_count == 2
    assert state.duplicate_count == 1


def test_observe_out_of_order_for_old_unseen_sequence() -> None:
    state = _asset_state()
    state.observe(_packet(sequence_number=10), 100.0)
    latest = _packet(sequence_number=15)
    state.observe(latest, 101.0)

    packet = _packet(sequence_number=12)
    seq_class, gap_size = state.observe(packet, 102.0)

    assert seq_class is SequenceClass.OUT_OF_ORDER
    assert gap_size == 0
    assert state.highest_sequence == 15
    assert state.latest_telemetry is latest
    assert state.last_progress_monotonic == 101.0
    assert state.out_of_order_count == 1
    assert 12 in state.recent_sequence_set


def test_observe_repeated_out_of_order_becomes_duplicate() -> None:
    state = _asset_state()
    state.observe(_packet(sequence_number=10), 100.0)
    state.observe(_packet(sequence_number=15), 101.0)
    state.observe(_packet(sequence_number=12), 102.0)

    seq_class, gap_size = state.observe(_packet(sequence_number=12), 103.0)

    assert seq_class is SequenceClass.DUPLICATE
    assert gap_size == 0
    assert state.out_of_order_count == 1
    assert state.duplicate_count == 1


def test_observe_sequence_equal_to_highest_is_duplicate() -> None:
    state = _asset_state()
    state.observe(_packet(sequence_number=10), 100.0)

    seq_class, gap_size = state.observe(_packet(sequence_number=10), 101.0)

    assert seq_class is SequenceClass.DUPLICATE
    assert gap_size == 0
    assert state.highest_sequence == 10
    assert state.duplicate_count == 1


@pytest.mark.parametrize(
    ("sequence_number", "now"),
    [
        (10, 100.0),
        (11, 101.0),
        (15, 102.0),
        (12, 103.0),
        (12, 104.0),
    ],
)
def test_observe_updates_last_seen_monotonic_for_every_class(
    sequence_number: int, now: float
) -> None:
    state = _asset_state()
    state.observe(_packet(sequence_number=10), 100.0)
    state.observe(_packet(sequence_number=11), 101.0)
    state.observe(_packet(sequence_number=15), 102.0)
    state.observe(_packet(sequence_number=12), 103.0)

    state.observe(_packet(sequence_number=sequence_number), now)

    assert state.last_seen_monotonic == now


def test_observe_updates_last_progress_monotonic_only_for_authoritative_classes() -> None:
    state = _asset_state()

    state.observe(_packet(sequence_number=10), 100.0)
    assert state.last_progress_monotonic == 100.0

    state.observe(_packet(sequence_number=11), 101.0)
    assert state.last_progress_monotonic == 101.0

    state.observe(_packet(sequence_number=15), 102.0)
    assert state.last_progress_monotonic == 102.0

    state.observe(_packet(sequence_number=12), 103.0)
    assert state.last_progress_monotonic == 102.0

    state.observe(_packet(sequence_number=12), 104.0)
    assert state.last_progress_monotonic == 102.0


@pytest.mark.parametrize(
    "sequence_number",
    [10, 11, 12, 15],
)
def test_observe_gap_size_is_zero_for_non_forward_gap_classes(
    sequence_number: int,
) -> None:
    state = _asset_state()
    state.observe(_packet(sequence_number=10), 100.0)
    state.observe(_packet(sequence_number=11), 101.0)
    state.observe(_packet(sequence_number=15), 102.0)
    state.observe(_packet(sequence_number=12), 103.0)

    _, gap_size = state.observe(_packet(sequence_number=sequence_number), 104.0)

    assert gap_size == 0


def test_observe_counters_are_consistent_across_classes() -> None:
    state = _asset_state()

    state.observe(_packet(sequence_number=10), 100.0)
    state.observe(_packet(sequence_number=11), 101.0)
    state.observe(_packet(sequence_number=15), 102.0)
    state.observe(_packet(sequence_number=12), 103.0)
    state.observe(_packet(sequence_number=12), 104.0)

    assert state.accepted_count == 3
    assert state.forward_gap_event_count == 1
    assert state.out_of_order_count == 1
    assert state.duplicate_count == 1


def test_observe_returns_tuple_of_sequence_class_and_int() -> None:
    state = _asset_state()

    result = state.observe(_packet(sequence_number=10), 100.0)

    assert isinstance(result, tuple)
    assert len(result) == 2
    assert isinstance(result[0], SequenceClass)
    assert isinstance(result[1], int)


@pytest.mark.parametrize("sequence_number", [0, 500])
def test_observe_first_packet_various_sequence_numbers(sequence_number: int) -> None:
    state = _asset_state()

    seq_class, gap_size = state.observe(_packet(sequence_number=sequence_number), 100.0)

    assert seq_class is SequenceClass.FIRST
    assert gap_size == 0
    assert state.highest_sequence == sequence_number


def test_observe_out_of_order_then_forward_gap() -> None:
    state = _asset_state()
    state.observe(_packet(sequence_number=10), 100.0)
    state.observe(_packet(sequence_number=15), 101.0)
    state.observe(_packet(sequence_number=12), 102.0)

    seq_class, gap_size = state.observe(_packet(sequence_number=20), 103.0)

    assert seq_class is SequenceClass.FORWARD_GAP
    assert gap_size == 4
    assert state.highest_sequence == 20
    assert state.out_of_order_count == 1
    assert state.forward_gap_event_count == 2


def test_observe_eviction_removes_from_both_deque_and_set() -> None:
    state = _asset_state(recent_sequence_capacity=2)
    state.observe(_packet(sequence_number=10), 100.0)
    state.observe(_packet(sequence_number=11), 101.0)
    state.observe(_packet(sequence_number=12), 102.0)

    seq_class, gap_size = state.observe(_packet(sequence_number=10), 103.0)

    assert seq_class is SequenceClass.OUT_OF_ORDER
    assert gap_size == 0
    assert list(state.recent_sequence_order) == [12, 10]
    assert state.recent_sequence_set == {12, 10}
    assert 11 not in state.recent_sequence_set


def test_observe_highest_sequence_evicted_from_history_is_duplicate() -> None:
    state = _asset_state(recent_sequence_capacity=1)
    state.observe(_packet(sequence_number=10), 100.0)
    state.observe(_packet(sequence_number=15), 101.0)
    state.observe(_packet(sequence_number=12), 102.0)

    assert 15 not in state.recent_sequence_set
    assert state.highest_sequence == 15

    seq_class, gap_size = state.observe(_packet(sequence_number=15), 103.0)

    assert seq_class is SequenceClass.DUPLICATE
    assert gap_size == 0
    assert state.highest_sequence == 15
    assert state.duplicate_count == 1
    assert state.out_of_order_count == 1


def test_forward_gap_event_count_is_events_not_estimated_missing_sum() -> None:
    state = _asset_state()
    state.observe(_packet(sequence_number=1), 100.0)

    seq_class, gap_size = state.observe(_packet(sequence_number=1001), 101.0)

    assert seq_class is SequenceClass.FORWARD_GAP
    assert gap_size == 999
    assert state.forward_gap_event_count == 1


def test_filling_gaps_later_does_not_change_forward_gap_event_count() -> None:
    state = _asset_state()
    state.observe(_packet(sequence_number=10), 100.0)
    state.observe(_packet(sequence_number=15), 101.0)

    assert state.forward_gap_event_count == 1

    for sequence_number in range(11, 15):
        state.observe(_packet(sequence_number=sequence_number), 102.0)

    assert state.forward_gap_event_count == 1
    assert state.out_of_order_count == 4


def test_latest_telemetry_is_not_mutable_after_observe() -> None:
    state = _asset_state()
    packet = _packet(sequence_number=10, temperature_c=22.5)
    state.observe(packet, 100.0)

    with pytest.raises(ValidationError):
        packet.temperature_c = 999.0

    assert state.latest_telemetry is not None
    assert state.latest_telemetry.temperature_c == 22.5


def test_refresh_health_no_op_when_never_observed() -> None:
    state = _asset_state()
    settings = _settings()

    result = state.refresh_health(100.0, settings)

    assert result is HealthState.ONLINE
    assert state.health is HealthState.ONLINE


@pytest.mark.parametrize(
    ("now", "expected"),
    [
        (104.999, HealthState.ONLINE),
        (105.0, HealthState.STALE),
        (114.999, HealthState.STALE),
        (115.0, HealthState.OFFLINE),
    ],
)
def test_refresh_health_boundary_values(now: float, expected: HealthState) -> None:
    state = _asset_state()
    state.observe(_packet(sequence_number=1), 100.0)
    settings = _settings()

    result = state.refresh_health(now, settings)

    assert result is expected
    assert state.health is expected


def test_refresh_health_uses_last_seen_not_last_progress() -> None:
    state = _asset_state()
    state.observe(_packet(sequence_number=10), 100.0)
    state.observe(_packet(sequence_number=10), 110.0)
    settings = _settings()

    result = state.refresh_health(111.0, settings)

    assert result is HealthState.ONLINE
    assert state.health is HealthState.ONLINE
    assert state.last_seen_monotonic == 110.0
    assert state.last_progress_monotonic == 100.0


def test_refresh_health_is_idempotent_without_new_observation() -> None:
    state = _asset_state()
    state.observe(_packet(sequence_number=1), 100.0)
    settings = _settings()

    first = state.refresh_health(110.0, settings)
    second = state.refresh_health(110.0, settings)

    assert first is HealthState.STALE
    assert second is HealthState.STALE
    assert state.health is HealthState.STALE


@pytest.mark.parametrize(
    ("before_now", "before_expected", "resume_now", "refresh_now"),
    [
        (110.0, HealthState.STALE, 110.0, 111.0),
        (115.0, HealthState.OFFLINE, 115.0, 116.0),
    ],
)
def test_refresh_health_returns_to_online_after_new_packet(
    before_now: float,
    before_expected: HealthState,
    resume_now: float,
    refresh_now: float,
) -> None:
    state = _asset_state()
    state.observe(_packet(sequence_number=1), 100.0)
    settings = _settings()

    assert state.refresh_health(before_now, settings) is before_expected
    state.observe(_packet(sequence_number=1), resume_now)

    result = state.refresh_health(refresh_now, settings)

    assert result is HealthState.ONLINE
    assert state.health is HealthState.ONLINE

from datetime import datetime

import pytest

from constellationops.asset_state import AssetHealth, AssetState, SequenceClass
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


def test_asset_health_members() -> None:
    assert set(AssetHealth) == {
        AssetHealth.ONLINE,
        AssetHealth.STALE,
        AssetHealth.OFFLINE,
    }


def test_asset_state_defaults() -> None:
    state = _asset_state()

    assert state.asset_id == "sat-001"
    assert state.recent_sequence_capacity == 50
    assert state.highest_sequence is None
    assert len(state.recent_sequence_order) == 0
    assert len(state.recent_sequence_set) == 0
    assert state.accepted_count == 0
    assert state.gap_count == 0
    assert state.duplicate_count == 0
    assert state.out_of_order_count == 0
    assert state.latest_telemetry is None
    assert state.last_seen_monotonic is None
    assert state.last_progress_monotonic is None
    assert state.health is AssetHealth.ONLINE


@pytest.mark.parametrize("recent_sequence_capacity", [0, -1])
def test_asset_state_rejects_invalid_capacity(recent_sequence_capacity: int) -> None:
    with pytest.raises(ValueError, match="recent_sequence_capacity must be at least 1"):
        _asset_state(recent_sequence_capacity=recent_sequence_capacity)


def test_record_sequence_adds_to_deque_and_set() -> None:
    state = _asset_state(recent_sequence_capacity=3)

    state.record_sequence(1)

    assert list(state.recent_sequence_order) == [1]
    assert state.recent_sequence_set == {1}


def test_record_sequence_evicts_on_every_insert_when_capacity_is_one() -> None:
    state = _asset_state(recent_sequence_capacity=1)

    state.record_sequence(1)
    assert list(state.recent_sequence_order) == [1]
    assert state.recent_sequence_set == {1}

    state.record_sequence(2)
    assert list(state.recent_sequence_order) == [2]
    assert state.recent_sequence_set == {2}
    assert 1 not in state.recent_sequence_set


def test_record_sequence_evicts_oldest_when_over_capacity() -> None:
    state = _asset_state(recent_sequence_capacity=3)

    for sequence_number in range(1, 11):
        state.record_sequence(sequence_number)

    assert list(state.recent_sequence_order) == [8, 9, 10]
    assert state.recent_sequence_set == {8, 9, 10}
    assert len(state.recent_sequence_order) == len(state.recent_sequence_set)


def test_record_sequence_membership_reflects_current_window() -> None:
    state = _asset_state(recent_sequence_capacity=3)

    for sequence_number in range(1, 11):
        state.record_sequence(sequence_number)

    for sequence_number in range(1, 8):
        assert sequence_number not in state.recent_sequence_set

    for sequence_number in range(8, 11):
        assert sequence_number in state.recent_sequence_set


def test_record_sequence_reinsert_is_no_op() -> None:
    state = _asset_state(recent_sequence_capacity=3)

    state.record_sequence(1)
    state.record_sequence(2)
    state.record_sequence(3)
    state.record_sequence(2)

    assert list(state.recent_sequence_order) == [1, 2, 3]
    assert state.recent_sequence_set == {1, 2, 3}


def test_asset_states_do_not_share_mutable_defaults() -> None:
    first = _asset_state(asset_id="sat-001")
    second = _asset_state(asset_id="sat-002")

    first.record_sequence(1)

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
    assert state.gap_count == 0
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
    assert state.gap_count == 0


def test_observe_forward_gap_returns_correct_gap_size_and_advances_state() -> None:
    state = _asset_state()
    state.observe(_packet(sequence_number=10), 100.0)

    packet = _packet(sequence_number=15)
    seq_class, gap_size = state.observe(packet, 102.0)

    assert seq_class is SequenceClass.FORWARD_GAP
    assert gap_size == 4
    assert state.highest_sequence == 15
    assert state.latest_telemetry is packet
    assert state.last_progress_monotonic == 102.0
    assert state.accepted_count == 2
    assert state.gap_count == 1
    assert 15 in state.recent_sequence_set


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
    assert state.gap_count == 1
    assert state.out_of_order_count == 1
    assert state.duplicate_count == 1


def test_observe_returns_tuple_of_sequence_class_and_int() -> None:
    state = _asset_state()

    result = state.observe(_packet(sequence_number=10), 100.0)

    assert isinstance(result, tuple)
    assert len(result) == 2
    assert isinstance(result[0], SequenceClass)
    assert isinstance(result[1], int)


def test_observe_first_packet_with_sequence_zero() -> None:
    state = _asset_state()

    seq_class, gap_size = state.observe(_packet(sequence_number=0), 100.0)

    assert seq_class is SequenceClass.FIRST
    assert gap_size == 0
    assert state.highest_sequence == 0


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
    assert state.gap_count == 2


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

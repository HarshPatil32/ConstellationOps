import pytest

from constellationops.asset_state import AssetHealth, AssetState, SequenceClass


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

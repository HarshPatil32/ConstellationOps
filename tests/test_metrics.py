import pytest

from constellationops.metrics import Metrics

_COUNTER_FIELDS = (
    "datagrams_received_total",
    "packets_invalid_total",
    "packets_enqueued_total",
    "packets_queue_dropped_total",
    "packets_processed_total",
    "sequence_gaps_total",
    "duplicate_packets_total",
    "out_of_order_packets_total",
)

_SNAPSHOT_KEYS = frozenset({*_COUNTER_FIELDS, "known_assets"})


def test_new_metrics_all_zero() -> None:
    metrics = Metrics()

    for field in _COUNTER_FIELDS:
        assert getattr(metrics, field) == 0


def test_increment_default_step() -> None:
    metrics = Metrics()

    metrics.increment("packets_processed_total")

    assert metrics.packets_processed_total == 1
    assert metrics.datagrams_received_total == 0


def test_increment_with_n() -> None:
    metrics = Metrics()

    metrics.increment("datagrams_received_total", n=5)

    assert metrics.datagrams_received_total == 5


def test_increment_accumulates() -> None:
    metrics = Metrics()

    metrics.increment("packets_enqueued_total")
    metrics.increment("packets_enqueued_total", n=4)

    assert metrics.packets_enqueued_total == 5


@pytest.mark.parametrize(
    "name",
    [
        "packets_invalid",
        "",
        "packets_processed_total ",
        "known_assets",
    ],
)
def test_increment_rejects_unknown_name(name: str) -> None:
    metrics = Metrics()

    with pytest.raises(ValueError, match=f"unknown counter: {name!r}"):
        metrics.increment(name)

    for field in _COUNTER_FIELDS:
        assert getattr(metrics, field) == 0


@pytest.mark.parametrize("n", [0, -1])
def test_increment_rejects_non_positive_n(n: int) -> None:
    metrics = Metrics()

    with pytest.raises(ValueError, match="n must be at least 1"):
        metrics.increment("packets_processed_total", n=n)

    assert metrics.packets_processed_total == 0


def test_increment_rejects_unknown_name_before_invalid_n() -> None:
    metrics = Metrics()

    with pytest.raises(ValueError, match="unknown counter: 'bogus'"):
        metrics.increment("bogus", n=0)


def test_snapshot_shape() -> None:
    metrics = Metrics()
    metrics.increment("datagrams_received_total", n=10)
    metrics.increment("packets_invalid_total", n=2)
    metrics.increment("packets_processed_total", n=7)

    result = metrics.snapshot(known_assets=3)

    assert set(result) == _SNAPSHOT_KEYS
    assert result["datagrams_received_total"] == 10
    assert result["packets_invalid_total"] == 2
    assert result["packets_processed_total"] == 7
    assert result["packets_enqueued_total"] == 0


@pytest.mark.parametrize("known_assets", [0, 1, 42])
def test_snapshot_reflects_known_assets(known_assets: int) -> None:
    metrics = Metrics()

    result = metrics.snapshot(known_assets=known_assets)

    assert result["known_assets"] == known_assets

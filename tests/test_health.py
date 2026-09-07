import pytest

from constellationops.config import Settings
from constellationops.health import HealthState, derive_health


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


def test_health_state_members() -> None:
    assert set(HealthState) == {
        HealthState.ONLINE,
        HealthState.STALE,
        HealthState.OFFLINE,
    }


@pytest.mark.parametrize(
    ("age_seconds", "expected"),
    [
        (4.999, HealthState.ONLINE),
        (5.0, HealthState.STALE),
        (14.999, HealthState.STALE),
        (15.0, HealthState.OFFLINE),
    ],
)
def test_derive_health_boundary_values(age_seconds: float, expected: HealthState) -> None:
    assert derive_health(age_seconds, _settings()) is expected


def test_derive_health_zero_age_is_online() -> None:
    assert derive_health(0.0, _settings()) is HealthState.ONLINE


def test_derive_health_negative_age_is_online() -> None:
    assert derive_health(-1.0, _settings()) is HealthState.ONLINE


def test_derive_health_custom_thresholds() -> None:
    settings = _settings(stale_after_seconds=1, offline_after_seconds=2)

    assert derive_health(0.999, settings) is HealthState.ONLINE
    assert derive_health(1.0, settings) is HealthState.STALE
    assert derive_health(1.999, settings) is HealthState.STALE
    assert derive_health(2.0, settings) is HealthState.OFFLINE

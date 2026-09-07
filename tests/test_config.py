import pytest

from constellationops.config import Settings

_ENV_VARS = (
    "UDP_HOST",
    "UDP_PORT",
    "QUEUE_CAPACITY",
    "RECENT_SEQUENCE_HISTORY",
    "STALE_AFTER_SECONDS",
    "OFFLINE_AFTER_SECONDS",
    "HEALTH_CHECK_INTERVAL_SECONDS",
)

_DEFAULT_SETTINGS = Settings(
    udp_host="0.0.0.0",
    udp_port=9999,
    queue_capacity=1000,
    recent_sequence_history=50,
    stale_after_seconds=5,
    offline_after_seconds=15,
    health_check_interval_seconds=5,
)


def _valid_settings_kwargs(**overrides: object) -> dict[str, object]:
    kwargs: dict[str, object] = {
        "udp_host": _DEFAULT_SETTINGS.udp_host,
        "udp_port": _DEFAULT_SETTINGS.udp_port,
        "queue_capacity": _DEFAULT_SETTINGS.queue_capacity,
        "recent_sequence_history": _DEFAULT_SETTINGS.recent_sequence_history,
        "stale_after_seconds": _DEFAULT_SETTINGS.stale_after_seconds,
        "offline_after_seconds": _DEFAULT_SETTINGS.offline_after_seconds,
        "health_check_interval_seconds": _DEFAULT_SETTINGS.health_check_interval_seconds,
    }
    kwargs.update(overrides)
    return kwargs


@pytest.fixture
def clear_config_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in _ENV_VARS:
        monkeypatch.delenv(name, raising=False)


def test_from_env_uses_defaults(clear_config_env: None) -> None:
    settings = Settings.from_env()

    assert settings == _DEFAULT_SETTINGS


def test_from_env_applies_overrides(clear_config_env: None, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("UDP_HOST", "127.0.0.1")
    monkeypatch.setenv("UDP_PORT", "8888")
    monkeypatch.setenv("QUEUE_CAPACITY", "500")
    monkeypatch.setenv("RECENT_SEQUENCE_HISTORY", "25")
    monkeypatch.setenv("STALE_AFTER_SECONDS", "3")
    monkeypatch.setenv("OFFLINE_AFTER_SECONDS", "10")
    monkeypatch.setenv("HEALTH_CHECK_INTERVAL_SECONDS", "2")

    settings = Settings.from_env()

    assert settings.udp_host == "127.0.0.1"
    assert settings.udp_port == 8888
    assert settings.queue_capacity == 500
    assert settings.recent_sequence_history == 25
    assert settings.stale_after_seconds == 3
    assert settings.offline_after_seconds == 10
    assert settings.health_check_interval_seconds == 2


@pytest.mark.parametrize(
    ("stale_after_seconds", "offline_after_seconds"),
    [(15, 15), (20, 15)],
)
def test_settings_rejects_invalid_thresholds_direct(
    stale_after_seconds: int,
    offline_after_seconds: int,
) -> None:
    with pytest.raises(ValueError, match="stale_after_seconds must be less than offline_after_seconds"):
        Settings(
            **_valid_settings_kwargs(
                stale_after_seconds=stale_after_seconds,
                offline_after_seconds=offline_after_seconds,
            )
        )


@pytest.mark.parametrize(
    ("field", "value", "match"),
    [
        ("udp_port", 0, "udp_port must be between 1 and 65535"),
        ("udp_port", 65536, "udp_port must be between 1 and 65535"),
        ("queue_capacity", 0, "queue_capacity must be at least 1"),
        ("recent_sequence_history", 0, "recent_sequence_history must be at least 1"),
        ("stale_after_seconds", 0, "stale_after_seconds must be at least 1"),
        ("offline_after_seconds", 0, "offline_after_seconds must be at least 1"),
        ("health_check_interval_seconds", 0, "health_check_interval_seconds must be at least 1"),
        ("stale_after_seconds", -1, "stale_after_seconds must be at least 1"),
        ("offline_after_seconds", -1, "offline_after_seconds must be at least 1"),
        ("health_check_interval_seconds", -1, "health_check_interval_seconds must be at least 1"),
    ],
)
def test_settings_rejects_invalid_bounds(
    field: str,
    value: int,
    match: str,
) -> None:
    with pytest.raises(ValueError, match=match):
        Settings(**_valid_settings_kwargs(**{field: value}))


def test_from_env_rejects_invalid_bounds(
    clear_config_env: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("QUEUE_CAPACITY", "0")

    with pytest.raises(ValueError, match="queue_capacity must be at least 1"):
        Settings.from_env()


def test_from_env_rejects_invalid_time_fields(
    clear_config_env: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("HEALTH_CHECK_INTERVAL_SECONDS", "0")

    with pytest.raises(ValueError, match="health_check_interval_seconds must be at least 1"):
        Settings.from_env()


def test_from_env_rejects_invalid_thresholds(
    clear_config_env: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("STALE_AFTER_SECONDS", "15")
    monkeypatch.setenv("OFFLINE_AFTER_SECONDS", "10")

    with pytest.raises(ValueError, match="stale_after_seconds must be less than offline_after_seconds"):
        Settings.from_env()


def test_from_env_rejects_malformed_integer(
    clear_config_env: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("UDP_PORT", "not-a-number")

    with pytest.raises(ValueError, match="invalid integer for UDP_PORT: 'not-a-number'"):
        Settings.from_env()

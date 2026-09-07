import asyncio
import logging
import time

import pytest

from constellationops.asset_state import AssetState
from constellationops.config import Settings
from constellationops.health import HealthState, derive_health, run_health_monitor


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


def _asset_state(**overrides: object) -> AssetState:
    kwargs: dict[str, object] = {
        "asset_id": "sat-001",
        "recent_sequence_capacity": 50,
    }
    kwargs.update(overrides)
    return AssetState(**kwargs)


async def _wait_for_stale(state: AssetState, timeout_seconds: float = 2.0) -> None:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        if state.health is HealthState.STALE:
            return
        await asyncio.sleep(0.01)
    raise AssertionError("expected asset health to become STALE")


async def test_health_monitor_flips_stale_without_packet_processing() -> None:
    settings = _settings(stale_after_seconds=1, offline_after_seconds=1000)
    state = _asset_state(last_seen_monotonic=time.monotonic() - 5)
    assets = {"sat-001": state}
    task = asyncio.create_task(run_health_monitor(assets, settings, interval_seconds=0.01))

    try:
        await _wait_for_stale(state)
        assert state.health is HealthState.STALE
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task


async def test_health_monitor_logs_per_asset_exception_and_continues(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    settings = _settings(stale_after_seconds=1, offline_after_seconds=1000)
    failing = _asset_state(asset_id="sat-bad", last_seen_monotonic=time.monotonic() - 5)
    healthy = _asset_state(asset_id="sat-good", last_seen_monotonic=time.monotonic() - 5)
    assets = {"sat-bad": failing, "sat-good": healthy}

    original_refresh_health = AssetState.refresh_health

    def raise_on_bad(self: AssetState, now: float, settings: Settings) -> HealthState:
        if self.asset_id == "sat-bad":
            raise RuntimeError("refresh failed")
        return original_refresh_health(self, now, settings)

    monkeypatch.setattr(AssetState, "refresh_health", raise_on_bad)
    caplog.set_level(logging.ERROR)

    task = asyncio.create_task(run_health_monitor(assets, settings, interval_seconds=0.01))

    try:
        await _wait_for_stale(healthy)
        assert healthy.health is HealthState.STALE
        assert not task.done()
        assert "sat-bad" in caplog.text
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task


async def test_health_monitor_does_not_swallow_cancelled_error_from_refresh_health(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    settings = _settings()
    state = _asset_state(last_seen_monotonic=time.monotonic() - 5)
    assets = {"sat-001": state}

    def raise_cancelled(self: AssetState, now: float, settings: Settings) -> HealthState:
        raise asyncio.CancelledError()

    monkeypatch.setattr(AssetState, "refresh_health", raise_cancelled)
    caplog.set_level(logging.ERROR)

    task = asyncio.create_task(run_health_monitor(assets, settings, interval_seconds=0.01))

    with pytest.raises(asyncio.CancelledError):
        await task

    assert "sat-001" not in caplog.text


async def test_health_monitor_cancels_cleanly() -> None:
    settings = _settings()
    task = asyncio.create_task(run_health_monitor({}, settings, interval_seconds=0.01))
    await asyncio.sleep(0.05)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

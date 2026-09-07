import asyncio
import logging
import time
from enum import Enum
from typing import TYPE_CHECKING

from constellationops.config import Settings

if TYPE_CHECKING:
    from constellationops.asset_state import AssetState

logger = logging.getLogger(__name__)


class HealthState(str, Enum):
    ONLINE = "ONLINE"
    STALE = "STALE"
    OFFLINE = "OFFLINE"


def derive_health(age_seconds: float, settings: Settings) -> HealthState:
    if age_seconds < settings.stale_after_seconds:
        return HealthState.ONLINE
    if age_seconds < settings.offline_after_seconds:
        return HealthState.STALE
    return HealthState.OFFLINE


async def run_health_monitor(
    assets: dict[str, AssetState],
    settings: Settings,
    interval_seconds: float,
) -> None:
    while True:
        now = time.monotonic()
        for asset_id, state in list(assets.items()):
            try:
                state.refresh_health(now, settings)
            except Exception:
                logger.exception("health check failed for asset %s", asset_id)
        await asyncio.sleep(interval_seconds)

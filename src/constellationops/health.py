from enum import Enum

from constellationops.config import Settings


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

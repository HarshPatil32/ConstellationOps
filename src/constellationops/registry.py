from dataclasses import dataclass, field

from constellationops.asset_state import AssetState


@dataclass
class AssetRegistry:
    recent_sequence_capacity: int
    _assets: dict[str, AssetState] = field(default_factory=dict, init=False, repr=False)

    def __post_init__(self) -> None:
        if self.recent_sequence_capacity < 1:
            raise ValueError("recent_sequence_capacity must be at least 1")

    def get_or_create(self, asset_id: str) -> AssetState:
        state = self._assets.get(asset_id)
        if state is None:
            state = AssetState(
                asset_id=asset_id,
                recent_sequence_capacity=self.recent_sequence_capacity,
            )
            self._assets[asset_id] = state
        return state

    def get(self, asset_id: str) -> AssetState | None:
        return self._assets.get(asset_id)

    def all(self) -> list[AssetState]:
        return list(self._assets.values())

    @property
    def assets(self) -> dict[str, AssetState]:
        return self._assets

    @property
    def known_assets(self) -> int:
        return len(self._assets)

import time

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel

from constellationops.asset_state import AssetState
from constellationops.health import HealthState
from constellationops.telemetry import TelemetryPacket

router = APIRouter()


class HealthResponse(BaseModel):
    status: str
    uptime_seconds: float
    queue_size: int
    queue_capacity: int


class AssetSummary(BaseModel):
    asset_id: str
    health: HealthState
    highest_sequence: int | None
    accepted_count: int
    forward_gap_event_count: int
    duplicate_count: int
    out_of_order_count: int
    last_seen_age_seconds: float | None


class AssetDetail(AssetSummary):
    latest_telemetry: TelemetryPacket | None


class MetricsResponse(BaseModel):
    datagrams_received_total: int
    packets_invalid_total: int
    packets_enqueued_total: int
    packets_queue_dropped_total: int
    packets_processed_total: int
    sequence_gaps_total: int
    duplicate_packets_total: int
    out_of_order_packets_total: int
    known_assets: int
    packets_per_second: float


def _last_seen_age_seconds(state: AssetState) -> float | None:
    if state.last_seen_monotonic is None:
        return None
    return time.monotonic() - state.last_seen_monotonic


def _asset_summary(state: AssetState) -> AssetSummary:
    return AssetSummary(
        asset_id=state.asset_id,
        health=state.health,
        highest_sequence=state.highest_sequence,
        accepted_count=state.accepted_count,
        forward_gap_event_count=state.forward_gap_event_count,
        duplicate_count=state.duplicate_count,
        out_of_order_count=state.out_of_order_count,
        last_seen_age_seconds=_last_seen_age_seconds(state),
    )


def _asset_detail(state: AssetState) -> AssetDetail:
    return AssetDetail(
        **_asset_summary(state).model_dump(),
        latest_telemetry=state.latest_telemetry,
    )


@router.get("/health", response_model=HealthResponse)
def get_health(request: Request) -> HealthResponse:
    queue = request.app.state.queue
    return HealthResponse(
        status="ok",
        uptime_seconds=time.monotonic() - request.app.state.started_monotonic,
        queue_size=queue.qsize(),
        queue_capacity=queue.maxsize,
    )


@router.get("/assets", response_model=list[AssetSummary])
def list_assets(request: Request) -> list[AssetSummary]:
    states = sorted(request.app.state.registry.all(), key=lambda state: state.asset_id)
    return [_asset_summary(state) for state in states]


@router.get("/assets/{asset_id}", response_model=AssetDetail)
def get_asset(asset_id: str, request: Request) -> AssetDetail:
    state = request.app.state.registry.get(asset_id)
    if state is None:
        raise HTTPException(status_code=404, detail=f"unknown asset: {asset_id}")
    return _asset_detail(state)


@router.get("/metrics", response_model=MetricsResponse)
def get_metrics(request: Request) -> MetricsResponse:
    metrics = request.app.state.metrics
    known_assets = request.app.state.registry.known_assets
    return MetricsResponse(**metrics.snapshot(known_assets))

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI

from constellationops.config import Settings
from constellationops.health import run_health_monitor
from constellationops.ingest import TelemetryProtocol
from constellationops.metrics import Metrics
from constellationops.processor import run_telemetry_processor
from constellationops.registry import AssetRegistry
from constellationops.telemetry import TelemetryPacket


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    transport: asyncio.DatagramTransport | None = None
    processor_task: asyncio.Task[None] | None = None
    health_task: asyncio.Task[None] | None = None

    try:
        settings = Settings.from_env()
        metrics = Metrics()
        registry = AssetRegistry(recent_sequence_capacity=settings.recent_sequence_history)
        queue: asyncio.Queue[TelemetryPacket] = asyncio.Queue(maxsize=settings.queue_capacity)

        app.state.settings = settings
        app.state.metrics = metrics
        app.state.registry = registry
        app.state.queue = queue

        loop = asyncio.get_running_loop()
        transport, _protocol = await loop.create_datagram_endpoint(
            lambda: TelemetryProtocol(queue, metrics),
            local_addr=(settings.udp_host, settings.udp_port),
        )
        app.state.transport = transport

        processor_task = asyncio.create_task(
            run_telemetry_processor(queue, registry, metrics)
        )
        health_task = asyncio.create_task(
            run_health_monitor(
                registry.assets,
                settings,
                settings.health_check_interval_seconds,
            )
        )
        app.state.processor_task = processor_task
        app.state.health_task = health_task

        yield
    finally:
        for task in (health_task, processor_task):
            if task is not None:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
        if transport is not None:
            transport.close()


def create_app() -> FastAPI:
    return FastAPI(lifespan=lifespan)


app = create_app()

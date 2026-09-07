import asyncio
import logging
import time
from collections.abc import Callable

from constellationops.asset_state import SequenceClass
from constellationops.metrics import Metrics
from constellationops.registry import AssetRegistry
from constellationops.telemetry import TelemetryPacket

logger = logging.getLogger(__name__)


async def run_telemetry_processor(
    queue: asyncio.Queue[TelemetryPacket],
    registry: AssetRegistry,
    metrics: Metrics,
    clock: Callable[[], float] = time.monotonic,
) -> None:
    while True:
        packet = await queue.get()
        try:
            seq_class, gap_size = registry.get_or_create(packet.asset_id).observe(
                packet, clock()
            )
            metrics.increment("packets_processed_total")
            if seq_class is SequenceClass.FORWARD_GAP:
                metrics.increment("sequence_gaps_total", n=gap_size)
            elif seq_class is SequenceClass.DUPLICATE:
                metrics.increment("duplicate_packets_total")
            elif seq_class is SequenceClass.OUT_OF_ORDER:
                metrics.increment("out_of_order_packets_total")
        except Exception:
            logger.exception("failed to process packet for asset %s", packet.asset_id)
        finally:
            queue.task_done()

import asyncio
import logging

from constellationops.metrics import Metrics
from constellationops.telemetry import InvalidPacketError, TelemetryPacket, decode_packet

logger = logging.getLogger(__name__)


class TelemetryProtocol(asyncio.DatagramProtocol):
    def __init__(self, queue: asyncio.Queue[TelemetryPacket], metrics: Metrics) -> None:
        self.queue = queue
        self.metrics = metrics

    def datagram_received(self, data: bytes, addr: tuple[str, int]) -> None:
        try:
            self.metrics.increment("datagrams_received_total")

            try:
                packet = decode_packet(data)
            except InvalidPacketError:
                self.metrics.increment("packets_invalid_total")
                return

            try:
                self.queue.put_nowait(packet)
            except asyncio.QueueFull:
                self.metrics.increment("packets_queue_dropped_total")
                return

            self.metrics.increment("packets_enqueued_total")
        except Exception:
            logger.exception("unexpected error handling datagram from %s", addr)

    def error_received(self, exc: Exception) -> None:
        logger.error("UDP transport error: %s", exc)

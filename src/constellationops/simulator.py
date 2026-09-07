import argparse
import asyncio
import logging
import random
from dataclasses import dataclass
from datetime import datetime

from constellationops.telemetry import TelemetryPacket, encode_packet

logger = logging.getLogger(__name__)

_DEFAULT_ASSETS = 5
_DEFAULT_RATE = 1.0
_DEFAULT_HOST = "127.0.0.1"
_DEFAULT_PORT = 9999
_DEFAULT_DROP_PROB = 0.0
_DEFAULT_DUP_PROB = 0.0
_DEFAULT_REORDER_PROB = 0.0
_DEFAULT_REORDER_DELAY_MS = 0.0

_TEMPERATURE_C_RANGE = (-20.0, 60.0)
_BATTERY_PCT_RANGE = (0.0, 100.0)
_SIGNAL_DBM_RANGE = (-120.0, -30.0)


@dataclass
class SimulatorStats:
    generated: int = 0
    dropped: int = 0
    duplicated: int = 0
    delayed: int = 0


def _asset_id(index: int) -> str:
    return f"sat-{index:03d}"


def _asset_rng(seed: int | None, index: int) -> random.Random:
    return random.Random(seed + index) if seed is not None else random.Random()


def _generate_telemetry_values(rng: random.Random) -> tuple[float, float, float]:
    temperature_c = rng.uniform(*_TEMPERATURE_C_RANGE)
    battery_pct = rng.uniform(*_BATTERY_PCT_RANGE)
    signal_dbm = rng.uniform(*_SIGNAL_DBM_RANGE)
    return temperature_c, battery_pct, signal_dbm


def _send(transport: asyncio.DatagramTransport, encoded: bytes, duplicate: bool) -> None:
    transport.sendto(encoded)
    if duplicate:
        transport.sendto(encoded)


async def _delayed_send(
    transport: asyncio.DatagramTransport,
    encoded: bytes,
    delay_s: float,
    duplicate: bool,
) -> None:
    await asyncio.sleep(delay_s)
    try:
        _send(transport, encoded, duplicate)
    except Exception:
        logger.exception("delayed send failed")


def _prune_pending(pending: list[asyncio.Task[None]]) -> None:
    pending[:] = [task for task in pending if not task.done()]


async def _run_asset(
    asset_id: str,
    transport: asyncio.DatagramTransport,
    rate_hz: float,
    deadline: float | None,
    rng: random.Random,
    drop_prob: float,
    dup_prob: float,
    reorder_prob: float,
    reorder_delay_ms: float,
) -> SimulatorStats:
    loop = asyncio.get_running_loop()
    sequence_number = 0
    interval = 1.0 / rate_hz
    stats = SimulatorStats()
    pending: list[asyncio.Task[None]] = []
    reorder_delay_s = reorder_delay_ms / 1000.0

    try:
        while deadline is None or loop.time() < deadline:
            try:
                temperature_c, battery_pct, signal_dbm = _generate_telemetry_values(rng)
                packet = TelemetryPacket(
                    asset_id=asset_id,
                    sequence_number=sequence_number,
                    sent_at=datetime.now(),
                    temperature_c=temperature_c,
                    battery_pct=battery_pct,
                    signal_dbm=signal_dbm,
                )
                encoded = encode_packet(packet)
                stats.generated += 1

                drop_roll = rng.random()
                dup_roll = rng.random()
                reorder_roll = rng.random()

                if drop_roll < drop_prob:
                    stats.dropped += 1
                else:
                    duplicate = dup_roll < dup_prob
                    reorder = reorder_roll < reorder_prob

                    if duplicate:
                        stats.duplicated += 1
                    if reorder:
                        stats.delayed += 1
                        _prune_pending(pending)
                        pending.append(
                            asyncio.create_task(
                                _delayed_send(transport, encoded, reorder_delay_s, duplicate)
                            )
                        )
                    else:
                        _send(transport, encoded, duplicate)

                sequence_number += 1
            except Exception:
                logger.exception("asset task failed for %s", asset_id)
                return stats

            if deadline is not None and loop.time() >= deadline:
                break

            await asyncio.sleep(interval)
    except asyncio.CancelledError:
        logger.info("asset task cancelled for %s", asset_id)
    finally:
        if pending:
            results = await asyncio.gather(*pending, return_exceptions=True)
            for result in results:
                if isinstance(result, BaseException):
                    logger.exception("delayed send task failed", exc_info=result)

    return stats


def _aggregate_stats(results: list[SimulatorStats | BaseException]) -> SimulatorStats:
    total = SimulatorStats()
    for result in results:
        if isinstance(result, SimulatorStats):
            total.generated += result.generated
            total.dropped += result.dropped
            total.duplicated += result.duplicated
            total.delayed += result.delayed
    return total


async def run_simulator(
    assets: int,
    rate: float,
    host: str,
    port: int,
    duration: float | None,
    seed: int | None,
    drop_prob: float = _DEFAULT_DROP_PROB,
    dup_prob: float = _DEFAULT_DUP_PROB,
    reorder_prob: float = _DEFAULT_REORDER_PROB,
    reorder_delay_ms: float = _DEFAULT_REORDER_DELAY_MS,
) -> SimulatorStats:
    loop = asyncio.get_running_loop()
    transport, _ = await loop.create_datagram_endpoint(
        asyncio.DatagramProtocol,
        remote_addr=(host, port),
    )

    deadline = loop.time() + duration if duration is not None else None

    try:
        tasks = [
            _run_asset(
                _asset_id(index),
                transport,
                rate,
                deadline,
                _asset_rng(seed, index),
                drop_prob,
                dup_prob,
                reorder_prob,
                reorder_delay_ms,
            )
            for index in range(1, assets + 1)
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for index, result in enumerate(results, start=1):
            if isinstance(result, BaseException):
                logger.exception(
                    "asset task raised for %s",
                    _asset_id(index),
                    exc_info=result,
                )
        return _aggregate_stats(results)
    finally:
        transport.close()


def _format_summary(stats: SimulatorStats) -> str:
    return (
        f"generated={stats.generated} "
        f"dropped={stats.dropped} "
        f"duplicated={stats.duplicated} "
        f"delayed={stats.delayed}"
    )


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="ConstellationOps telemetry simulator")
    parser.add_argument("--assets", type=int, default=_DEFAULT_ASSETS)
    parser.add_argument("--rate", type=float, default=_DEFAULT_RATE)
    parser.add_argument("--host", default=_DEFAULT_HOST)
    parser.add_argument("--port", type=int, default=_DEFAULT_PORT)
    parser.add_argument("--duration", type=float, default=None)
    parser.add_argument("--seed", type=int, default=None)
    parser.add_argument("--drop-prob", type=float, default=_DEFAULT_DROP_PROB)
    parser.add_argument("--dup-prob", type=float, default=_DEFAULT_DUP_PROB)
    parser.add_argument("--reorder-prob", type=float, default=_DEFAULT_REORDER_PROB)
    parser.add_argument(
        "--reorder-delay-ms",
        type=float,
        default=_DEFAULT_REORDER_DELAY_MS,
        help="Delay for reordered packets in milliseconds; run may exceed --duration by this amount",
    )
    return parser


def _validate_args(args: argparse.Namespace, parser: argparse.ArgumentParser) -> None:
    if args.assets < 1:
        parser.error("--assets must be at least 1")
    if args.rate <= 0:
        parser.error("--rate must be greater than 0")
    if not 1 <= args.port <= 65535:
        parser.error("--port must be between 1 and 65535")
    if args.duration is not None and args.duration <= 0:
        parser.error("--duration must be greater than 0")
    for name in ("drop_prob", "dup_prob", "reorder_prob"):
        value = getattr(args, name)
        if not 0.0 <= value <= 1.0:
            parser.error(f"--{name.replace('_', '-')} must be between 0 and 1")
    if args.reorder_delay_ms < 0:
        parser.error("--reorder-delay-ms must be non-negative")
    if args.reorder_prob > 0 and args.reorder_delay_ms <= 0:
        parser.error("--reorder-delay-ms must be greater than 0 when --reorder-prob is greater than 0")


def main(argv: list[str] | None = None) -> None:
    parser = _build_parser()
    args = parser.parse_args(argv)
    _validate_args(args, parser)
    stats = asyncio.run(
        run_simulator(
            assets=args.assets,
            rate=args.rate,
            host=args.host,
            port=args.port,
            duration=args.duration,
            seed=args.seed,
            drop_prob=args.drop_prob,
            dup_prob=args.dup_prob,
            reorder_prob=args.reorder_prob,
            reorder_delay_ms=args.reorder_delay_ms,
        )
    )
    print(_format_summary(stats))


if __name__ == "__main__":
    main()

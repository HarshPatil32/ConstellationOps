import argparse
import asyncio
import logging
import random
from datetime import datetime

from constellationops.telemetry import TelemetryPacket, encode_packet

logger = logging.getLogger(__name__)

_DEFAULT_ASSETS = 5
_DEFAULT_RATE = 1.0
_DEFAULT_HOST = "127.0.0.1"
_DEFAULT_PORT = 9999

_TEMPERATURE_C_RANGE = (-20.0, 60.0)
_BATTERY_PCT_RANGE = (0.0, 100.0)
_SIGNAL_DBM_RANGE = (-120.0, -30.0)


def _asset_id(index: int) -> str:
    return f"sat-{index:03d}"


def _asset_rng(seed: int | None, index: int) -> random.Random:
    return random.Random(seed + index) if seed is not None else random.Random()


def _generate_telemetry_values(rng: random.Random) -> tuple[float, float, float]:
    temperature_c = rng.uniform(*_TEMPERATURE_C_RANGE)
    battery_pct = rng.uniform(*_BATTERY_PCT_RANGE)
    signal_dbm = rng.uniform(*_SIGNAL_DBM_RANGE)
    return temperature_c, battery_pct, signal_dbm


async def _run_asset(
    asset_id: str,
    transport: asyncio.DatagramTransport,
    rate_hz: float,
    deadline: float | None,
    rng: random.Random, 
) -> None:
    loop = asyncio.get_running_loop()
    sequence_number = 0
    interval = 1.0 / rate_hz

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
            transport.sendto(encode_packet(packet))
            sequence_number += 1
        except Exception:
            logger.exception("asset task failed for %s", asset_id)
            return

        if deadline is not None and loop.time() >= deadline:
            return

        await asyncio.sleep(interval)


async def run_simulator(
    assets: int,
    rate: float,
    host: str,
    port: int,
    duration: float | None,
    seed: int | None,
) -> None:
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
            )
            for index in range(1, assets + 1)
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for index, result in enumerate(results, start=1):
            if isinstance(result, Exception):
                logger.exception(
                    "asset task raised for %s",
                    _asset_id(index),
                    exc_info=result,
                )
    finally:
        transport.close()


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="ConstellationOps telemetry simulator")
    parser.add_argument("--assets", type=int, default=_DEFAULT_ASSETS)
    parser.add_argument("--rate", type=float, default=_DEFAULT_RATE)
    parser.add_argument("--host", default=_DEFAULT_HOST)
    parser.add_argument("--port", type=int, default=_DEFAULT_PORT)
    parser.add_argument("--duration", type=float, default=None)
    parser.add_argument("--seed", type=int, default=None)
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


def main(argv: list[str] | None = None) -> None:
    parser = _build_parser()
    args = parser.parse_args(argv)
    _validate_args(args, parser)
    asyncio.run(
        run_simulator(
            assets=args.assets,
            rate=args.rate,
            host=args.host,
            port=args.port,
            duration=args.duration,
            seed=args.seed,
        )
    )


if __name__ == "__main__":
    main()

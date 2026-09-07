# ConstellationOps — Design

## Problem Statement

ConstellationOps is a real-time telemetry ingestion and health-monitoring system for simulated assets that communicate over an unreliable network. The system is designed to model the reliability problems that show up in distributed telemetry systems: lost, duplicated, reordered, delayed, or malformed packets; packets that arrive faster than the consumer can process them; and assets that sometimes stop transmitting entirely.

The P0 system will ingest telemetry concurrently without allowing malformed traffic or one misbehaving asset to destabilize the rest of the process. It must maintain meaningful per-asset state despite UDP's lack of delivery and ordering guarantees. It must distinguish receipt of valid telemetry from forward sequence progress, detect stale and offline assets, expose operational state and reliability counters for inspection, and keep ingestion buffering bounded under overload. The goal is not to reproduce a real spacecraft or production satellite platform, but to build a small, inspectable system that demonstrates the reliability and concurrency principles involved in telemetry ingestion.

## System Invariants

1. **Valid telemetry only enters system state.** Malformed or schema-invalid telemetry must never mutate per-asset state. The UDP boundary can receive arbitrary bytes. Parsing and validation happen before telemetry becomes trusted application data. Invalid packets are rejected and counted rather than entering the processing pipeline.

2. **Ingestion buffering is bounded.** The amount of telemetry waiting to be processed must have a fixed upper bound independent of incoming traffic volume. The receiver must never create an unbounded backlog. A bounded queue establishes the maximum amount of queued work, and overload must result in an explicit, measurable load-shedding decision rather than uncontrolled queue growth. Per-asset duplicate detection uses a bounded recent-sequence history so that structure does not grow without limit as sequences advance. P0 does not cap total in-memory asset count; the guarantee here is bounded ingestion backlog and bounded per-asset sequence history, not a global cap on all process memory.

3. **Overload is explicit and observable.** When the system cannot keep up with incoming telemetry, packets may be dropped intentionally, but those drops must never be silent. UDP means transport-level delivery is not guaranteed. ConstellationOps additionally needs to distinguish intentional application-side load shedding from normal processing. Queue-overflow drops therefore increment an explicit metric.

4. **Asset streams are logically isolated.** A malformed, late, duplicate, or otherwise problematic telemetry stream from one asset must not corrupt the state of another asset. Sequence tracking, health, timestamps, and latest telemetry are maintained independently per `asset_id`. Problems associated with one asset stay associated with that asset.

5. **Sequence state is monotonic.** An asset's accepted sequence progress must never move backward. Each asset maintains a `highest_sequence`. A packet with a sequence greater than that value represents forward progress. Duplicate or late/out-of-order packets may be observed and counted, but they cannot replace the latest forward-progress telemetry or reduce `highest_sequence`. A sequence jump indicates estimated missing telemetry, not confirmed packet loss. Use language such as `sequence_gaps_total` / `estimated_missing_total`; do not claim the missing packets were definitely lost.

6. **Receipt and progress are separate concepts.** Receiving a valid packet and observing forward sequence progress must be tracked independently. `last_seen` updates whenever a valid packet for the asset is received, including duplicates and late packets. `last_progress`, `highest_sequence`, and the latest forward-progress telemetry update only when the sequence advances. An asset repeatedly sending old or duplicate packets is still communicating even though its telemetry stream is not progressing normally.

7. **Health derives from observed communication.** Asset health is determined from elapsed time since the most recent valid packet, using explicit `ONLINE → STALE → OFFLINE` thresholds. Health is based on `last_seen`, not `last_progress`. Duplicate or out-of-order but otherwise valid packets show that the asset is still communicating and refresh health even though they do not advance telemetry state. Disappearing assets become observable without requiring a disconnect signal that UDP cannot provide.

8. **Reliability behavior is observable.** Every important reliability outcome must be inspectable through state or metrics rather than occurring silently. At minimum, the system should make the relevant categories visible: valid/processed telemetry, malformed/rejected packets, duplicates, late/out-of-order packets, sequence gaps/estimated missing telemetry, application load-shedding drops, and asset health/state. The read API exists to expose this state.

## P0 Components

### 1. Asset / Network Simulator

The simulator creates configurable concurrent simulated assets. Each maintains its own sequence counter and emits telemetry over UDP at a configurable rate. Each simulated asset owns a dedicated `random.Random` seeded from the base seed and its stable asset index, rather than sharing one RNG across concurrent tasks, so that generated or injected behavior depends only on seed and asset index—not on asyncio scheduling order—and stays reproducible across runs. It also provides the controlled source of unreliable-network behavior needed to exercise packet loss, reordering, delay, duplication, malformed traffic, disappearing assets, and traffic spikes. The simulator is a verification harness—not a runtime enforcer—that makes the reliability scenarios behind Invariants 4, 5, 6, and 7 reproducible without depending on external systems or real hardware.

Telemetry uses this shape:

- `asset_id`
- `sequence_number`
- `sent_at`
- `temperature_c`
- `battery_pct`
- `signal_dbm`

### 2. UDP Telemetry Receiver

The receiver owns the network boundary. It accepts UDP datagrams, decodes/parses them, validates them against the telemetry schema, rejects malformed input, records the appropriate receive/rejection metrics, and enqueues valid telemetry non-blockingly, dropping on overflow rather than blocking (see Bounded Ingestion Queue below for why). UDP is intentional because the project needs to confront loss, duplication, and reordering at the application layer rather than having the transport hide those behaviors. This component is forced by Invariant 1 and participates directly in Invariants 2, 3, and 8.

### 3. Bounded Ingestion Queue

A fixed-capacity in-process queue separates network receipt from telemetry processing. It absorbs short bursts while imposing a hard upper bound on queued telemetry. When capacity is exhausted, the receiver performs an explicit load-shedding action and records the drop rather than allowing backlog growth to consume arbitrary memory. This component exists directly because of Invariants 2 and 3 and provides observable overload behavior required by Invariant 8.

On overflow, the receiver drops the incoming packet rather than evicting an already-queued one (drop-newest, not drop-oldest). Evicting a queued packet would require the receiver to reach into the queue's internals to remove an arbitrary item, adding complexity for no benefit under P0's uniform, undifferentiated telemetry—there is no priority signal that makes an already-queued packet less valuable than the one just arrived. Drop-newest is also the natural behavior of `asyncio.Queue.put_nowait`, which is the primitive that keeps the receiver non-blocking (see the UDP Telemetry Receiver section above). The receiver never blocks on a full queue: `datagram_received` is a synchronous callback on `asyncio.DatagramProtocol`, not a coroutine, so it cannot `await queue.put()` at all. Even if it could, blocking would stall further datagram handling on this receiver, turning one overloaded moment into delayed processing for every asset and violating both the bounded-buffering guarantee (Invariant 2) and the requirement that overload be explicit and immediate rather than a hidden backlog (Invariant 3).

### 4. Telemetry Processor / Sequence Classifier

A single telemetry-processing path consumes validated packets from the bounded queue and classifies each packet relative to that asset's sequence history as forward progress, duplicate, or late/out-of-order. It detects forward sequence gaps as estimated missing telemetry and updates state according to the classification. Every valid packet refreshes `last_seen`; only forward progress updates `last_progress`, `highest_sequence`, and latest forward-progress telemetry. The processor resolves the target `AssetState` by `packet.asset_id` before calling `observe()`; `AssetState` does not re-validate routing. Receiver-side monotonic time (`time.monotonic()`) is passed into `observe()` as `now`. Keeping mutation serialized in P0 avoids unnecessary locking and race conditions while preserving concurrent network ingestion. This component primarily enforces Invariants 4, 5, and 6 and emits reliability information required by Invariant 8.

For sequence state, the intended model is:

- `highest_sequence`
- bounded `recent_sequences`

That bounded history helps identify duplicates without retaining every sequence number forever.

### 5. Per-Asset State Store

The in-memory asset state model holds independent state for each simulated asset, including `last_seen`, `last_progress`, `highest_sequence`, bounded recent-sequence information, latest forward-progress telemetry, health state, and relevant counters. Every valid packet refreshes `last_seen`; only forward sequence progress refreshes `last_progress`, `highest_sequence`, and latest forward-progress telemetry. State belonging to one asset cannot overwrite or corrupt another asset's state, and late/duplicate packets cannot regress forward-progress state. Per-asset gap observability distinguishes forward-gap **events** (`forward_gap_event_count`) from **estimated missing sequence numbers** on a single observation (`gap_size` returned by `observe()`). This component exists because of Invariants 4, 5, and 6 and provides the state consumed by health monitoring and the read API. This is in-process state, not a database.

### 6. Health Monitor

The health monitor periodically evaluates each known asset using monotonic elapsed time since `last_seen` and explicit thresholds to derive `ONLINE`, `STALE`, or `OFFLINE`. Because UDP provides no connection lifecycle, inactivity must be inferred from time rather than from a disconnect event. Health uses valid communication rather than sequence advancement, so valid duplicate/late packets still refresh `last_seen`. This component is directly required by Invariant 7 and makes asset disappearance observable under Invariant 8.

### 7. Metrics / Read-Only FastAPI Surface

The API provides a read-only inspection surface over current system and per-asset state so that the behavior of the ingestion pipeline can be verified externally. It exposes health/state and reliability counters such as malformed packets, duplicates, late/out-of-order packets, sequence gaps/estimated missing telemetry, and load-shedding drops. The ingestion pipeline owns the behavior and FastAPI only exposes it. This component exists primarily because of Invariant 8 and provides visibility into the enforcement of the other invariants.

### Invariant → Component Coverage

| Invariant | Primary enforcing component(s) |
|---|---|
| 1. Valid telemetry only | UDP Receiver |
| 2. Bounded ingestion buffering | Bounded Ingestion Queue + Per-Asset State (bounded `recent_sequences`) |
| 3. Observable overload | UDP Receiver + Bounded Queue + Metrics |
| 4. Asset isolation | Telemetry Processor + Per-Asset State |
| 5. Monotonic sequence state | Sequence Classifier + Per-Asset State |
| 6. Receipt ≠ progress | Sequence Classifier + Per-Asset State |
| 7. Time-based health | Health Monitor + Per-Asset State |
| 8. Reliability is observable | Metrics / FastAPI Surface + producing components |

The table lists components that enforce runtime behavior. The Asset / Network Simulator exercises Invariants 4–7 during development and testing but does not enforce ingestion or state semantics itself.

## Deliberately Excluded

| Item | Reason |
|---|---|
| Kafka | P0 uses a bounded in-process queue; a distributed broker would add operational complexity without solving a current requirement. |
| Redis | P0 has no cross-process cache, coordination, or shared-state requirement. |
| PostgreSQL | P0 tests live reliability behavior and does not require durable telemetry or asset-state persistence. |
| Supabase | No hosted database, authentication, or backend-as-a-service capability is required. |
| Kubernetes | P0 is intentionally a single deployable process and does not require container orchestration. |
| Terraform | There is no P0 cloud infrastructure to provision or manage. |
| Cloud infra | P0 runs locally so the system behavior can be developed and demonstrated without a deployment dependency. |
| Auth | The P0 API is a local read-only inspection surface, not a public or multi-user service. |
| Frontend | P0 validates ingestion, reliability, health, and observability behavior through the API rather than a custom UI. |
| Microservices | Splitting the small P0 pipeline across services would introduce distributed coordination without satisfying an additional requirement. |
| Grafana | P0 exposes metrics/state directly; a separate visualization stack is unnecessary before the underlying signals are established. |
| Orbital mechanics | Assets are abstract telemetry producers; the project models distributed-system reliability rather than spacecraft physics. |
| Command/control | P0 observes simulated assets but does not send commands to or actuate them. |
| SpaceX APIs | The project uses only simulated data and has no dependency on proprietary or public SpaceX systems. |
| ML | P0 health and reliability behavior is deterministic and does not require learned models. |

## Non-Affiliation Disclaimer

ConstellationOps is an independent educational and simulation project. It is not affiliated with, endorsed by, sponsored by, or connected to SpaceX or any other real spaceflight, satellite, or aerospace company. All assets, telemetry, network behavior, and operational scenarios are simulated. The project does not model or interact with real spacecraft, orbital systems, command-and-control systems, or proprietary APIs.

# Hermes — Geospatial Dispatch & Matching Engine

> A high-throughput geospatial dispatch engine built in Go using Uber H3. Designed to answer the core dispatch question in sub-20ms:
> *"An order just dropped. Which of 10,000 moving riders is closest, available, and can be atomically assigned before concurrent orders claim them?"*

---

## Architecture Overview

```
[10,000 Mobile Rider WebSockets]
             │  (Fast network I/O, 512B socket buffers)
             ▼
[Buffered Channel: chan LocationPing]  <-- The Shock Absorber (Backpressure Defense)
             │
     ┌───────┴───────┬───────────────┐  <-- Drained by Fixed Worker Pool
     ▼               ▼               ▼
  [Worker 1]     [Worker 2] ...  [Worker 16]
     │               │               │
     └───────┬───────┴───────────────┘
             ▼  (Controlled, batched writes)
[Sharded In-Memory Spatial Index]       <-- 64 Shards (cell & 63 bitwise modulo)
             │
             ├── Rider Notebook (O(1) RiderID -> H3 Cell Map)
             └── 64 Sharded Cell Sets (H3 Cell -> Set[RiderID])
```

---

## Engineering Milestones

### Milestone 1: In-Memory Hexagonal Spatial Index (Complete)
- **Uber H3 Integration:** Resolution 8 (~460m edge length, ~0.737 km² hexagon area) for optimal urban dispatch.
- **64-Way Lock Striping:** Sharded mutexes via bitwise modulo (`cell & 63`), cutting per-shard lock contention by 98.4%.
- **Dual Indexing:** $O(1)$ rider lookup map guarded by `RWMutex` + sharded cell sets for rapid geographic lookups.
- **Concentric Ring Expansion:** Multi-ring radial search (`k=0, 1, 2...`) with deduplication.
- **Verification:** 50 concurrent writers (2,500 location hops) + 20 concurrent readers tested under `go test -race` with 0 races and 0 deadlocks.

### Milestone 2: High-Throughput GPS Ingestion & WebSockets (Complete)
- **Network Decoupling:** Persistent WebSocket connections decouple raw TCP reads from index locks via a bounded ring-buffer channel.
- **Fixed Worker Pool:** 16 CPU-bound background workers drain the channel, compute H3 spherical geometry, and update shard partitions.
- **Backpressure & Anti-Bufferbloat:** Non-blocking `select/default` enqueue drops stale pings when buffer saturates, preventing delayed GPS routing.
- **Memory Optimization:** Tuned Gorilla WebSocket socket buffers to 512 bytes (saving ~70MB of RAM across 10,000 persistent connections).
- **Verification:** 10,000 pings across 50 concurrent goroutines processed in **65ms** with 0 data races.

### Milestone 3: Matching Engine & Atomic Claim State Machine (Complete)
- **Lockless Rider State Machine:** 100% lockless atomic CAS state transitions (`StateIdle -> StateOffered -> StateAssigned`) with 64-bit numeric order ID binding via hardware `atomic.CompareAndSwapUint32`.
- **Auto-Cascading Dispatch:** Instantaneous cascade to adjacent candidates when a candidate is raced or busy, without global mutex convoys.
- **Unified HTTP/WebSocket Server:** `internal/server` tying M1 (Spatial Index), M2 (Ingestion Pipeline), and M3 (Matching Engine) together.
- **Verification:** 
  - Thundering Herd: 50 concurrent goroutines competing for 1 rider $\rightarrow$ exactly 1 winner, 49 cleanly rejected with `ErrNoRidersAvailable`.
  - Scale Concurrency: 50 orders vs 50 riders in the same cell at the exact same microsecond $\rightarrow$ 1:1 bipartite matching, 0 double-bookings, 0 split-brain states, 0 data races.
  - Full End-to-End Test: Live WebSocket GPS streaming while concurrent orders fire and match in sub-100ms.

### Milestone 4: Hardening, Offer Lifecycle & The Swiggy Async Flow (Complete)
- **Singleflight Order Deduplication:** Coalesces duplicate customer order submissions into a single in-flight dispatch using `orderFlight` channels, preventing duplicate allocations and ghost rider claims under network retry storms.
- **Ghost Rider Elimination & Timestamp Fencing:** Atomic cell migration holding `riderMu` throughout shard transfers (`riderMu` -> `shard.mu` strict lock hierarchy) eliminates ghost duplicate entries or void disappearance during rapid movement. Timestamp fencing discards stale, out-of-order GPS telemetry ($T_{new} \le T_{current}$).
- **15-Second Active Offer Lease Reaper & Auto-Cascade:** Auto-cascades expired offers to subsequent candidate riders via active `time.AfterFunc` deadline reapers.
- **0ms Explicit Rejection:** Instantaneous offer cascade when a rider explicitly declines an order without waiting for timeout leases.
- **The Swiggy Async Flow:** Decoupled `POST /api/v1/orders` returning `202 Accepted` in <2ms and executing background matching, paired with `GET /api/v1/orders/{order_id}` live status polling (`SEARCHING` -> `OFFERED` -> `ASSIGNED` / `EXHAUSTED`). Verified under `-race`.

### Milestone 5: Multi-Node Deployment & Chaos Testing
- Multi-replica state synchronization using Redis Pub/Sub.
- Live real-time Leaflet.js radar map.
- Chaos kill-testing verifying zero dropped orders under node failure.

---

## Running Tests

All tests are verified under Go's race detector:

```bash
# Run all unit and integration tests with race detection
go test -v -race ./...

# Run ingestion pipeline benchmarks
go test -v -bench=. -benchmem ./internal/ingest
```

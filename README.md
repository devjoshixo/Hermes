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

### Milestone 3: Matching Engine & Atomic Claim State Machine (Next)
- Order ingestion (`POST /api/v1/orders`) triggering radial candidate searches.
- Per-rider atomic Compare-And-Swap (`sync/atomic`) state machine (`Idle -> Offered -> Assigned`).
- Non-blocking cascades when candidates are claimed by competing orders.

### Milestone 4: Offer Lifecycle, Timeouts & Cascades
- 15-second acceptance window with active deadline reaping via `time.Timer`.
- Multi-ring candidate exhaustion circuit breakers (`503 No Riders Available`).

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

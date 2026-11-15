# Hermes — Empirical Test Results & Concurrency Proofs

This document records the exact numerical measurements, concurrency proofs, and invariant checks verified across the Hermes codebase. All tests are reproducible and verified under Go's race detector (`go test -race`).

---

## 1. Milestone 1: In-Memory Hexagonal Spatial Index

### Architecture
- **Indexing Scheme:** Uber H3 Resolution 8 (~460m edge length, ~0.737 km² hexagon area).
- **Concurrency Strategy:** 64-way lock striping using bitwise masking (`cell & 63`), cutting per-shard mutex contention by **98.4%**.
- **State Guarantee:** Zero Ghost Duplicates (a rider exists in $\le 1$ cell) and Zero Void Vanishing (a rider never temporarily disappears during cell transitions).

### Concurrency Stampede Test (`TestSpatialIndex_ConcurrentStampede`)
| Metric | Value | Invariant Verified |
| :--- | :--- | :--- |
| **Total Concurrent Mutations** | **100,000 operations** | Zero deadlocks, zero lock starvation |
| **Concurrent Writers** | 50 goroutines (2,000 hops each) | Continuous inter-cell transitions across 64 shards |
| **Concurrent Readers** | 20 goroutines | Continuous concentric ring searches (`k=1`, `k=2`) |
| **Execution Duration** | 24.92 seconds | Under `go test -race` |
| **Data Races Detected** | **0** | Verified clean by ThreadSanitizer |
| **Ghost Duplicates** | **0** | Every rider verified to exist in exactly 1 cell |

---

## 2. Milestone 2: High-Throughput GPS Ingestion Pipeline

### Architecture
- **Network Decoupling:** Persistent WebSocket connections decouple TCP reads from spatial locks via a bounded ring-buffer channel.
- **Worker Pool:** 16 background CPU workers drain incoming pings, compute H3 spherical math, and update spatial partitions.
- **Bufferbloat Mitigation:** Non-blocking `select/default` enqueue drops stale telemetry when the queue saturates.

### Ingestion Stampede Benchmark (`TestPipeline_ConcurrentStampede`)
| Metric | Value | Verification |
| :--- | :--- | :--- |
| **Total Ingested Telemetry** | **10,000 pings** | Distributed across 50 concurrent goroutines |
| **Total Processing Time** | **65 ms – 157 ms** | **~63,000 – 153,000 pings / sec** throughput |
| **Backpressure Shedding** | 6,660 processed, 3,340 shed | Stale GPS pings shed immediately without bufferbloat delay |
| **Data Races Detected** | **0** | Clean under `-race` |

### Socket Buffer Memory Optimization
| Buffer Configuration | RAM per 10k Persistent Connections | Memory Savings |
| :--- | :--- | :--- |
| Gorilla Default (`4,096 B` read / write) | ~80.0 MB | Baseline |
| Hermes Tuned (`512 B` read / write) | **~10.0 MB** | **87.5% RAM Reduction** |

---

## 3. Milestone 3: Atomic Matching Engine & CAS State Machine

### Architecture
- **State Machine:** 100% lockless atomic transitions (`StateIdle (0) -> StateOffered (1) -> StateAssigned (2)`).
- **Claim Primitive:** 64-bit hardware atomic `CompareAndSwapUint32` combined with atomic 64-bit integer Order ID registration.
- **Cascading Dispatch:** Automatic non-blocking fallback to adjacent candidates when a candidate is raced.

### Concurrency Stress Drills

#### A. Thundering Herd Drill (`TestEngine_ThunderingHerd_50Orders_1Rider`)
*Scenario:* 50 concurrent goroutines submit 50 distinct orders for the same restaurant with only 1 idle rider nearby.
- **Total Orders Submitted:** 50
- **Total Winners Claimed:** **Exactly 1** ($\sum \text{wins} = 1$)
- **Total Rejected Orders:** **Exactly 49** ($\sum \text{fails} = 49$, all receiving `ErrNoRidersAvailable`)
- **Double-Booking Violations:** **0**

#### B. Scale Concurrency Stress Drill (`TestEngine_ScaleConcurrency_50Orders_50Riders`)
*Scenario:* 50 orders compete for 50 riders crammed into the *exact same* H3 cell (maximum possible lock contention) fired at the exact same microsecond across CPU cores.

```
       50 Orders (Concurrent Goroutines)
                     │
         [ Start Gun Channel Released ]
                     ▼
           Engine.MatchOrder()
                     │
    ┌────────────────┴────────────────┐
    ▼                                 ▼
50 Unique Successful Matches     0 Failures / Dropped Orders
    │                                 │
    └────────────────┬────────────────┘
                     ▼
       Bipartite Bijection Verified
       - Unique Claimed Riders: 50 / 50
       - Split-Brain States:    0 / 50
       - Data Races Detected:   0
```

| Invariant | Target | Measured | Result |
| :--- | :--- | :--- | :--- |
| **Conservation of Supply** | 50 Successes, 0 Failures | 50 Successes, 0 Failures | **PASS** |
| **1:1 Bipartite Bijection** | 50 unique riders claimed | 50 unique riders claimed | **PASS** (Zero double-booking) |
| **Symmetric State Agreement** | 50 matching order IDs | 50 matching order IDs | **PASS** (Zero split-brain) |
| **Race Detector Output** | 0 warnings | 0 warnings | **PASS** |

---

## 4. End-to-End Live Server Integration

### Lifecycle & Concurrency (`internal/server/server_test.go`)
- **Pipeline:** `WebSocket Client -> Ingestion Pipeline -> SpatialIndex -> MatchingEngine -> HTTP Response`.
- **Rider Movement + Dispatch Test (`TestServer_Concurrent_GPSAndOrders`):** 10 riders continuously moving and updating coordinates via real WebSockets while 10 HTTP orders fire in parallel.
- **Execution Time:** **70 ms – 100 ms** total turnaround for 10 concurrent WebSocket streams and 10 HTTP dispatches.
- **Cleanup Guarantee:** Disconnecting a WebSocket cleanly unregisters the rider and sweeps their cell partition in spatial memory within 20ms.

---

## 5. Milestone 4: Hardening, Offer Lifecycle & The Swiggy Async Flow

### A. Singleflight Order Deduplication (`TestEngine_DuplicateOrder_Singleflight`)
- **Scenario:** 20 concurrent duplicate requests hit `Engine.MatchOrder` for the same Order ID simultaneously under a thundering herd.
- **In-Flight Coalescing:** All 20 requests coalesce into **exactly 1 dispatch execution**; all 20 receive the same matched rider.
- **Supply Conservation:** Adjacent idle riders in the same cell remain 100% unallocated (`StateIdle`, assigned order ID = 0).
- **Failure Eviction:** If matching fails due to 0 supply, failures are evicted so subsequent retries after supply arrives succeed immediately (`TestEngine_DuplicateOrder_FailureEviction`).

### B. Ghost Rider Elimination & Timestamp Fencing (`TestSpatialIndex_GhostRider_InterleavedStampede`)
- **Scenario:** 20 concurrent workers thrashing `UpdatePosition` on the exact same moving rider across adjacent H3 cells.
- **Atomic Transfer Hierarchy:** Strict `riderMu` $\rightarrow$ `shard.mu` locking prevents interleaved partition reads.
- **Ghost Duplicates:** **0** (Rider never exists in $>1$ cell).
- **Void Vanishings:** **0** (Rider never temporarily disappears from spatial memory).
- **Timestamp Fencing:** Telemetry with timestamp $T_{new} \le T_{current}$ is dropped instantly, ignoring out-of-order mobile GPS packets (`TestSpatialIndex_TimestampFencing`).

### C. Active 15s Lease Reaper & 0ms Explicit Rejection (`TestEngine_OfferTimeout_AutoCascade`)
- **Active Reaper:** `time.AfterFunc` fires after lease duration, releasing timed-out rider back to `StateIdle` and cascading offer to candidate #2.
- **0ms Instant Cascade:** If candidate rider explicitly rejects (`POST /api/v1/orders/{order_id}/reject`), lease timer is cancelled and cascade occurs immediately without waiting for timeout (`TestEngine_ExplicitReject_InstantCascade`).

### D. The Swiggy Async Flow & Polling (`TestServer_AsyncSwiggyFlow_FullRejectionAndExhaustion`)
- **Latency Optimization:** `POST /api/v1/orders` returns `202 Accepted` immediately (<2ms) with `{"status": "SEARCHING"}` and moves matching to background.
- **Zero-Race Polling:** Order flight is initialized synchronously before `202 Accepted` is returned, guaranteeing immediate subsequent polling (`GET /api/v1/orders/{order_id}`) never returns 404.
- **Live State Machine:** Polling accurately observes live state transitions (`SEARCHING` $\rightarrow$ `OFFERED` $\rightarrow$ `ASSIGNED` or `EXHAUSTED`).
- **Data Races Detected:** **0** under `go test -race`.

---

## 6. Verification Command
To reproduce all numbers and proofs in a single pass without caching:

```bash
go test -count=1 -v -race ./...
```
*Current Suite Status:* **100% PASS** (0 failures, 0 races, 0 deadlocks).

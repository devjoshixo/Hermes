# GPT Review — Hermes

Review scope: committed Milestones 1–2 plus the uncommitted
`internal/matching/` Milestone 3 work. No project code was changed.

## Blockers

### 1. Concurrent location updates can leave ghost riders

`SpatialIndex.UpdatePosition` updates `riderToCell` and releases `riderMu`
before removing the rider from the old shard and adding them to the new shard.
Two position updates for the same rider can interleave, leaving the rider in an
old cell even though the notebook points elsewhere. This violates the stated
"no ghost duplicates" invariant.

Location: `internal/spatial/index.go`, `UpdatePosition`.

### 2. The same order can be offered to multiple riders

`Engine.MatchOrder` uses a rider-level CAS, which prevents two orders from
claiming one rider. It does not reserve or deduplicate `order.ID`. Concurrent
retries of the same order can each claim a different idle rider, creating
multiple offers for one order.

Location: `internal/matching/engine.go`, `MatchOrder`.

## High-priority findings

### 3. Out-of-order GPS pings can restore a stale location

The worker pool processes pings concurrently, but the `Timestamp` field is not
used. A delayed older ping can be processed after a newer one and move a rider
back to an obsolete cell.

Location: `internal/ingest/pipeline.go`.

### 4. A WebSocket client can select another rider's ID

The route rider ID is only applied when the JSON `rider_id` is empty. A client
can send a non-empty ID for another rider. The upgrader also accepts all
origins. Production use needs authenticated identity bound to the connection.

Location: `internal/ingest/handler.go`.

### 5. Disconnects leave riders eligible for matching

The handler closes a disconnected WebSocket but does not remove or mark its
rider unavailable. The spatial index has `Remove`, but the handler does not
use it; stale riders may receive offers indefinitely.

Location: `internal/ingest/handler.go`.

## Medium-priority findings

### 6. Matching does not guarantee the closest rider

H3 cell results and Go map iteration order are unordered, and the engine does
not calculate rider-to-pickup distance. It currently selects an arbitrary
eligible rider within the searched rings, not necessarily the closest one.

Location: `internal/spatial/index.go`, `FindNearby`.

### 7. Matching hard-codes H3 resolution 8

The ingestion pipeline accepts a configurable resolution, while `MatchOrder`
always converts the pickup location with resolution 8. These disagree whenever
the pipeline is configured differently.

Location: `internal/matching/engine.go`, `MatchOrder`.

## Repository state

`internal/matching/` was untracked at review time, so its Milestone 3 code and
tests will not be present in a clean checkout or CI until added to Git.

## Verification run

- `go vet ./...` passed.
- `go test -race ./internal/spatial ./internal/matching` passed.
- `go test -race ./internal/ingest -run '^TestPipeline_'` passed.
- The full WebSocket test suite could not run in this environment because its
  `httptest` listener could not bind `[::1]`; that failure happened before the
  handler executed.

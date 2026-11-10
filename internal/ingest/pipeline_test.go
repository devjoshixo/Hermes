package ingest

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/devjoshixo/hermes/internal/spatial"
	"github.com/uber/h3-go/v3"
)

func TestPipeline_BasicIngestion(t *testing.T) {
	index := spatial.NewSpatialIndex(64)
	pipeline := NewPipeline(index, 100, 4, 8)
	defer pipeline.Stop()

	// Connaught Place, New Delhi
	ping := LocationPing{
		RiderID:   "rider_101",
		Lat:       28.6315,
		Lng:       77.2167,
		Timestamp: time.Now().UnixMilli(),
	}

	ok := pipeline.Submit(ping)
	if !ok {
		t.Fatalf("expected ping to be enqueued successfully")
	}

	// Calculate expected cell
	coord := h3.GeoCoord{Latitude: ping.Lat, Longitude: ping.Lng}
	expectedCell := uint64(h3.FromGeo(coord, 8))

	// Allow worker a brief moment to process
	var found bool
	for i := 0; i < 50; i++ {
		riders := index.GetRidersInCell(expectedCell)
		for _, r := range riders {
			if r == "rider_101" {
				found = true
				break
			}
		}
		if found {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	if !found {
		t.Fatalf("expected rider_101 to be in cell %d, but was not found", expectedCell)
	}

	proc, drop := pipeline.Stats()
	if proc != 1 || drop != 0 {
		t.Fatalf("unexpected stats: processed=%d, dropped=%d", proc, drop)
	}
}

func TestPipeline_BackpressureDrop(t *testing.T) {
	index := spatial.NewSpatialIndex(64)
	// Tiny buffer of 3, but we won't start workers immediately to force buffer saturation
	p := &Pipeline{
		index:      index,
		pingCh:     make(chan LocationPing, 3),
		numWorkers: 1,
		resolution: 8,
	}
	// Do NOT call p.start() yet: channel won't be drained

	ping := LocationPing{
		RiderID:   "rider_test",
		Lat:       28.6315,
		Lng:       77.2167,
		Timestamp: time.Now().UnixMilli(),
	}

	// First 3 must succeed
	for i := 0; i < 3; i++ {
		if !p.Submit(ping) {
			t.Fatalf("expected ping %d to succeed", i)
		}
	}

	// 4th and 5th MUST be dropped by backpressure defense
	for i := 3; i < 5; i++ {
		if p.Submit(ping) {
			t.Fatalf("expected ping %d to be dropped due to buffer saturation", i)
		}
	}

	_, dropped := p.Stats()
	if dropped != 2 {
		t.Fatalf("expected 2 dropped pings, got %d", dropped)
	}
}

func TestPipeline_ConcurrentStampede(t *testing.T) {
	index := spatial.NewSpatialIndex(64)
	pipeline := NewPipeline(index, 5000, 16, 8)

	numGoroutines := 50
	pingsPerGoroutine := 200 // 10,000 total pings
	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	startTime := time.Now()

	for g := 0; g < numGoroutines; g++ {
		go func(gID int) {
			defer wg.Done()
			for p := 0; p < pingsPerGoroutine; p++ {
				ping := LocationPing{
					RiderID:   fmt.Sprintf("rider_%d_%d", gID, p%10),
					Lat:       28.6315 + (float64(p) * 0.001),
					Lng:       77.2167 + (float64(gID) * 0.001),
					Timestamp: time.Now().UnixMilli(),
				}
				pipeline.Submit(ping)
			}
		}(g)
	}

	wg.Wait()
	pipeline.Stop()

	elapsed := time.Since(startTime)
	proc, drop := pipeline.Stats()

	t.Logf("Stampede finished in %v: Processed=%d, Dropped=%d", elapsed, proc, drop)

	if proc+drop != uint64(numGoroutines*pingsPerGoroutine) {
		t.Fatalf("total accounted pings (%d) != sent pings (%d)", proc+drop, numGoroutines*pingsPerGoroutine)
	}
}

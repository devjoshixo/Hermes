package spatial

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/uber/h3-go/v3"
)

// TestRiderMovement verifies that when a rider moves:
// 1. They appear in the new cell.
// 2. They are completely removed from the old cell (No Ghost Duplicates).
func TestRiderMovement(t *testing.T) {
	idx := NewSpatialIndex(64)

	// Connaught Place (Cell A)
	cellA := uint64(h3.FromGeo(h3.GeoCoord{Latitude: 28.6315, Longitude: 77.2167}, 8))

	// Karol Bagh (Cell B)
	cellB := uint64(h3.FromGeo(h3.GeoCoord{Latitude: 28.6517, Longitude: 77.1906}, 8))

	// Step 1: Rider starts at Connaught Place (Cell A) at t=1000
	idx.UpdatePosition("rider_dev", cellA, 1000)

	ridersInA := idx.GetRidersInCell(cellA)
	if len(ridersInA) != 1 || ridersInA[0] != "rider_dev" {
		t.Fatalf("expected [rider_dev] in Cell A, got: %v", ridersInA)
	}

	// Step 2: Rider drives to Karol Bagh (Cell B) at t=2000
	idx.UpdatePosition("rider_dev", cellB, 2000)

	// Check Karol Bagh (Cell B): Must be there!
	ridersInB := idx.GetRidersInCell(cellB)
	if len(ridersInB) != 1 || ridersInB[0] != "rider_dev" {
		t.Fatalf("expected [rider_dev] in Cell B, got: %v", ridersInB)
	}

	// Check Connaught Place (Cell A): Must be GONE! (No Ghost Duplicate)
	ridersInAAfter := idx.GetRidersInCell(cellA)
	if len(ridersInAAfter) != 0 {
		t.Fatalf("Invariant broken! Ghost rider found in Cell A: %v", ridersInAAfter)
	}
}

// TestSpatialIndex_FindNearby verifies concentric ring expansion:
// 1. Returns empty when no riders are nearby.
// 2. Returns only center riders when maxK = 0.
// 3. Expands to neighbor rings when maxK = 1 to find more riders.
func TestSpatialIndex_FindNearby(t *testing.T) {
	idx := NewSpatialIndex(64)

	// Connaught Place (Center / Ring 0)
	origin := uint64(h3.FromGeo(h3.GeoCoord{Latitude: 28.6315, Longitude: 77.2167}, 8))

	// Get neighbor hexagons in Ring 1
	neighbors := h3.KRing(h3.H3Index(origin), 1)
	neighborCell1 := uint64(neighbors[1])
	neighborCell2 := uint64(neighbors[2])

	// Case 1: Empty sector -> should return 0 riders
	emptyResult := idx.FindNearby(origin, 0, 10)
	if len(emptyResult) != 0 {
		t.Fatalf("expected 0 riders in empty sector, got %d", len(emptyResult))
	}

	// Case 2: Put 1 rider in center, 2 in neighbor ring
	idx.UpdatePosition("rider_center", origin, 1000)
	idx.UpdatePosition("rider_near_1", neighborCell1, 1000)
	idx.UpdatePosition("rider_near_2", neighborCell2, 1000)

	// Search ONLY Ring 0 (maxK = 0)
	centerOnly := idx.FindNearby(origin, 0, 10)
	if len(centerOnly) != 1 || centerOnly[0] != "rider_center" {
		t.Fatalf("expected only [rider_center] in Ring 0, got %v", centerOnly)
	}

	// Search Ring 0 + Ring 1 (maxK = 1) -> Should gather all 3!
	allNearby := idx.FindNearby(origin, 1, 10)
	if len(allNearby) != 3 {
		t.Fatalf("expected all 3 riders in Ring 1, got %d: %v", len(allNearby), allNearby)
	}
}

// TestSpatialIndex_TimestampFencing proves that out-of-order stale GPS pings are dropped.
func TestSpatialIndex_TimestampFencing(t *testing.T) {
	idx := NewSpatialIndex(64)

	cellA := uint64(h3.FromGeo(h3.GeoCoord{Latitude: 28.6315, Longitude: 77.2167}, 8)) // CP
	cellB := uint64(h3.FromGeo(h3.GeoCoord{Latitude: 28.6517, Longitude: 77.1906}, 8)) // Karol Bagh

	// 1. Initial position at t = 1000 in Cell A
	if !idx.UpdatePosition("rider_speedy", cellA, 1000) {
		t.Fatalf("expected initial position update to succeed")
	}

	// 2. Delayed out-of-order ping arrives from past: t = 500 claiming rider is in Cell B
	accepted := idx.UpdatePosition("rider_speedy", cellB, 500)
	if accepted {
		t.Fatalf("STALE PING ACCEPTED: expected timestamp 500 to be rejected when current is 1000")
	}

	// Invariant: Rider must STILL be in Cell A, and NOT in Cell B
	cell, exists := idx.GetRiderCell("rider_speedy")
	if !exists || cell != cellA {
		t.Fatalf("rider moved incorrectly to stale cell: %d", cell)
	}
	if len(idx.GetRidersInCell(cellB)) != 0 {
		t.Fatalf("stale ping created ghost rider in Cell B!")
	}

	// 3. Newer ping arrives: t = 2000 moving rider to Cell B
	if !idx.UpdatePosition("rider_speedy", cellB, 2000) {
		t.Fatalf("expected newer ping t=2000 to be accepted")
	}

	cell, exists = idx.GetRiderCell("rider_speedy")
	if !exists || cell != cellB {
		t.Fatalf("expected rider to be in Cell B, got: %d", cell)
	}
}

// TestSpatialIndex_GhostRider_InterleavedStampede fires 20 concurrent goroutines
// updating the location of the EXACT SAME rider to eliminate the ghost rider race condition.
func TestSpatialIndex_GhostRider_InterleavedStampede(t *testing.T) {
	idx := NewSpatialIndex(64)
	origin := uint64(h3.FromGeo(h3.GeoCoord{Latitude: 28.6315, Longitude: 77.2167}, 8))
	sampleCellsRaw := h3.KRing(h3.H3Index(origin), 3)

	const totalUpdates = 200
	var wg sync.WaitGroup
	startGun := make(chan struct{})

	// 20 workers racing to update the exact same rider's location concurrently
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			<-startGun

			for j := 0; j < 10; j++ {
				// Guaranteed monotonically increasing timestamps across iterations
				ts := time.Now().UnixNano()
				targetCell := uint64(sampleCellsRaw[rand.Intn(len(sampleCellsRaw))])
				idx.UpdatePosition("rider_contested", targetCell, ts)
			}
		}(i)
	}

	close(startGun)
	wg.Wait()

	// INVARIANT AUDIT: Rider must exist in EXACTLY ONE cell across the entire spatial index!
	var cellOccurrences []uint64
	for _, rawCell := range sampleCellsRaw {
		cellID := uint64(rawCell)
		riders := idx.GetRidersInCell(cellID)
		for _, r := range riders {
			if r == "rider_contested" {
				cellOccurrences = append(cellOccurrences, cellID)
			}
		}
	}

	if len(cellOccurrences) > 1 {
		t.Fatalf("GHOST RIDER DETECTED: rider_contested was found in %d cells simultaneously: %v",
			len(cellOccurrences), cellOccurrences)
	}
	if len(cellOccurrences) == 0 {
		t.Fatalf("VOID VANISHING: rider_contested vanished from all cells!")
	}
}

// TestSpatialIndex_ConcurrentStampede fires 50 writers and 20 readers simultaneously.
func TestSpatialIndex_ConcurrentStampede(t *testing.T) {
	idx := NewSpatialIndex(64)
	origin := uint64(h3.FromGeo(h3.GeoCoord{Latitude: 28.6315, Longitude: 77.2167}, 8))

	sampleCellsRaw := h3.KRing(h3.H3Index(origin), 3)
	sampleCells := make([]uint64, len(sampleCellsRaw))
	for i, c := range sampleCellsRaw {
		sampleCells[i] = uint64(c)
	}

	var readersWg sync.WaitGroup
	var writersWg sync.WaitGroup
	stopSignal := make(chan struct{})

	// 1. 20 concurrent readers continuously searching nearby
	for r := 0; r < 20; r++ {
		readersWg.Add(1)
		go func(readerID int) {
			defer readersWg.Done()
			for {
				select {
				case <-stopSignal:
					return
				default:
					randomOrigin := sampleCells[rand.Intn(len(sampleCells))]
					_ = idx.FindNearby(randomOrigin, 2, 5)
				}
			}
		}(r)
	}

	// 2. 50 concurrent writers firing 2,500 total location updates
	for w := 0; w < 50; w++ {
		writersWg.Add(1)
		go func(writerID int) {
			defer writersWg.Done()
			for i := 0; i < 50; i++ {
				riderID := fmt.Sprintf("rider_%d_%d", writerID, i%10)
				randomCell := sampleCells[rand.Intn(len(sampleCells))]
				idx.UpdatePosition(riderID, randomCell, time.Now().UnixNano())
			}
		}(w)
	}

	writersWg.Wait()
	close(stopSignal)
	readersWg.Wait()

	// 3. Final Invariant Audit
	idx.riderMu.RLock()
	defer idx.riderMu.RUnlock()

	for riderID, loc := range idx.riderToLocation {
		ridersInCell := idx.GetRidersInCell(loc.cell)
		found := false
		for _, r := range ridersInCell {
			if r == riderID {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("Invariant broken! Rider %s is in notebook for cell %d, but missing in shard!", riderID, loc.cell)
		}
	}
}

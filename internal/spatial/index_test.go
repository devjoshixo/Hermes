package spatial

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"

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

	// Step 1: Rider starts at Connaught Place (Cell A)
	idx.UpdatePosition("rider_dev", cellA)

	ridersInA := idx.GetRidersInCell(cellA)
	if len(ridersInA) != 1 || ridersInA[0] != "rider_dev" {
		t.Fatalf("expected [rider_dev] in Cell A, got: %v", ridersInA)
	}

	// Step 2: Rider drives to Karol Bagh (Cell B)
	idx.UpdatePosition("rider_dev", cellB)

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
	idx.UpdatePosition("rider_center", origin)
	idx.UpdatePosition("rider_near_1", neighborCell1)
	idx.UpdatePosition("rider_near_2", neighborCell2)

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

// TestSpatialIndex_ConcurrentStampede fires 50 writers and 20 readers simultaneously.
// Ran with 'go test -race' to verify that lock-striping prevents:
// 1. Data races (concurrent map read/write panics)
// 2. Deadlocks between shards
// 3. Lost rider updates
func TestSpatialIndex_ConcurrentStampede(t *testing.T) {
	idx := NewSpatialIndex(64)
	origin := uint64(h3.FromGeo(h3.GeoCoord{Latitude: 28.6315, Longitude: 77.2167}, 8))

	// Get 37 hexagons around Delhi as our testing playground
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
					return // stop when told to
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
				idx.UpdatePosition(riderID, randomCell)
			}
		}(w)
	}

	// Wait for all 50 writers to finish their 2,500 updates
	writersWg.Wait()

	// Stop all readers and wait for them to finish
	close(stopSignal)
	readersWg.Wait()

	// 3. Final Invariant Audit: Did any rider get lost or corrupted?
	idx.riderMu.RLock()
	defer idx.riderMu.RUnlock()

	for riderID, cellID := range idx.riderToCell {
		ridersInCell := idx.GetRidersInCell(cellID)
		found := false
		for _, r := range ridersInCell {
			if r == riderID {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("Invariant broken! Rider %s is in notebook for cell %d, but missing in shard!", riderID, cellID)
		}
	}
}

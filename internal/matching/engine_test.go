package matching

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/devjoshixo/hermes/internal/spatial"
	"github.com/uber/h3-go/v3"
)

func TestEngine_MatchAndAccept(t *testing.T) {
	// Connaught Place coordinates
	cpLat := 28.6315
	cpLng := 77.2167
	cpCell := uint64(h3.FromGeo(h3.GeoCoord{Latitude: cpLat, Longitude: cpLng}, 8))

	// 1. Initialize Spatial Index and Matching Engine
	idx := spatial.NewSpatialIndex(64)
	engine := NewEngine(idx, 5*time.Second)

	// 2. Register Rider and place them in the spatial index
	rider := NewRider("rider_cp")
	engine.RegisterRider(rider)
	idx.UpdatePosition("rider_cp", cpCell)

	// Invariant 0: Rider must start Idle with order ID 0
	if rider.State() != StateIdle {
		t.Fatalf("expected initial state StateIdle (%d), got %d", StateIdle, rider.State())
	}
	if rider.AssignedOrderID() != 0 {
		t.Fatalf("expected initial order ID 0, got %d", rider.AssignedOrderID())
	}

	// 3. Customer places Order #101 at Connaught Place
	order := Order{
		ID:        101,
		PickupLat: cpLat,
		PickupLng: cpLng,
	}

	// 4. Engine matches order
	matchedRider, err := engine.MatchOrder(order, 1, 5)
	if err != nil {
		t.Fatalf("expected successful match, got error: %v", err)
	}

	// Invariant 1: Matched rider must be rider_cp, state StateOffered, order ID 101
	if matchedRider.ID != "rider_cp" {
		t.Fatalf("expected matched rider rider_cp, got %s", matchedRider.ID)
	}
	if matchedRider.State() != StateOffered {
		t.Fatalf("expected StateOffered (%d), got %d", StateOffered, matchedRider.State())
	}
	if matchedRider.AssignedOrderID() != 101 {
		t.Fatalf("expected assigned order ID 101, got %d", matchedRider.AssignedOrderID())
	}

	// 5. Rider taps "Accept" on their phone
	accepted := engine.AcceptOrder("rider_cp", 101)
	if !accepted {
		t.Fatalf("expected AcceptOrder to succeed")
	}

	// Invariant 2: State must transition to StateAssigned, order ID remains 101
	if matchedRider.State() != StateAssigned {
		t.Fatalf("expected StateAssigned (%d), got %d", StateAssigned, matchedRider.State())
	}
	if matchedRider.AssignedOrderID() != 101 {
		t.Fatalf("expected assigned order ID 101, got %d", matchedRider.AssignedOrderID())
	}
}

func TestEngine_AutoCascade(t *testing.T) {
	cpLat := 28.6315
	cpLng := 77.2167
	cpCell := uint64(h3.FromGeo(h3.GeoCoord{Latitude: cpLat, Longitude: cpLng}, 8))

	idx := spatial.NewSpatialIndex(64)
	engine := NewEngine(idx, 5*time.Second)

	// Register Rider A and Rider B at the same location
	riderA := NewRider("rider_A")
	riderB := NewRider("rider_B")
	engine.RegisterRider(riderA)
	engine.RegisterRider(riderB)
	idx.UpdatePosition("rider_A", cpCell)
	idx.UpdatePosition("rider_B", cpCell)

	// Precondition: Rider A is already busy / offered Order #1
	if !riderA.TryOffer(1) {
		t.Fatalf("failed to set up precondition: riderA should accept offer 1")
	}

	// Action: Order #2 arrives at Connaught Place
	order2 := Order{
		ID:        2,
		PickupLat: cpLat,
		PickupLng: cpLng,
	}

	matchedRider, err := engine.MatchOrder(order2, 1, 5)
	if err != nil {
		t.Fatalf("expected cascade match to succeed, got: %v", err)
	}

	// Invariant 1: Engine skipped busy Rider A and locked Rider B
	if matchedRider.ID != "rider_B" {
		t.Fatalf("expected matched rider to be rider_B, got %s", matchedRider.ID)
	}
	if matchedRider.State() != StateOffered {
		t.Fatalf("expected rider_B to be StateOffered, got %d", matchedRider.State())
	}
	if matchedRider.AssignedOrderID() != 2 {
		t.Fatalf("expected rider_B to be assigned Order #2, got %d", matchedRider.AssignedOrderID())
	}

	// Invariant 2: Rider A's state was NOT corrupted
	if riderA.State() != StateOffered {
		t.Fatalf("expected rider_A state to remain StateOffered, got %d", riderA.State())
	}
	if riderA.AssignedOrderID() != 1 {
		t.Fatalf("expected rider_A order ID to remain 1, got %d", riderA.AssignedOrderID())
	}
}

func TestEngine_ThunderingHerd_50Orders_1Rider(t *testing.T) {
	cpLat := 28.6315
	cpLng := 77.2167
	cpCell := uint64(h3.FromGeo(h3.GeoCoord{Latitude: cpLat, Longitude: cpLng}, 8))

	idx := spatial.NewSpatialIndex(64)
	engine := NewEngine(idx, 5*time.Second)

	// 1 single rider available at Connaught Place
	soloRider := NewRider("rider_solo")
	engine.RegisterRider(soloRider)
	idx.UpdatePosition("rider_solo", cpCell)

	const totalOrders = 50
	var winCount int64
	var failCount int64

	var wg sync.WaitGroup
	startGun := make(chan struct{})

	// Spin up 50 goroutines competing for the same rider at the exact same moment
	for i := 1; i <= totalOrders; i++ {
		wg.Add(1)
		orderID := uint64(1000 + i)
		go func(id uint64) {
			defer wg.Done()
			<-startGun // Synchronized release

			order := Order{
				ID:        id,
				PickupLat: cpLat,
				PickupLng: cpLng,
			}

			matched, err := engine.MatchOrder(order, 1, 5)
			if err == nil && matched != nil {
				atomic.AddInt64(&winCount, 1)
			} else if errors.Is(err, ErrNoRidersAvailable) {
				atomic.AddInt64(&failCount, 1)
			}
		}(orderID)
	}

	// Fire start gun!
	close(startGun)
	wg.Wait()

	// Invariant 1: Exactly 1 winner, 49 losers (Strict Mutual Exclusion)
	if winCount != 1 {
		t.Fatalf("MUTUAL EXCLUSION VIOLATION: expected exactly 1 winner, got %d", winCount)
	}
	if failCount != totalOrders-1 {
		t.Fatalf("expected exactly %d failures, got %d", totalOrders-1, failCount)
	}

	// Invariant 2: Solo rider is in StateOffered
	if soloRider.State() != StateOffered {
		t.Fatalf("expected solo rider to be StateOffered, got %d", soloRider.State())
	}

	// Invariant 3: Solo rider is assigned to a valid winning order ID
	assignedID := soloRider.AssignedOrderID()
	if assignedID < 1001 || assignedID > 1000+totalOrders {
		t.Fatalf("invalid assigned order ID: %d", assignedID)
	}
}

func TestEngine_ScaleConcurrency_50Orders_50Riders(t *testing.T) {
	cpLat := 28.6315
	cpLng := 77.2167
	cpCell := uint64(h3.FromGeo(h3.GeoCoord{Latitude: cpLat, Longitude: cpLng}, 8))

	idx := spatial.NewSpatialIndex(64)
	engine := NewEngine(idx, 5*time.Second)

	const poolSize = 50

	// 1. Register 50 riders, all in the same Connaught Place cell (maximum lock contention)
	riders := make([]*Rider, poolSize)
	for i := 0; i < poolSize; i++ {
		rID := fmt.Sprintf("rider_%02d", i)
		r := NewRider(rID)
		riders[i] = r
		engine.RegisterRider(r)
		idx.UpdatePosition(rID, cpCell)
	}

	var (
		mu           sync.Mutex
		matchedMap   = make(map[uint64]string) // orderID -> riderID
		claimedBy    = make(map[string]uint64) // riderID -> orderID
		successCount int64
		failCount    int64
		wg           sync.WaitGroup
		startGun     = make(chan struct{})
	)

	// 2. Launch 50 concurrent orders competing for the 50 riders
	for i := 0; i < poolSize; i++ {
		wg.Add(1)
		orderID := uint64(5000 + i)
		go func(id uint64) {
			defer wg.Done()
			<-startGun // Synchronized release across all CPU cores

			order := Order{
				ID:        id,
				PickupLat: cpLat,
				PickupLng: cpLng,
			}

			// Search maxK=1 with limit=poolSize so all candidates are returned
			matched, err := engine.MatchOrder(order, 1, poolSize)
			if err == nil && matched != nil {
				atomic.AddInt64(&successCount, 1)
				mu.Lock()
				matchedMap[id] = matched.ID
				claimedBy[matched.ID] = id
				mu.Unlock()
			} else {
				atomic.AddInt64(&failCount, 1)
			}
		}(orderID)
	}

	// 3. Fire start gun!
	close(startGun)
	wg.Wait()

	// Invariant 3: Zero unmatched waste (Conservation of Supply)
	if successCount != poolSize {
		t.Fatalf("expected all %d orders to succeed, but got %d successes and %d failures", poolSize, successCount, failCount)
	}

	// Invariant 1: Strict 1:1 Bijection (No double-booking)
	if len(claimedBy) != poolSize {
		t.Fatalf("DOUBLE BOOKING DETECTED: expected %d unique riders claimed, but only %d were claimed", poolSize, len(claimedBy))
	}
	if len(matchedMap) != poolSize {
		t.Fatalf("expected %d matched orders, got %d", poolSize, len(matchedMap))
	}

	// Invariant 2: Symmetric State Agreement across all 50 riders
	for _, r := range riders {
		if r.State() != StateOffered {
			t.Fatalf("expected rider %s to be StateOffered, got %d", r.ID, r.State())
		}
		expectedOrderID, exists := claimedBy[r.ID]
		if !exists {
			t.Fatalf("rider %s was never recorded in claimedBy", r.ID)
		}
		if r.AssignedOrderID() != expectedOrderID {
			t.Fatalf("SPLIT BRAIN: rider %s thinks assigned order is %d, but order %d claims it",
				r.ID, r.AssignedOrderID(), expectedOrderID)
		}
	}
}

package matching

import (
	"sync"
	"sync/atomic"
	"testing"
)

func TestRider_StateTransitions(t *testing.T) {
	rider := NewRider("rider_delhi_1")

	// 1. Initial State: Idle, no order
	if rider.State() != StateIdle || rider.AssignedOrderID() != 0 {
		t.Fatalf("expected initial state Idle, got state=%d order=%d", rider.State(), rider.AssignedOrderID())
	}

	// 2. Order 101 claims offer
	if !rider.TryOffer(101) {
		t.Fatalf("expected TryOffer(101) to succeed")
	}
	if rider.State() != StateOffered || rider.AssignedOrderID() != 101 {
		t.Fatalf("expected state Offered with order 101, got state=%d order=%d", rider.State(), rider.AssignedOrderID())
	}

	// 3. Order 102 attempts to steal: MUST FAIL
	if rider.TryOffer(102) {
		t.Fatalf("expected TryOffer(102) to fail on already offered rider")
	}

	// 4. Order 102 tries to confirm: MUST FAIL
	if rider.ConfirmAssign(102) {
		t.Fatalf("expected ConfirmAssign(102) to fail for non-owner order")
	}

	// 5. Order 101 confirms assignment
	if !rider.ConfirmAssign(101) {
		t.Fatalf("expected ConfirmAssign(101) to succeed")
	}
	if rider.State() != StateAssigned {
		t.Fatalf("expected state Assigned, got %d", rider.State())
	}

	// 6. Release rider
	rider.Release()
	if rider.State() != StateIdle || rider.AssignedOrderID() != 0 {
		t.Fatalf("expected state Idle after release, got state=%d order=%d", rider.State(), rider.AssignedOrderID())
	}
}

func TestRider_ConcurrentRace(t *testing.T) {
	rider := NewRider("rider_race_test")
	numRacers := 100

	var wg sync.WaitGroup
	wg.Add(numRacers)

	var winCount uint64
	var loseCount uint64

	startBarrier := make(chan struct{})

	for i := 1; i <= numRacers; i++ {
		go func(orderID uint64) {
			defer wg.Done()
			<-startBarrier // All 100 goroutines fire at the exact same microsecond

			if rider.TryOffer(orderID) {
				atomic.AddUint64(&winCount, 1)
			} else {
				atomic.AddUint64(&loseCount, 1)
			}
		}(uint64(i))
	}

	close(startBarrier) // Fire the starter pistol!
	wg.Wait()

	if winCount != 1 {
		t.Fatalf("expected exactly 1 winner, got %d", winCount)
	}
	if loseCount != uint64(numRacers-1) {
		t.Fatalf("expected %d losers, got %d", numRacers-1, loseCount)
	}
}

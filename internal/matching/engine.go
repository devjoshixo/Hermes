package matching

import (
	"errors"
	"sync"
	"time"

	"github.com/devjoshixo/hermes/internal/spatial"
	"github.com/uber/h3-go/v3"
)

var (
	// ErrNoRidersAvailable is returned when no idle riders are found within search radius.
	ErrNoRidersAvailable = errors.New("no riders available nearby")
	// ErrRiderNotFound is returned when a rider ID is not registered.
	ErrRiderNotFound = errors.New("rider not found")
)

// orderFlight tracks an in-flight or completed order matching operation.
type orderFlight struct {
	done  chan struct{}
	rider *Rider
	err   error
}

// activeOffer tracks an offered order lease with its deadline timer and candidate cascade list.
type activeOffer struct {
	timer      *time.Timer
	order      Order
	riderID    string
	candidates []string
	candIndex  int
	maxK       int
	limit      int
}

// Engine coordinates spatial queries, atomic rider dispatch, and lease lifecycle.
type Engine struct {
	spatialIndex *spatial.SpatialIndex
	ridersMu     sync.RWMutex
	riders       map[string]*Rider
	offerTimeout time.Duration

	ordersMu sync.Mutex
	orders   map[uint64]*orderFlight

	offersMu sync.Mutex
	offers   map[uint64]*activeOffer
}

// NewEngine initializes a matching engine.
func NewEngine(spatialIndex *spatial.SpatialIndex, offerTimeout time.Duration) *Engine {
	if offerTimeout <= 0 {
		offerTimeout = 15 * time.Second
	}
	return &Engine{
		spatialIndex: spatialIndex,
		riders:       make(map[string]*Rider),
		offerTimeout: offerTimeout,
		orders:       make(map[uint64]*orderFlight),
		offers:       make(map[uint64]*activeOffer),
	}
}

// RegisterRider adds a rider to the engine registry.
func (e *Engine) RegisterRider(rider *Rider) {
	e.ridersMu.Lock()
	e.riders[rider.ID] = rider
	e.ridersMu.Unlock()
}

// UnregisterRider removes a rider from the engine registry.
func (e *Engine) UnregisterRider(riderID string) {
	e.ridersMu.Lock()
	delete(e.riders, riderID)
	e.ridersMu.Unlock()
}

// GetRider looks up a rider by ID safely under read lock.
func (e *Engine) GetRider(riderID string) *Rider {
	e.ridersMu.RLock()
	defer e.ridersMu.RUnlock()
	return e.riders[riderID]
}

// MatchOrder searches nearby H3 rings and atomically offers the order to the first idle candidate.
// It includes singleflight idempotency: duplicate requests coalesce into one matching operation.
func (e *Engine) MatchOrder(order Order, maxK int, limit int) (*Rider, error) {
	if maxK < 0 || limit <= 0 {
		return nil, ErrNoRidersAvailable
	}

	// 1. Idempotency & In-Flight Coalescing
	e.ordersMu.Lock()
	if flight, exists := e.orders[order.ID]; exists {
		e.ordersMu.Unlock()
		<-flight.done // Wait for in-flight match to complete
		return flight.rider, flight.err
	}

	flight := &orderFlight{done: make(chan struct{})}
	e.orders[order.ID] = flight
	e.ordersMu.Unlock()

	// 2. Perform candidate search & CAS offer with active lease timer
	matchedRider, err := e.searchAndOffer(order, maxK, limit)

	// 3. Record outcome and wake any waiting duplicate requests
	flight.rider = matchedRider
	flight.err = err
	close(flight.done)

	// 4. Failure eviction: do not freeze failures into memory so future retries can succeed
	if err != nil {
		e.ordersMu.Lock()
		delete(e.orders, order.ID)
		e.ordersMu.Unlock()
	}

	return matchedRider, err
}

func (e *Engine) searchAndOffer(order Order, maxK int, limit int) (*Rider, error) {
	coord := h3.GeoCoord{
		Latitude:  order.PickupLat,
		Longitude: order.PickupLng,
	}
	pickupCell := uint64(h3.FromGeo(coord, 8))

	candidates := e.spatialIndex.FindNearby(pickupCell, maxK, limit)
	if len(candidates) == 0 {
		return nil, ErrNoRidersAvailable
	}

	return e.tryOfferCandidates(order, candidates, 0, maxK, limit)
}

func (e *Engine) tryOfferCandidates(order Order, candidates []string, startIndex int, maxK int, limit int) (*Rider, error) {
	for i := startIndex; i < len(candidates); i++ {
		riderID := candidates[i]
		rider := e.GetRider(riderID)
		if rider == nil {
			continue
		}

		// Atomic CAS: StateIdle -> StateOffered
		if rider.TryOffer(order.ID) {
			// Arm the active lease reaper timer
			e.armOfferLease(order, rider.ID, candidates, i, maxK, limit)
			return rider, nil
		}
	}

	return nil, ErrNoRidersAvailable
}

func (e *Engine) armOfferLease(order Order, riderID string, candidates []string, candIndex int, maxK int, limit int) {
	e.offersMu.Lock()
	defer e.offersMu.Unlock()

	// Stop any pre-existing timer for this order
	if existing, exists := e.offers[order.ID]; exists && existing.timer != nil {
		existing.timer.Stop()
	}

	offer := &activeOffer{
		order:      order,
		riderID:    riderID,
		candidates: candidates,
		candIndex:  candIndex,
		maxK:       maxK,
		limit:      limit,
	}

	// Active deadline reaper
	offer.timer = time.AfterFunc(e.offerTimeout, func() {
		e.handleOfferTimeout(order.ID, riderID)
	})

	e.offers[order.ID] = offer
}

// handleOfferTimeout releases the timed-out rider and cascades to the next candidate.
func (e *Engine) handleOfferTimeout(orderID uint64, riderID string) {
	e.offersMu.Lock()
	offer, exists := e.offers[orderID]
	if !exists || offer.riderID != riderID {
		e.offersMu.Unlock()
		return
	}
	delete(e.offers, orderID)
	e.offersMu.Unlock()

	// 1. Release the timed-out rider back to StateIdle
	rider := e.GetRider(riderID)
	if rider != nil && rider.State() == StateOffered {
		rider.Release()
	}

	// 2. Cascade to the next candidate
	_, _ = e.tryOfferCandidates(offer.order, offer.candidates, offer.candIndex+1, offer.maxK, offer.limit)
}

// AcceptOrder records rider acceptance, transitioning StateOffered -> StateAssigned and disarming the reaper.
func (e *Engine) AcceptOrder(riderID string, orderID uint64) bool {
	rider := e.GetRider(riderID)
	if rider == nil {
		return false
	}

	if !rider.ConfirmAssign(orderID) {
		return false
	}

	// Disarm active lease reaper
	e.offersMu.Lock()
	if offer, exists := e.offers[orderID]; exists && offer.riderID == riderID {
		if offer.timer != nil {
			offer.timer.Stop()
		}
		delete(e.offers, orderID)
	}
	e.offersMu.Unlock()

	return true
}

// RejectOrder explicitly declines an offer, releasing the rider and cascading in 0ms without waiting for timeout.
func (e *Engine) RejectOrder(riderID string, orderID uint64) bool {
	e.offersMu.Lock()
	offer, exists := e.offers[orderID]
	if !exists || offer.riderID != riderID {
		e.offersMu.Unlock()
		return false
	}
	if offer.timer != nil {
		offer.timer.Stop()
	}
	delete(e.offers, orderID)
	e.offersMu.Unlock()

	// 1. Release rider back to StateIdle immediately
	rider := e.GetRider(riderID)
	if rider != nil {
		rider.Release()
	}

	// 2. Instant 0ms cascade to next candidate
	_, _ = e.tryOfferCandidates(offer.order, offer.candidates, offer.candIndex+1, offer.maxK, offer.limit)
	return true
}

// ClearOrder removes an order from the idempotency registry once delivered or cancelled.
func (e *Engine) ClearOrder(orderID uint64) {
	e.ordersMu.Lock()
	delete(e.orders, orderID)
	e.ordersMu.Unlock()
}

// ReleaseRider resets a rider back to StateIdle if offer expires or delivery completes.
func (e *Engine) ReleaseRider(riderID string) {
	rider := e.GetRider(riderID)
	if rider != nil {
		rider.Release()
	}
}

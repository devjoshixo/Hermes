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
	// ErrOrderNotFound is returned when an order ID is not present in the order registry.
	ErrOrderNotFound = errors.New("order not found")
)

// Order status constants tracking the lifecycle of an order.
const (
	OrderStatusSearching = "SEARCHING"
	OrderStatusOffered   = "OFFERED"
	OrderStatusAssigned  = "ASSIGNED"
	OrderStatusExhausted = "EXHAUSTED"
)

// orderFlight tracks an in-flight or completed order matching operation.
type orderFlight struct {
	status  string
	riderID string
	done    chan struct{}
	rider   *Rider
	err     error
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

// MatchOrderAsync registers the order synchronously in OrderStatusSearching and runs the candidate
// search and atomic offer asynchronously in the background.
func (e *Engine) MatchOrderAsync(order Order, maxK int, limit int) *orderFlight {
	if maxK < 0 || limit <= 0 {
		flight := &orderFlight{
			status: OrderStatusExhausted,
			done:   make(chan struct{}),
			err:    ErrNoRidersAvailable,
		}
		close(flight.done)
		return flight
	}

	// 1. Idempotency & In-Flight Coalescing
	e.ordersMu.Lock()
	if flight, exists := e.orders[order.ID]; exists {
		// If previously exhausted, allow retry by resetting the flight
		if flight.status != OrderStatusExhausted {
			e.ordersMu.Unlock()
			return flight
		}
	}

	flight := &orderFlight{
		status: OrderStatusSearching,
		done:   make(chan struct{}),
	}
	e.orders[order.ID] = flight
	e.ordersMu.Unlock()

	// 2. Perform candidate search & CAS offer asynchronously
	go func() {
		matchedRider, err := e.searchAndOffer(order, maxK, limit)

		e.ordersMu.Lock()
		flight.rider = matchedRider
		flight.err = err
		if err != nil {
			flight.status = OrderStatusExhausted
			flight.riderID = ""
		} else {
			flight.status = OrderStatusOffered
			flight.riderID = matchedRider.ID
		}
		close(flight.done)
		e.ordersMu.Unlock()
	}()

	return flight
}

// MatchOrder searches nearby H3 rings and atomically offers the order to the first idle candidate.
// It delegates to MatchOrderAsync and waits for the in-flight matching operation to complete.
func (e *Engine) MatchOrder(order Order, maxK int, limit int) (*Rider, error) {
	flight := e.MatchOrderAsync(order, maxK, limit)
	<-flight.done
	e.ordersMu.Lock()
	defer e.ordersMu.Unlock()
	return flight.rider, flight.err
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
	nextRider, err := e.tryOfferCandidates(offer.order, offer.candidates, offer.candIndex+1, offer.maxK, offer.limit)
	e.ordersMu.Lock()
	if flight, exists := e.orders[orderID]; exists {
		if err != nil {
			flight.status = OrderStatusExhausted
			flight.riderID = ""
			flight.rider = nil
		} else {
			flight.status = OrderStatusOffered
			flight.riderID = nextRider.ID
			flight.rider = nextRider
		}
	}
	e.ordersMu.Unlock()
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

	// Update order flight state
	e.ordersMu.Lock()
	if flight, exists := e.orders[orderID]; exists {
		flight.status = OrderStatusAssigned
		flight.riderID = riderID
		flight.rider = rider
	}
	e.ordersMu.Unlock()

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
	nextRider, err := e.tryOfferCandidates(offer.order, offer.candidates, offer.candIndex+1, offer.maxK, offer.limit)
	e.ordersMu.Lock()
	if flight, exists := e.orders[orderID]; exists {
		if err != nil {
			flight.status = OrderStatusExhausted
			flight.riderID = ""
			flight.rider = nil
		} else {
			flight.status = OrderStatusOffered
			flight.riderID = nextRider.ID
			flight.rider = nextRider
		}
	}
	e.ordersMu.Unlock()

	return true
}

// GetOrderStatus returns the current dispatch status and assigned/offered rider ID for an order.
func (e *Engine) GetOrderStatus(orderID uint64) (string, string, error) {
	e.ordersMu.Lock()
	defer e.ordersMu.Unlock()

	flight, exists := e.orders[orderID]
	if !exists {
		return "", "", ErrOrderNotFound
	}
	return flight.status, flight.riderID, nil
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

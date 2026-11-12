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
// Concurrent duplicates wait on the done channel, receiving the identical outcome.
type orderFlight struct {
	done  chan struct{}
	rider *Rider
	err   error
}

// Engine coordinates spatial queries and atomic rider dispatch.
type Engine struct {
	spatialIndex *spatial.SpatialIndex
	ridersMu     sync.RWMutex
	riders       map[string]*Rider
	offerTimeout time.Duration

	ordersMu sync.Mutex
	orders   map[uint64]*orderFlight
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
// It includes singleflight idempotency: concurrent duplicate requests for the same order ID
// coalesce into a single matching operation, guaranteeing exactly one rider allocation.
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

	// 2. Perform candidate search & CAS offer
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

	for _, riderID := range candidates {
		rider := e.GetRider(riderID)
		if rider == nil {
			continue
		}

		// Atomic CAS: StateIdle -> StateOffered
		if rider.TryOffer(order.ID) {
			return rider, nil
		}
		// If TryOffer returned false, rider was taken by another order or is busy.
		// Cascades automatically to the next candidate!
	}

	return nil, ErrNoRidersAvailable
}

// ClearOrder removes an order from the idempotency registry once delivered or cancelled.
func (e *Engine) ClearOrder(orderID uint64) {
	e.ordersMu.Lock()
	delete(e.orders, orderID)
	e.ordersMu.Unlock()
}

// AcceptOrder records rider acceptance, transitioning StateOffered -> StateAssigned.
func (e *Engine) AcceptOrder(riderID string, orderID uint64) bool {
	rider := e.GetRider(riderID)
	if rider == nil {
		return false
	}
	return rider.ConfirmAssign(orderID)
}

// ReleaseRider resets a rider back to StateIdle if offer expires or delivery completes.
func (e *Engine) ReleaseRider(riderID string) {
	rider := e.GetRider(riderID)
	if rider != nil {
		rider.Release()
	}
}

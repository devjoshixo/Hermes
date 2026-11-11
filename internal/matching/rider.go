package matching

import "sync/atomic"

// Rider state machine constants.
const (
	StateIdle     uint32 = 0 // Available for dispatch
	StateOffered  uint32 = 1 // Phone ringing with order offer (15s lease)
	StateAssigned uint32 = 2 // Order accepted, actively delivering
)

// Rider represents an agent in the dispatch system. 100% lockless.
type Rider struct {
	ID              string
	state           uint32 // Atomic: StateIdle, StateOffered, StateAssigned
	assignedOrderID uint64 // Atomic: 0 if unassigned, else numeric order ID
}

// NewRider initializes a rider in the StateIdle state.
func NewRider(id string) *Rider {
	return &Rider{
		ID:    id,
		state: StateIdle,
	}
}

// State returns the current atomic state of the rider.
func (r *Rider) State() uint32 {
	return atomic.LoadUint32(&r.state)
}

// AssignedOrderID returns the numeric order ID currently attached to this rider.
func (r *Rider) AssignedOrderID() uint64 {
	return atomic.LoadUint64(&r.assignedOrderID)
}

// TryOffer attempts an atomic CAS from StateIdle -> StateOffered.
// If won, atomically records the order ID and returns true.
func (r *Rider) TryOffer(orderID uint64) bool {
	if !atomic.CompareAndSwapUint32(&r.state, StateIdle, StateOffered) {
		return false
	}
	atomic.StoreUint64(&r.assignedOrderID, orderID)
	return true
}

// ConfirmAssign transitions the rider from StateOffered -> StateAssigned
// once the rider clicks 'Accept' on their phone.
func (r *Rider) ConfirmAssign(orderID uint64) bool {
	if atomic.LoadUint64(&r.assignedOrderID) != orderID {
		return false
	}
	return atomic.CompareAndSwapUint32(&r.state, StateOffered, StateAssigned)
}

// Release resets the rider back to StateIdle if an offer expires,
// is rejected, or the delivery finishes.
func (r *Rider) Release() {
	atomic.StoreUint64(&r.assignedOrderID, 0)
	atomic.StoreUint32(&r.state, StateIdle)
}

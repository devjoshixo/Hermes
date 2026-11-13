package spatial

import (
	"sync"

	"github.com/uber/h3-go/v3"
)

// CellSet represents an unordered set of rider IDs in a single H3 cell.
type CellSet map[string]struct{}

// Shard guards a subset of H3 cells with its own independent RWMutex.
type Shard struct {
	mu    sync.RWMutex
	cells map[uint64]CellSet // H3 CellID -> Set of Rider IDs
}

// riderLocation stores the rider's latest known H3 cell and timestamp.
type riderLocation struct {
	cell      uint64
	timestamp int64
}

// SpatialIndex is a lock-striped in-memory spatial index.
type SpatialIndex struct {
	numShards uint64
	mask      uint64
	shards    []*Shard

	// riderMu guards riderToLocation to track each rider's current location and timestamp in O(1)
	riderMu         sync.RWMutex
	riderToLocation map[string]riderLocation // RiderID -> Current H3 CellID & Timestamp
}

// NewSpatialIndex initializes a spatial index with a power-of-2 number of shards (default 64).
func NewSpatialIndex(numShards int) *SpatialIndex {
	if numShards <= 0 || (numShards&(numShards-1)) != 0 {
		numShards = 64 // enforce power-of-2 for 1-cycle bitwise masking
	}

	n := uint64(numShards)
	idx := &SpatialIndex{
		numShards:       n,
		mask:            n - 1,
		shards:          make([]*Shard, n),
		riderToLocation: make(map[string]riderLocation),
	}

	for i := uint64(0); i < n; i++ {
		idx.shards[i] = &Shard{
			cells: make(map[uint64]CellSet),
		}
	}

	return idx
}

// getShard returns the shard responsible for a given H3 cell using bitwise masking.
func (idx *SpatialIndex) getShard(cell uint64) *Shard {
	// Bitwise AND replaces expensive modulo operator (%)
	return idx.shards[cell&idx.mask]
}

// GetRidersInCell returns a slice of all rider IDs currently in cell.
func (idx *SpatialIndex) GetRidersInCell(cell uint64) []string {
	shard := idx.getShard(cell)
	shard.mu.RLock()
	defer shard.mu.RUnlock()

	set, exists := shard.cells[cell]
	if !exists || len(set) == 0 {
		return nil
	}

	riders := make([]string, 0, len(set))
	for rID := range set {
		riders = append(riders, rID)
	}
	return riders
}

// FindNearby searches concentric rings (k=0, 1, 2... maxK) around originCell
// until it gathers at least 'limit' riders or exhausts maxK rings.
func (idx *SpatialIndex) FindNearby(originCell uint64, maxK int, limit int) []string {
	if limit <= 0 {
		return nil
	}

	seen := make(map[string]struct{})
	var result []string

	for k := 0; k <= maxK; k++ {
		// h3.KRing returns all cells up to distance k.
		// For ring k, we query the grid disk.
		ringCells := h3.KRing(h3.H3Index(originCell), k)
		for _, c := range ringCells {
			cellID := uint64(c)
			riders := idx.GetRidersInCell(cellID)
			for _, rID := range riders {
				if _, already := seen[rID]; !already {
					seen[rID] = struct{}{}
					result = append(result, rID)
					if len(result) >= limit {
						return result
					}
				}
			}
		}
	}

	return result
}

// UpdatePosition atomically moves a rider from their old cell to newCell.
// CONTRACT:
// 1. Must satisfy Invariant 1: No Ghost Duplicates (atomic transfer across shards).
// 2. Must satisfy Invariant 2: No Void Vanishing.
// 3. Stale pings (timestamp <= latest known timestamp) are dropped immediately.
// 4. If oldCell == newCell, only the timestamp is updated (0 shard work).
func (idx *SpatialIndex) UpdatePosition(riderID string, newCell uint64, timestamp int64) bool {
	idx.riderMu.Lock()
	defer idx.riderMu.Unlock()

	loc, exists := idx.riderToLocation[riderID]

	// Case 1: Out-of-order stale ping check (Timestamp Fencing)
	if exists && timestamp <= loc.timestamp {
		return false // Stale ping dropped
	}

	// Case 2: Rider stayed in the exact same hexagon -> update timestamp only
	if exists && loc.cell == newCell {
		idx.riderToLocation[riderID] = riderLocation{cell: newCell, timestamp: timestamp}
		return true
	}

	// Case 3: Remove rider from old shard
	if exists {
		oldShard := idx.getShard(loc.cell)
		oldShard.mu.Lock()
		if set, ok := oldShard.cells[loc.cell]; ok {
			delete(set, riderID)
		}
		oldShard.mu.Unlock()
	}

	// Case 4: Put rider into new shard
	newShard := idx.getShard(newCell)
	newShard.mu.Lock()
	if newShard.cells[newCell] == nil {
		newShard.cells[newCell] = make(CellSet)
	}
	newShard.cells[newCell][riderID] = struct{}{}
	newShard.mu.Unlock()

	// Case 5: Update notebook
	idx.riderToLocation[riderID] = riderLocation{cell: newCell, timestamp: timestamp}
	return true
}

// Remove completely deletes a rider from the index when they disconnect.
func (idx *SpatialIndex) Remove(riderID string) {
	idx.riderMu.Lock()
	defer idx.riderMu.Unlock()

	loc, exists := idx.riderToLocation[riderID]
	if !exists {
		return
	}
	delete(idx.riderToLocation, riderID)

	oldShard := idx.getShard(loc.cell)
	oldShard.mu.Lock()
	if set, ok := oldShard.cells[loc.cell]; ok {
		delete(set, riderID)
	}
	oldShard.mu.Unlock()
}

// GetRiderCell returns the current cell of the rider if present.
func (idx *SpatialIndex) GetRiderCell(riderID string) (uint64, bool) {
	idx.riderMu.RLock()
	defer idx.riderMu.RUnlock()
	loc, exists := idx.riderToLocation[riderID]
	return loc.cell, exists
}

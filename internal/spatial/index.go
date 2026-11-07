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

// SpatialIndex is a lock-striped in-memory spatial index.
type SpatialIndex struct {
	numShards uint64
	mask      uint64
	shards    []*Shard

	// riderMu guards riderToCell to track each rider's current location in O(1)
	riderMu     sync.RWMutex
	riderToCell map[string]uint64 // RiderID -> Current H3 CellID
}

// NewSpatialIndex initializes a spatial index with a power-of-2 number of shards (default 64).
func NewSpatialIndex(numShards int) *SpatialIndex {
	if numShards <= 0 || (numShards&(numShards-1)) != 0 {
		numShards = 64 // enforce power-of-2 for 1-cycle bitwise masking
	}

	n := uint64(numShards)
	idx := &SpatialIndex{
		numShards:   n,
		mask:        n - 1,
		shards:      make([]*Shard, n),
		riderToCell: make(map[string]uint64),
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
// 1. Must satisfy Invariant 1: No Ghost Duplicates.
// 2. Must satisfy Invariant 2: No Void Vanishing.
// 3. If oldCell == newCell, it is a no-op (return early).
func (idx *SpatialIndex) UpdatePosition(riderID string, newCell uint64) {
	idx.riderMu.Lock()
	oldCell, exists := idx.riderToCell[riderID]

	// Case 1: Rider stayed in the exact same hexagon -> 0 work to do
	if exists && oldCell == newCell {
		idx.riderMu.Unlock()
		return
	}

	// Update the notebook
	idx.riderToCell[riderID] = newCell
	idx.riderMu.Unlock()

	// Case 2: Rider was previously in an old cell -> remove them from old shard
	if exists {
		oldShard := idx.getShard(oldCell)
		oldShard.mu.Lock()
		if set, ok := oldShard.cells[oldCell]; ok {
			delete(set, riderID)
		}
		oldShard.mu.Unlock()
	}

	// Case 3: Put rider into the new cell's shard
	newShard := idx.getShard(newCell)
	newShard.mu.Lock()
	if newShard.cells[newCell] == nil {
		newShard.cells[newCell] = make(CellSet)
	}
	newShard.cells[newCell][riderID] = struct{}{}
	newShard.mu.Unlock()
}

// Remove completely deletes a rider from the index when they disconnect.
func (idx *SpatialIndex) Remove(riderID string) {
	idx.riderMu.Lock()
	oldCell, exists := idx.riderToCell[riderID]
	if !exists {
		idx.riderMu.Unlock()
		return
	}
	delete(idx.riderToCell, riderID)
	idx.riderMu.Unlock()

	oldShard := idx.getShard(oldCell)
	oldShard.mu.Lock()
	if set, ok := oldShard.cells[oldCell]; ok {
		delete(set, riderID)
	}
	oldShard.mu.Unlock()
}

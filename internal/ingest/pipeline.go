package ingest

import (
	"sync"
	"sync/atomic"

	"github.com/devjoshixo/hermes/internal/spatial"
	"github.com/uber/h3-go/v3"
)

// Pipeline manages the ingestion of GPS pings, decoupling network intake
// from spatial index updates using a buffered ring-buffer channel and worker pool.
type Pipeline struct {
	index      *spatial.SpatialIndex
	pingCh     chan LocationPing
	numWorkers int
	resolution int
	stats      PipelineStats

	mu       sync.RWMutex
	isClosed bool
	wg       sync.WaitGroup
}

// NewPipeline creates an ingestion pipeline with a fixed buffer size and worker count.
// Default resolution 8 corresponds to an average hexagon area of ~0.737 sq km (edge ~460m).
func NewPipeline(index *spatial.SpatialIndex, bufferSize int, numWorkers int, resolution int) *Pipeline {
	if numWorkers <= 0 {
		numWorkers = 4
	}
	if bufferSize <= 0 {
		bufferSize = 1000
	}
	if resolution <= 0 {
		resolution = 8
	}

	p := &Pipeline{
		index:      index,
		pingCh:     make(chan LocationPing, bufferSize),
		numWorkers: numWorkers,
		resolution: resolution,
	}

	p.start()
	return p
}

// start launches the worker pool.
func (p *Pipeline) start() {
	p.wg.Add(p.numWorkers)
	for i := 0; i < p.numWorkers; i++ {
		go p.worker()
	}
}

// worker continuously drains pings from the channel, computes the H3 cell,
// and updates the sharded spatial index.
func (p *Pipeline) worker() {
	defer p.wg.Done()

	for ping := range p.pingCh {
		coord := h3.GeoCoord{
			Latitude:  ping.Lat,
			Longitude: ping.Lng,
		}
		cell := uint64(h3.FromGeo(coord, p.resolution))

		p.index.UpdatePosition(ping.RiderID, cell)
		atomic.AddUint64(&p.stats.processed, 1)
	}
}

// Submit attempts to enqueue a location ping.
// If the buffer is full, it immediately drops the ping and increments the dropped metric
// (backpressure defense against bufferbloat).
// Returns true if enqueued, false if dropped or if the pipeline is stopped.
func (p *Pipeline) Submit(ping LocationPing) bool {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if p.isClosed {
		return false
	}

	select {
	case p.pingCh <- ping:
		return true
	default:
		atomic.AddUint64(&p.stats.dropped, 1)
		return false
	}
}

// Stats returns the current telemetry of the pipeline.
func (p *Pipeline) Stats() (processed uint64, dropped uint64) {
	return p.stats.Processed(), p.stats.Dropped()
}

// Stop gracefully shuts down the pipeline: closes the channel, waits for all
// workers to drain queued pings, and guarantees zero sends on closed channel.
func (p *Pipeline) Stop() {
	p.mu.Lock()
	if p.isClosed {
		p.mu.Unlock()
		return
	}
	p.isClosed = true
	close(p.pingCh)
	p.mu.Unlock()

	p.wg.Wait()
}

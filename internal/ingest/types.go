package ingest

import "sync/atomic"

// LocationPing represents a single GPS telemetry update from a rider's device.
type LocationPing struct {
	RiderID   string  `json:"rider_id"`
	Lat       float64 `json:"lat"`
	Lng       float64 `json:"lng"`
	Timestamp int64   `json:"timestamp"` // Unix timestamp in milliseconds
}

// PipelineStats tracks ingestion metrics using atomic counters.
type PipelineStats struct {
	processed uint64
	dropped   uint64
}

// Processed returns the total number of pings successfully updated in the spatial index.
func (s *PipelineStats) Processed() uint64 {
	return atomic.LoadUint64(&s.processed)
}

// Dropped returns the total number of pings dropped due to buffer saturation (backpressure).
func (s *PipelineStats) Dropped() uint64 {
	return atomic.LoadUint64(&s.dropped)
}

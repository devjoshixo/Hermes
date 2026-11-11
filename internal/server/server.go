package server

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/devjoshixo/hermes/internal/ingest"
	"github.com/devjoshixo/hermes/internal/matching"
	"github.com/devjoshixo/hermes/internal/spatial"
)

// Server encapsulates the HTTP and WebSocket endpoints, tying spatial indexing,
// GPS ingestion, and atomic matching into a unified service.
type Server struct {
	router       *http.ServeMux
	spatialIndex *spatial.SpatialIndex
	pipeline     *ingest.Pipeline
	wsHandler    *ingest.WebSocketHandler
	engine       *matching.Engine
}

// NewServer initializes the server and registers all routes.
func NewServer(
	spatialIndex *spatial.SpatialIndex,
	pipeline *ingest.Pipeline,
	engine *matching.Engine,
) *Server {
	s := &Server{
		router:       http.NewServeMux(),
		spatialIndex: spatialIndex,
		pipeline:     pipeline,
		wsHandler:    ingest.NewWebSocketHandler(pipeline),
		engine:       engine,
	}
	s.routes()
	return s
}

// ServeHTTP implements http.Handler for easy integration with standard library servers.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.router.ServeHTTP(w, r)
}

func (s *Server) routes() {
	// Rider WebSocket GPS ingestion
	s.router.HandleFunc("GET /ws/rider/{rider_id}", s.handleRiderWS)

	// Order dispatch endpoints
	s.router.HandleFunc("POST /api/v1/orders", s.handleCreateOrder)
	s.router.HandleFunc("POST /api/v1/orders/{order_id}/accept", s.handleAcceptOrder)

	// Health and telemetry
	s.router.HandleFunc("GET /healthz", s.handleHealthz)
}

// handleRiderWS upgrades the rider connection, registers them in the matching engine,
// and streams live GPS telemetry into the ingestion pipeline.
func (s *Server) handleRiderWS(w http.ResponseWriter, r *http.Request) {
	riderID := r.PathValue("rider_id")
	if riderID == "" {
		http.Error(w, "missing rider_id", http.StatusBadRequest)
		return
	}

	// Register rider as active and available
	rider := matching.NewRider(riderID)
	s.engine.RegisterRider(rider)

	defer func() {
		// Clean up on disconnect: unregister from engine and spatial index
		s.engine.UnregisterRider(riderID)
		s.spatialIndex.Remove(riderID)
	}()

	// Delegate to M2 WebSocket handler for continuous GPS streaming
	s.wsHandler.HandleRiderWS(w, r)
}

// CreateOrderRequest represents customer order payload.
type CreateOrderRequest struct {
	OrderID   uint64  `json:"order_id"`
	PickupLat float64 `json:"pickup_lat"`
	PickupLng float64 `json:"pickup_lng"`
	MaxK      int     `json:"max_k,omitempty"`
	Limit     int     `json:"limit,omitempty"`
}

// CreateOrderResponse returns matched rider details.
type CreateOrderResponse struct {
	OrderID uint64 `json:"order_id"`
	RiderID string `json:"rider_id"`
	Status  string `json:"status"`
}

// handleCreateOrder processes an incoming order and attempts atomic dispatch.
func (s *Server) handleCreateOrder(w http.ResponseWriter, r *http.Request) {
	var req CreateOrderRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}

	maxK := req.MaxK
	if maxK <= 0 {
		maxK = 2
	}
	limit := req.Limit
	if limit <= 0 {
		limit = 10
	}

	order := matching.Order{
		ID:        req.OrderID,
		PickupLat: req.PickupLat,
		PickupLng: req.PickupLng,
	}

	matchedRider, err := s.engine.MatchOrder(order, maxK, limit)
	if err != nil {
		if errors.Is(err, matching.ErrNoRidersAvailable) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusNotFound)
			_ = json.NewEncoder(w).Encode(map[string]string{
				"error": "no riders available nearby",
			})
			return
		}
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(CreateOrderResponse{
		OrderID: req.OrderID,
		RiderID: matchedRider.ID,
		Status:  "OFFERED",
	})
}

// handleAcceptOrder handles rider order acceptance.
func (s *Server) handleAcceptOrder(w http.ResponseWriter, r *http.Request) {
	orderIDStr := r.PathValue("order_id")
	orderID, err := strconv.ParseUint(orderIDStr, 10, 64)
	if err != nil {
		http.Error(w, "invalid order_id", http.StatusBadRequest)
		return
	}

	riderID := r.URL.Query().Get("rider_id")
	if riderID == "" {
		var body struct {
			RiderID string `json:"rider_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err == nil {
			riderID = body.RiderID
		}
	}

	if riderID == "" {
		http.Error(w, "missing rider_id", http.StatusBadRequest)
		return
	}

	if !s.engine.AcceptOrder(riderID, orderID) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusConflict)
		_ = json.NewEncoder(w).Encode(map[string]string{
			"error": "unable to accept order: state not offered or order mismatch",
		})
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"order_id": orderID,
		"rider_id": riderID,
		"status":   "ASSIGNED",
	})
}

// handleHealthz returns operational stats.
func (s *Server) handleHealthz(w http.ResponseWriter, r *http.Request) {
	processed, dropped := s.pipeline.Stats()
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"status":          "ok",
		"timestamp":       time.Now().Unix(),
		"pings_processed": processed,
		"pings_dropped":   dropped,
	})
}

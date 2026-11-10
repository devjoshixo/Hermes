package ingest

import (
	"net/http"
	"time"

	"github.com/gorilla/websocket"
)

// WebSocketHandler manages live rider WebSocket connections and pipes incoming
// GPS telemetry into the decoupled ingestion pipeline.
type WebSocketHandler struct {
	pipeline *Pipeline
	upgrader websocket.Upgrader
}

// NewWebSocketHandler creates a handler wired to an ingestion pipeline.
// Default socket buffers are tuned to 512 bytes (optimized for tiny ~50-byte GPS pings
// across thousands of persistent connections).
func NewWebSocketHandler(pipeline *Pipeline) *WebSocketHandler {
	return &WebSocketHandler{
		pipeline: pipeline,
		upgrader: websocket.Upgrader{
			CheckOrigin: func(r *http.Request) bool {
				return true // Permissive origin for mobile/client dispatch
			},
			ReadBufferSize:  512,
			WriteBufferSize: 512,
		},
	}
}

// HandleRiderWS upgrades the connection to WebSocket and runs the read loop.
// Route: GET /ws/rider/{rider_id}
func (h *WebSocketHandler) HandleRiderWS(w http.ResponseWriter, r *http.Request) {
	conn, err := h.upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	defer conn.Close()

	riderID := r.PathValue("rider_id")

	for {
		var ping LocationPing
		err := conn.ReadJSON(&ping)
		if err != nil {
			// Rider app closed, disconnected, or socket severed
			break
		}

		if ping.RiderID == "" {
			ping.RiderID = riderID
		}
		if ping.Timestamp == 0 {
			ping.Timestamp = time.Now().UnixMilli()
		}

		// Non-blocking submission to our shock-absorber channel
		h.pipeline.Submit(ping)
	}
}

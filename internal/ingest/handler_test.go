package ingest

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/devjoshixo/hermes/internal/spatial"
	"github.com/gorilla/websocket"
	"github.com/uber/h3-go/v3"
)

func TestWebSocketHandler_DirectHandoff(t *testing.T) {
	index := spatial.NewSpatialIndex(64)
	pipeline := NewPipeline(index, 100, 2, 8)
	defer pipeline.Stop()

	handler := NewWebSocketHandler(pipeline)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /ws/rider/{rider_id}", handler.HandleRiderWS)

	server := httptest.NewServer(mux)
	defer server.Close()

	// 1. Check Initial State: Pipeline must be at 0 processed, 0 dropped
	initProc, initDrop := pipeline.Stats()
	t.Logf("[Phase 1 - Baseline] Initial Stats: Processed=%d, Dropped=%d", initProc, initDrop)
	if initProc != 0 || initDrop != 0 {
		t.Fatalf("expected initial stats 0/0, got %d/%d", initProc, initDrop)
	}

	// 2. Connect client over WebSocket
	wsURL := "ws" + strings.TrimPrefix(server.URL, "http") + "/ws/rider/rider_delhi_42"
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("failed to dial websocket: %v", err)
	}
	defer conn.Close()
	t.Logf("[Phase 2 - Connection] Rider connected to WebSocket: %s", wsURL)

	// Verify state is still 0 after connecting
	procAfterConn, _ := pipeline.Stats()
	if procAfterConn != 0 {
		t.Fatalf("expected 0 pings after connection, got %d", procAfterConn)
	}

	// 3. Send 1 GPS ping
	ping := LocationPing{
		Lat:       28.6315,
		Lng:       77.2167,
		Timestamp: time.Now().UnixMilli(),
	}
	if err := conn.WriteJSON(ping); err != nil {
		t.Fatalf("failed to send ping: %v", err)
	}
	t.Logf("[Phase 3 - Send Ping] Sent GPS ping over socket: Lat=%.4f, Lng=%.4f", ping.Lat, ping.Lng)

	// 4. Await state transition from 0 -> 1
	var transitioned bool
	for i := 0; i < 50; i++ {
		proc, drop := pipeline.Stats()
		if proc == 1 && drop == 0 {
			t.Logf("[Phase 4 - Handoff Verified] Worker drained channel! Processed=%d, Dropped=%d", proc, drop)
			transitioned = true
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	if !transitioned {
		currProc, currDrop := pipeline.Stats()
		t.Fatalf("expected state transition to 1 processed ping, got proc=%d, drop=%d", currProc, currDrop)
	}

	// 5. Clean disconnect
	conn.Close()
	t.Logf("[Phase 5 - Disconnect] Socket closed cleanly without panic.")
}

func TestWebSocketHandler_FullIntegration_M1_M2(t *testing.T) {
	index := spatial.NewSpatialIndex(64)
	pipeline := NewPipeline(index, 100, 4, 8)
	defer pipeline.Stop()

	handler := NewWebSocketHandler(pipeline)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /ws/rider/{rider_id}", handler.HandleRiderWS)

	server := httptest.NewServer(mux)
	defer server.Close()

	baseURL := "ws" + strings.TrimPrefix(server.URL, "http") + "/ws/rider/"

	// Connect Rider A and Rider B
	connA, _, err := websocket.DefaultDialer.Dial(baseURL+"rider_delhi_101", nil)
	if err != nil {
		t.Fatalf("failed to dial Rider A: %v", err)
	}
	defer connA.Close()

	connB, _, err := websocket.DefaultDialer.Dial(baseURL+"rider_delhi_102", nil)
	if err != nil {
		t.Fatalf("failed to dial Rider B: %v", err)
	}
	defer connB.Close()

	cpCoord := h3.GeoCoord{Latitude: 28.6315, Longitude: 77.2167}
	noidaCoord := h3.GeoCoord{Latitude: 28.6280, Longitude: 77.3649}

	cpCell := uint64(h3.FromGeo(cpCoord, 8))
	noidaCell := uint64(h3.FromGeo(noidaCoord, 8))

	// Step 1: Both riders ping from Connaught Place
	t.Logf("[Step 1] Both Rider A and Rider B pinging from Connaught Place (Cell %d)...", cpCell)
	if err := connA.WriteJSON(LocationPing{Lat: cpCoord.Latitude, Lng: cpCoord.Longitude}); err != nil {
		t.Fatalf("failed to send ping A: %v", err)
	}
	if err := connB.WriteJSON(LocationPing{Lat: cpCoord.Latitude, Lng: cpCoord.Longitude}); err != nil {
		t.Fatalf("failed to send ping B: %v", err)
	}

	assertRidersInCell(t, index, cpCell, []string{"rider_delhi_101", "rider_delhi_102"})
	t.Logf("[Step 1 PASSED] Both riders successfully indexed in Connaught Place.")

	// Step 2: Rider A drives to Noida Sector 62
	t.Logf("[Step 2] Rider A moving to Noida (Cell %d)...", noidaCell)
	if err := connA.WriteJSON(LocationPing{Lat: noidaCoord.Latitude, Lng: noidaCoord.Longitude}); err != nil {
		t.Fatalf("failed to send ping A: %v", err)
	}

	assertRidersInCell(t, index, noidaCell, []string{"rider_delhi_101"})
	assertRidersInCell(t, index, cpCell, []string{"rider_delhi_102"}) // No ghost duplicate!
	t.Logf("[Step 2 PASSED] Rider A moved to Noida. Connaught Place retains only Rider B.")

	// Step 3: Radial Search from Connaught Place
	nearbyCP := index.FindNearby(cpCell, 1, 10)
	if len(nearbyCP) != 1 || nearbyCP[0] != "rider_delhi_102" {
		t.Fatalf("expected only Rider B near CP, got: %v", nearbyCP)
	}
	t.Logf("[Step 3 PASSED] Radial search from Connaught Place correctly found only Rider B.")
}

func assertRidersInCell(t *testing.T, index *spatial.SpatialIndex, cell uint64, expectedRiders []string) {
	t.Helper()
	var matched bool
	for attempt := 0; attempt < 50; attempt++ {
		actual := index.GetRidersInCell(cell)
		if len(actual) == len(expectedRiders) {
			matchCount := 0
			for _, exp := range expectedRiders {
				for _, act := range actual {
					if exp == act {
						matchCount++
						break
					}
				}
			}
			if matchCount == len(expectedRiders) {
				matched = true
				break
			}
		}
		time.Sleep(5 * time.Millisecond)
	}

	if !matched {
		actual := index.GetRidersInCell(cell)
		t.Fatalf("cell %d: expected riders %v, got %v", cell, expectedRiders, actual)
	}
}


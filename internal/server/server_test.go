package server

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/devjoshixo/hermes/internal/ingest"
	"github.com/devjoshixo/hermes/internal/matching"
	"github.com/devjoshixo/hermes/internal/spatial"
	"github.com/gorilla/websocket"
)

func TestServer_EndToEnd_Lifecycle(t *testing.T) {
	// Connaught Place coordinates
	cpLat := 28.6315
	cpLng := 77.2167

	// 1. Initialize M1, M2, M3 components
	spatialIndex := spatial.NewSpatialIndex(64)
	pipeline := ingest.NewPipeline(spatialIndex, 1000, 4, 8)
	defer pipeline.Stop()

	engine := matching.NewEngine(spatialIndex, 5*time.Second)
	srv := NewServer(spatialIndex, pipeline, engine)

	// 2. Spin up live HTTP test server
	ts := httptest.NewServer(srv)
	defer ts.Close()

	// 3. Simulated rider connects over WebSocket: GET /ws/rider/rider_live_1
	wsURL := "ws" + strings.TrimPrefix(ts.URL, "http") + "/ws/rider/rider_live_1"
	wsClient, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("failed to dial rider websocket: %v", err)
	}

	// 4. Rider streams GPS telemetry
	ping := ingest.LocationPing{
		RiderID:   "rider_live_1",
		Lat:       cpLat,
		Lng:       cpLng,
		Timestamp: time.Now().UnixMilli(),
	}
	if err := wsClient.WriteJSON(ping); err != nil {
		t.Fatalf("failed to send GPS ping: %v", err)
	}

	// Wait 20ms for worker pool to drain channel and update H3 index
	time.Sleep(20 * time.Millisecond)

	// Invariant 1: Rider is registered in Engine and indexed in SpatialIndex
	r := engine.GetRider("rider_live_1")
	if r == nil {
		t.Fatalf("expected rider_live_1 to be registered in engine")
	}
	if r.State() != matching.StateIdle {
		t.Fatalf("expected state StateIdle (%d), got %d", matching.StateIdle, r.State())
	}

	// 5. Customer places Order #101 at Connaught Place: POST /api/v1/orders
	orderReq := CreateOrderRequest{
		OrderID:   101,
		PickupLat: cpLat,
		PickupLng: cpLng,
		MaxK:      1,
		Limit:     5,
	}
	body, _ := json.Marshal(orderReq)
	resp, err := http.Post(ts.URL+"/api/v1/orders", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("order request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		t.Fatalf("expected 200 OK, got %d: %s", resp.StatusCode, string(respBody))
	}

	var orderResp CreateOrderResponse
	if err := json.NewDecoder(resp.Body).Decode(&orderResp); err != nil {
		t.Fatalf("failed to decode order response: %v", err)
	}

	// Invariant 2: Matched rider is rider_live_1 in OFFERED status
	if orderResp.RiderID != "rider_live_1" {
		t.Fatalf("expected matched rider rider_live_1, got %s", orderResp.RiderID)
	}
	if orderResp.Status != "OFFERED" {
		t.Fatalf("expected status OFFERED, got %s", orderResp.Status)
	}
	if r.State() != matching.StateOffered {
		t.Fatalf("expected rider in StateOffered, got %d", r.State())
	}

	// 6. Rider accepts order: POST /api/v1/orders/101/accept?rider_id=rider_live_1
	acceptURL := fmt.Sprintf("%s/api/v1/orders/101/accept?rider_id=rider_live_1", ts.URL)
	acceptResp, err := http.Post(acceptURL, "application/json", nil)
	if err != nil {
		t.Fatalf("accept request failed: %v", err)
	}
	defer acceptResp.Body.Close()

	if acceptResp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(acceptResp.Body)
		t.Fatalf("expected 200 OK for accept, got %d: %s", acceptResp.StatusCode, string(respBody))
	}

	// Invariant 3: Rider is now ASSIGNED to Order 101
	if r.State() != matching.StateAssigned {
		t.Fatalf("expected rider in StateAssigned (%d), got %d", matching.StateAssigned, r.State())
	}
	if r.AssignedOrderID() != 101 {
		t.Fatalf("expected assigned order ID 101, got %d", r.AssignedOrderID())
	}

	// 7. Rider app disconnects (socket closes)
	wsClient.Close()
	time.Sleep(20 * time.Millisecond) // Allow server goroutine defer cleanup to run

	// Invariant 4: Clean disconnect cleanup
	if engine.GetRider("rider_live_1") != nil {
		t.Fatalf("expected rider_live_1 to be unregistered from engine after disconnect")
	}
}

func TestServer_Concurrent_GPSAndOrders(t *testing.T) {
	cpLat := 28.6315
	cpLng := 77.2167

	spatialIndex := spatial.NewSpatialIndex(64)
	pipeline := ingest.NewPipeline(spatialIndex, 2000, 8, 8)
	defer pipeline.Stop()

	engine := matching.NewEngine(spatialIndex, 5*time.Second)
	srv := NewServer(spatialIndex, pipeline, engine)

	ts := httptest.NewServer(srv)
	defer ts.Close()

	const riderCount = 10
	clients := make([]*websocket.Conn, riderCount)

	// 1. Connect 10 simulated riders over WebSockets and stream initial GPS
	for i := 0; i < riderCount; i++ {
		riderID := fmt.Sprintf("rider_concurrent_%02d", i)
		wsURL := fmt.Sprintf("ws%s/ws/rider/%s", strings.TrimPrefix(ts.URL, "http"), riderID)
		conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
		if err != nil {
			t.Fatalf("failed to connect rider %s: %v", riderID, err)
		}
		clients[i] = conn
		defer conn.Close()

		// Stream initial ping
		ping := ingest.LocationPing{
			RiderID:   riderID,
			Lat:       cpLat,
			Lng:       cpLng,
			Timestamp: time.Now().UnixMilli(),
		}
		if err := conn.WriteJSON(ping); err != nil {
			t.Fatalf("failed to send ping for %s: %v", riderID, err)
		}
	}

	// Allow workers to index all 10 riders
	time.Sleep(30 * time.Millisecond)

	// 2. Concurrently fire continuous GPS movement while firing 10 customer orders
	var (
		wg           sync.WaitGroup
		startGun     = make(chan struct{})
		matchedRiders = make(map[string]bool)
		mu           sync.Mutex
		successCount int64
	)

	// Background GPS movement stream
	for i := 0; i < riderCount; i++ {
		wg.Add(1)
		go func(conn *websocket.Conn, idx int) {
			defer wg.Done()
			<-startGun
			// Stream 5 micro-movements
			for m := 0; m < 5; m++ {
				p := ingest.LocationPing{
					Lat:       cpLat + float64(m)*0.0001,
					Lng:       cpLng + float64(m)*0.0001,
					Timestamp: time.Now().UnixMilli(),
				}
				_ = conn.WriteJSON(p)
				time.Sleep(2 * time.Millisecond)
			}
		}(clients[i], i)
	}

	// 10 concurrent orders hitting POST /api/v1/orders
	for i := 0; i < riderCount; i++ {
		wg.Add(1)
		orderID := uint64(7000 + i)
		go func(oID uint64) {
			defer wg.Done()
			<-startGun

			reqBody, _ := json.Marshal(CreateOrderRequest{
				OrderID:   oID,
				PickupLat: cpLat,
				PickupLng: cpLng,
				MaxK:      1,
				Limit:     riderCount,
			})

			resp, err := http.Post(ts.URL+"/api/v1/orders", "application/json", bytes.NewReader(reqBody))
			if err != nil {
				return
			}
			defer resp.Body.Close()

			if resp.StatusCode == http.StatusOK {
				var rResp CreateOrderResponse
				if err := json.NewDecoder(resp.Body).Decode(&rResp); err == nil {
					mu.Lock()
					matchedRiders[rResp.RiderID] = true
					successCount++
					mu.Unlock()
				}
			}
		}(orderID)
	}

	// Fire start gun!
	close(startGun)
	wg.Wait()

	// Invariant 1: All 10 orders matched
	if successCount != riderCount {
		t.Fatalf("expected %d successful matches, got %d", riderCount, successCount)
	}

	// Invariant 2: 10 unique riders matched (Zero double-booking)
	if len(matchedRiders) != riderCount {
		t.Fatalf("expected %d unique matched riders, got %d", riderCount, len(matchedRiders))
	}
}

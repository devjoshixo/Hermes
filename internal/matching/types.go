package matching

// Order represents an incoming delivery request requiring dispatch.
type Order struct {
	ID        uint64  `json:"order_id"`
	PickupLat float64 `json:"pickup_lat"` // Restaurant latitude
	PickupLng float64 `json:"pickup_lng"` // Restaurant longitude
}

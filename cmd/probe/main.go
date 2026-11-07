package main

import (
	"fmt"
	"github.com/uber/h3-go/v3"
)

func main() {
	// Connaught Place, New Delhi
	coord := h3.GeoCoord{
		Latitude:  28.6315,
		Longitude: 77.2167,
	}

	// Resolution 8 corresponds to an average hexagon area of ~0.737 sq km (edge ~460m)
	resolution := 8
	index := h3.FromGeo(coord, resolution)
	fmt.Printf("Delhi (Connaught Place) H3 Index: %s (uint64: %d)\n", h3.ToString(index), index)

	// Ring 1 (the 6 immediate adjacent hexagons)
	neighbors := h3.KRing(index, 1)
	fmt.Printf("Immediate Neighbors count (including center): %d\n", len(neighbors))
	for i, n := range neighbors {
		fmt.Printf("  [%d] Hexagon: %s (Distance to center: %d)\n", i, h3.ToString(n), h3.DistanceBetween(index, n))
	}
}

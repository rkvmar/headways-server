package main

import (
	"encoding/json"
	"path/filepath"
	"testing"
)

func TestEnrichVehiclePositionsTripInfo(t *testing.T) {
	datafeedsDir = filepath.Join("data", "gtfs")

	payload := []byte(`{"entity":[{"id":"e1","vehicle":{"trip":{"tripId":"UC:0_yyllfs1"},"stopId":""}}]}`)
	out := enrichVehiclePositions(payload)

	var feed map[string]interface{}
	if err := json.Unmarshal(out, &feed); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	entity := feed["entity"].([]interface{})[0].(map[string]interface{})
	vehicle := entity["vehicle"].(map[string]interface{})
	trip := vehicle["trip"].(map[string]interface{})

	if trip["tripInfoFound"] != true {
		t.Fatalf("expected tripInfoFound, got %v", trip["tripInfoFound"])
	}
	if trip["tripHeadsign"] == "" {
		t.Fatalf("expected tripHeadsign to be enriched")
	}
	if trip["shapeId"] == "" {
		t.Fatalf("expected shapeId to be enriched")
	}
}

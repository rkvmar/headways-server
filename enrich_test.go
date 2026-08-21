package main

import (
	"encoding/csv"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestEnrichVehiclePositionsTripInfo(t *testing.T) {
	datafeedsDir = filepath.Join("data", "gtfs")

	// Use a trip that exists in the currently downloaded feed; upstream
	// agencies rotate trip IDs, so hardcoding one rots.
	file, err := os.Open(filepath.Join(datafeedsDir, "trips.txt"))
	if err != nil {
		t.Skipf("no trips.txt available: %v", err)
	}
	defer file.Close()
	reader := csv.NewReader(file)
	reader.FieldsPerRecord = -1
	headers, err := reader.Read()
	if err != nil {
		t.Skipf("empty trips.txt: %v", err)
	}
	tripIDIdx := -1
	for i, h := range headers {
		if h == "trip_id" {
			tripIDIdx = i
		}
	}
	record, err := reader.Read()
	if err != nil || tripIDIdx < 0 || record[tripIDIdx] == "" {
		t.Skipf("no usable trip row: %v", err)
	}
	tripID := record[tripIDIdx]

	payload := []byte(`{"entity":[{"id":"e1","vehicle":{"trip":{"tripId":"` + tripID + `"},"stopId":""}}]}`)
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
	if vehicle["routeShortName"] == "" {
		t.Fatalf("expected routeShortName to be enriched")
	}
}

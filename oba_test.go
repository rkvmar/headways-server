package main

import (
	"encoding/json"
	"testing"
)

func TestBuildVehicleFeed(t *testing.T) {
	env := &obaEnvelope{
		Data: obaData{
			List: []obaVehicle{
				{
					VehicleID: "40_935.1",
					TripID:    "40_T1",
					Location:  &obaPoint{Lat: 47.6, Lon: -122.3},
					TripStatus: &obaTripSt{
						TripID:    "40_T1",
						Deviation: 42,
						Position:  &obaPoint{Lat: 47.61, Lon: -122.31},
						Orienta:   90,
						Closest:   "40_99914",
					},
					OccupStatus: "FEW_SEATS_AVAILABLE",
					LastUpdate:  1710978787000,
				},
				{ // vehicle with no trip should still appear
					VehicleID: "40_200",
					Location:  &obaPoint{Lat: 47.0, Lon: -122.0},
					LastUpdate: 1710978787000,
				},
			},
			References: obaRefs{
				Routes: []obaRoute{
					{ID: "40_100479", ShortName: "1 Line", LongName: "Northgate - Angle Lake"},
				},
				Trips: []obaTrip{
					{ID: "40_T1", RouteID: "40_100479", HeadSign: "Angle Lake", ServiceID: "S1", ShapeID: "SH1", BlockID: "B1", Direction: "0"},
				},
			},
		},
	}

	payload, _ := seattle.buildVehicleFeed(env)
	var feed map[string]interface{}
	if err := json.Unmarshal(payload, &feed); err != nil {
		t.Fatalf("unmarshal built feed: %v", err)
	}
	entities := feed["entity"].([]interface{})
	if len(entities) != 2 {
		t.Fatalf("expected 2 entities, got %d", len(entities))
	}

	// First vehicle: full trip enrichment.
	e0 := entities[0].(map[string]interface{})
	v0 := e0["vehicle"].(map[string]interface{})
	if v0["routeShortName"] != "1 Line" {
		t.Fatalf("expected routeShortName '1 Line', got %v", v0["routeShortName"])
	}
	pos := v0["position"].(map[string]interface{})
	if pos["latitude"] != 47.61 || pos["longitude"] != -122.31 {
		t.Fatalf("expected position from tripStatus, got %v", pos)
	}
	if v0["bearing"] != float64(90) {
		t.Fatalf("expected bearing 90, got %v", v0["bearing"])
	}
	trip := v0["trip"].(map[string]interface{})
	if trip["delay"] != float64(42) || trip["routeId"] != "40_100479" || trip["tripHeadsign"] != "Angle Lake" {
		t.Fatalf("unexpected trip enrichment: %v", trip)
	}
	// Closest stop is reported on the vehicle (not the trip), stripped of the
	// OBA "<n>_" agency prefix so the consumer can resolve it against GTFS.
	if v0["stopId"] != "99914" {
		t.Fatalf("expected closest stop on vehicle, got %v", v0["stopId"])
	}

	// Second vehicle: no trip assigned.
	e1 := entities[1].(map[string]interface{})
	v1 := e1["vehicle"].(map[string]interface{})
	if _, ok := v1["trip"].(map[string]interface{}); !ok {
		t.Fatalf("expected trip map even without assignment")
	}
	if tid := v1["trip"].(map[string]interface{})["tripId"]; tid != "" {
		t.Fatalf("expected empty tripId, got %v", tid)
	}
}

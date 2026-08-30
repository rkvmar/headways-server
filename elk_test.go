package main

import (
	"encoding/json"
	"testing"

	gtfs "github.com/MobilityData/gtfs-realtime-bindings/golang/gtfs"
	"google.golang.org/protobuf/proto"
)

func TestElkParsePositions(t *testing.T) {
	f := &gtfs.FeedMessage{
		Header: &gtfs.FeedHeader{GtfsRealtimeVersion: proto.String("2.0")},
		Entity: []*gtfs.FeedEntity{{
			Id: proto.String("1"),
			Vehicle: &gtfs.VehiclePosition{
				Trip: &gtfs.TripDescriptor{
					TripId:  proto.String("1290337"),
					RouteId: proto.String("E114"),
				},
				Vehicle:   &gtfs.VehicleDescriptor{Id: proto.String("832")},
				Position:  &gtfs.Position{Latitude: proto.Float32(38.43098), Longitude: proto.Float32(-121.397), Bearing: proto.Float32(90), Speed: proto.Float32(0)},
				Timestamp: proto.Uint64(1788049989),
			},
		}},
	}
	body, err := proto.Marshal(f)
	if err != nil {
		t.Fatalf("marshal feed: %v", err)
	}

	payload, err := elk.parsePositions(body)
	if err != nil {
		t.Fatalf("parsePositions: %v", err)
	}
	var feed map[string]interface{}
	if err := json.Unmarshal(payload, &feed); err != nil {
		t.Fatalf("unmarshal parsed feed: %v", err)
	}

	entities := feed["entity"].([]interface{})
	if len(entities) != 1 {
		t.Fatalf("expected 1 entity, got %d", len(entities))
	}
	v := entities[0].(map[string]interface{})["vehicle"].(map[string]interface{})

	pos := v["position"].(map[string]interface{})
	if pos["latitude"] != float64(38.43098) || pos["longitude"] != float64(-121.397) {
		t.Fatalf("unexpected position: %v", pos)
	}

	trip := v["trip"].(map[string]interface{})
	if trip["tripId"] != "1290337" || trip["routeId"] != "E114" {
		t.Fatalf("unexpected trip: %v", trip)
	}
	if v["vehicle"].(map[string]interface{})["id"] != "832" {
		t.Fatalf("unexpected vehicle id: %v", v["vehicle"])
	}
}

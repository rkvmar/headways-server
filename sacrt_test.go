package main

import (
	"encoding/json"
	"testing"

	gtfs "github.com/MobilityData/gtfs-realtime-bindings/golang/gtfs"
	"google.golang.org/protobuf/proto"
)

func TestSacrtParsePositions(t *testing.T) {
	f := &gtfs.FeedMessage{
		Header: &gtfs.FeedHeader{GtfsRealtimeVersion: proto.String("2.0")},
		Entity: []*gtfs.FeedEntity{{
			Id: proto.String("1"),
			Vehicle: &gtfs.VehiclePosition{
				Trip: &gtfs.TripDescriptor{
					TripId:  proto.String("1286051"),
					RouteId: proto.String("051"),
				},
				Vehicle:   &gtfs.VehicleDescriptor{Id: proto.String("1582")},
				Position:  &gtfs.Position{Latitude: proto.Float32(38.584934), Longitude: proto.Float32(-121.494354), Bearing: proto.Float32(359), Speed: proto.Float32(0)},
				Timestamp: proto.Uint64(1788049628),
			},
		}},
	}
	body, err := proto.Marshal(f)
	if err != nil {
		t.Fatalf("marshal feed: %v", err)
	}

	payload, err := sacrt.parsePositions(body)
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
	if pos["latitude"] != float64(38.584934) || pos["longitude"] != float64(-121.494354) {
		t.Fatalf("unexpected position: %v", pos)
	}

	trip := v["trip"].(map[string]interface{})
	if trip["tripId"] != "1286051" || trip["routeId"] != "051" {
		t.Fatalf("unexpected trip: %v", trip)
	}
	if v["vehicle"].(map[string]interface{})["id"] != "1582" {
		t.Fatalf("unexpected vehicle id: %v", v["vehicle"])
	}
}
package main

import (
	"testing"

	gtfs "github.com/MobilityData/gtfs-realtime-bindings/golang/gtfs"
	"google.golang.org/protobuf/proto"
)

func TestTripUpdatePredictions(t *testing.T) {
	f := &gtfs.FeedMessage{
		Header: &gtfs.FeedHeader{GtfsRealtimeVersion: proto.String("2.0")},
		Entity: []*gtfs.FeedEntity{{
			Id: proto.String("1"),
			TripUpdate: &gtfs.TripUpdate{
				Trip:  &gtfs.TripDescriptor{TripId: proto.String("T1")},
				Delay: proto.Int32(120),
				StopTimeUpdate: []*gtfs.TripUpdate_StopTimeUpdate{
					{StopId: proto.String("S1"), Departure: &gtfs.TripUpdate_StopTimeEvent{Time: proto.Int64(1000)}},
					{StopId: proto.String("S2"), Arrival: &gtfs.TripUpdate_StopTimeEvent{Time: proto.Int64(2000)}},
					{StopId: proto.String("S3")}, // no time -> skipped
				},
			},
		}},
	}

	d := tripUpdatePredictions(f)
	if d.perTrip["T1"] != 120 {
		t.Fatalf("expected trip delay 120, got %d", d.perTrip["T1"])
	}
	if d.perStop["T1"]["S1"] != 1000 {
		t.Fatalf("expected departure time at S1 = 1000, got %d", d.perStop["T1"]["S1"])
	}
	if d.perStop["T1"]["S2"] != 2000 {
		t.Fatalf("expected arrival-derived time at S2 = 2000, got %d", d.perStop["T1"]["S2"])
	}
	if _, ok := d.perStop["T1"]["S3"]; ok {
		t.Fatal("S3 should be skipped (no time)")
	}
}

func TestTripUpdateStoreAdjust(t *testing.T) {
	s := tripUpdateStore{}
	s.set(tripUpdateData{
		perStop: map[string]map[string]int64{"T1": {"S1": 500}},
		perTrip: map[string]int64{"T1": 100, "T2": 0},
	})

	if got := s.adjustedDeparture("T1", "S1", 10); got != 500 {
		t.Fatalf("per-stop prediction should win, got %d", got)
	}
	if got := s.adjustedDeparture("T1", "S9", 100); got != 200 {
		t.Fatalf("expected trip offset applied (100+100), got %d", got)
	}
	if got := s.adjustedDeparture("T2", "S1", 300); got != 300 {
		t.Fatalf("zero offset should be a no-op, got %d", got)
	}
	if got := s.adjustedDeparture("T9", "S1", 300); got != 300 {
		t.Fatalf("unknown trip should be static, got %d", got)
	}

	if pred, ok := s.predictedDeparture("T1", "S1"); !ok || pred != 500 {
		t.Fatalf("predictedDeparture: got %d, %v", pred, ok)
	}
	if _, ok := s.predictedDeparture("T1", "S9"); ok {
		t.Fatal("predictedDeparture should miss for unknown stop")
	}
}
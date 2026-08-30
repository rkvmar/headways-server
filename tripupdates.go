package main

import (
	"sync"

	gtfs "github.com/MobilityData/gtfs-realtime-bindings/golang/gtfs"
)

type tripUpdateData struct {
	perStop map[string]map[string]int64 // tripID -> stopID -> predicted departure (absolute unix)
	perTrip map[string]int64            // tripID -> deviation seconds applied to static times
}

type tripUpdateStore struct {
	mu   sync.RWMutex
	data tripUpdateData
}

func (s *tripUpdateStore) set(d tripUpdateData) {
	s.mu.Lock()
	s.data = d
	s.mu.Unlock()
}

func (s *tripUpdateStore) adjustedDeparture(tripID, stopID string, staticAbs int64) int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if m, ok := s.data.perStop[tripID]; ok {
		if abs, ok := m[stopID]; ok && abs > 0 {
			return abs
		}
	}
	if off, ok := s.data.perTrip[tripID]; ok && off != 0 {
		return staticAbs + off
	}
	return staticAbs
}

func (s *tripUpdateStore) predictedDeparture(tripID, stopID string) (int64, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	m, ok := s.data.perStop[tripID]
	if !ok {
		return 0, false
	}
	abs, ok := m[stopID]
	return abs, ok && abs > 0
}

func tripUpdatePredictions(feed *gtfs.FeedMessage) tripUpdateData {
	d := tripUpdateData{
		perStop: map[string]map[string]int64{},
		perTrip: map[string]int64{},
	}
	for _, e := range feed.GetEntity() {
		tu := e.GetTripUpdate()
		if tu == nil {
			continue
		}
		tripID := tu.GetTrip().GetTripId()
		if tripID == "" {
			continue
		}
		d.perTrip[tripID] = int64(tu.GetDelay())
		for _, stu := range tu.GetStopTimeUpdate() {
			stopID := stu.GetStopId()
			if stopID == "" {
				continue
			}
			t := stu.GetDeparture().GetTime()
			if t == 0 {
				t = stu.GetArrival().GetTime()
			}
			if t > 0 {
				if d.perStop[tripID] == nil {
					d.perStop[tripID] = map[string]int64{}
				}
				d.perStop[tripID][stopID] = t
			}
		}
	}
	return d
}

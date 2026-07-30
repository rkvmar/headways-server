package main

import (
	"encoding/json"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/graphql-go/graphql"
)

var gqlSchema graphql.Schema

func init() {
	q := graphql.NewObject(graphql.ObjectConfig{
		Name: "Query",
		Fields: graphql.Fields{
			"agencies":    &graphql.Field{Type: graphql.NewList(agencyType), Resolve: resolveAgencies},
			"routes":      &graphql.Field{Type: graphql.NewList(routeType), Resolve: resolveRoutes},
			"stops":       &graphql.Field{Type: graphql.NewList(stopType), Resolve: resolveStops},
			"trips":       &graphql.Field{Type: graphql.NewList(tripRowType), Resolve: resolveTrips},
			"vehicleFeed": &graphql.Field{Type: vehicleFeedEnvelopeType, Resolve: resolveVehicleFeed},
			"tripDetail": &graphql.Field{
				Type: tripDetailType,
				Args: graphql.FieldConfigArgument{
					"tripId": &graphql.ArgumentConfig{Type: graphql.NewNonNull(graphql.String)},
				},
				Resolve: resolveTripDetail,
			},
			"shape": &graphql.Field{
				Type: graphql.NewList(graphql.NewList(graphql.Float)),
				Args: graphql.FieldConfigArgument{
					"shapeId": &graphql.ArgumentConfig{Type: graphql.NewNonNull(graphql.String)},
				},
				Resolve: resolveShape,
			},
		},
	})

	var err error
	gqlSchema, err = graphql.NewSchema(graphql.SchemaConfig{Query: q})
	if err != nil {
		log.Fatalf("graphql schema: %v", err)
	}
}

func graphQLHandler(w http.ResponseWriter, r *http.Request) {
	var params struct {
		Query         string                 `json:"query"`
		OperationName string                 `json:"operationName"`
		Variables     map[string]interface{} `json:"variables"`
	}
	if err := json.NewDecoder(r.Body).Decode(&params); err != nil {
		http.Error(w, `{"error":"invalid json"}`, http.StatusBadRequest)
		return
	}

	result := graphql.Do(graphql.Params{
		Schema:         gqlSchema,
		RequestString:  params.Query,
		VariableValues: params.Variables,
		OperationName:  params.OperationName,
	})

	w.Header().Set("Content-Type", "application/json")
	if len(result.Errors) > 0 {
		w.WriteHeader(http.StatusBadRequest)
	}
	json.NewEncoder(w).Encode(result)
}

// vehicle feed types
var vehicleFeedEnvelopeType = graphql.NewObject(graphql.ObjectConfig{
	Name: "VehicleFeedEnvelope",
	Fields: graphql.Fields{
		"fetchedAt": &graphql.Field{Type: graphql.String},
		"data":      &graphql.Field{Type: vehicleFeedDataType},
	},
})

var vehicleFeedDataType = graphql.NewObject(graphql.ObjectConfig{
	Name: "VehicleFeedData",
	Fields: graphql.Fields{
		"entity": &graphql.Field{Type: graphql.NewList(entityType)},
	},
})

var entityType = graphql.NewObject(graphql.ObjectConfig{
	Name: "Entity",
	Fields: graphql.Fields{
		"id":   &graphql.Field{Type: graphql.String},
		"vehicle": &graphql.Field{
			Type: vehicleDataType,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				m, _ := p.Source.(map[string]interface{})
				return m["vehicle"], nil
			},
		},
	},
})

var vehicleDataType = graphql.NewObject(graphql.ObjectConfig{
	Name: "VehicleData",
	Fields: graphql.Fields{
		"trip": &graphql.Field{
			Type: tripDataType,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				m, _ := p.Source.(map[string]interface{})
				return m["trip"], nil
			},
		},
		"position": &graphql.Field{
			Type: positionType,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				m, _ := p.Source.(map[string]interface{})
				return m["position"], nil
			},
		},
		"timestamp":           &graphql.Field{Type: graphql.Int},
		"stopId":              &graphql.Field{Type: graphql.String},
		"currentStopSequence": &graphql.Field{Type: graphql.Int},
		"occupancyStatus":     &graphql.Field{Type: graphql.String},
		"stopName":            &graphql.Field{Type: graphql.String},
		"vehicle": &graphql.Field{
			Type: vehicleDescriptorType,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				m, _ := p.Source.(map[string]interface{})
				return m["vehicle"], nil
			},
		},
		"vehicleYear":   &graphql.Field{Type: graphql.Int},
		"vehicleMake":   &graphql.Field{Type: graphql.String},
		"vehicleModel":  &graphql.Field{Type: graphql.String},
		"vehicleFuel":   &graphql.Field{Type: graphql.String},
		"vehicleLength": &graphql.Field{Type: graphql.Int},
		"vehicleIconCode": &graphql.Field{
			Type: graphql.String,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				m, _ := p.Source.(map[string]interface{})
				if v, ok := m["vehicleIconCode"]; ok {
					return v, nil
				}
				return m["icon_code"], nil
			},
		},
	},
})

var tripDataType = graphql.NewObject(graphql.ObjectConfig{
	Name: "TripData",
	Fields: graphql.Fields{
		"tripId":             &graphql.Field{Type: graphql.String},
		"routeId":            &graphql.Field{Type: graphql.String},
		"directionId":        &graphql.Field{Type: graphql.Int},
		"delay":              &graphql.Field{Type: graphql.Int},
		"startTime":          &graphql.Field{Type: graphql.String},
		"startDate":          &graphql.Field{Type: graphql.String},
		"scheduleRelationship": &graphql.Field{Type: graphql.String},
	},
})

var positionType = graphql.NewObject(graphql.ObjectConfig{
	Name: "Position",
	Fields: graphql.Fields{
		"latitude":  &graphql.Field{Type: graphql.Float},
		"longitude": &graphql.Field{Type: graphql.Float},
		"bearing":   &graphql.Field{Type: graphql.Float},
		"speed":     &graphql.Field{Type: graphql.Float},
	},
})

var vehicleDescriptorType = graphql.NewObject(graphql.ObjectConfig{
	Name: "VehicleDescriptor",
	Fields: graphql.Fields{
		"id":    &graphql.Field{Type: graphql.String},
		"label": &graphql.Field{Type: graphql.String},
	},
})

// GTFS types
var agencyType = graphql.NewObject(graphql.ObjectConfig{
	Name: "Agency",
	Fields: graphql.Fields{
		"agency_id":       &graphql.Field{Type: graphql.String},
		"agency_name":     &graphql.Field{Type: graphql.String},
		"agency_url":      &graphql.Field{Type: graphql.String},
		"agency_timezone": &graphql.Field{Type: graphql.String},
		"agency_lang":     &graphql.Field{Type: graphql.String},
		"agency_phone":    &graphql.Field{Type: graphql.String},
		"agency_fare_url": &graphql.Field{Type: graphql.String},
		"agency_email":    &graphql.Field{Type: graphql.String},
	},
})

var routeType = graphql.NewObject(graphql.ObjectConfig{
	Name: "Route",
	Fields: graphql.Fields{
		"route_id":         &graphql.Field{Type: graphql.String},
		"agency_id":        &graphql.Field{Type: graphql.String},
		"route_short_name": &graphql.Field{Type: graphql.String},
		"route_long_name":  &graphql.Field{Type: graphql.String},
		"route_type":       &graphql.Field{Type: graphql.String},
		"route_color":      &graphql.Field{Type: graphql.String},
		"route_text_color": &graphql.Field{Type: graphql.String},
		"route_url":        &graphql.Field{Type: graphql.String},
		"route_desc":       &graphql.Field{Type: graphql.String},
	},
})

var stopType = graphql.NewObject(graphql.ObjectConfig{
	Name: "Stop",
	Fields: graphql.Fields{
		"stop_id":   &graphql.Field{Type: graphql.String},
		"stop_name": &graphql.Field{Type: graphql.String},
		"stop_lat":  &graphql.Field{Type: graphql.String},
		"stop_lon":  &graphql.Field{Type: graphql.String},
	},
})

var tripRowType = graphql.NewObject(graphql.ObjectConfig{
	Name: "Trip",
	Fields: graphql.Fields{
		"trip_id":                 &graphql.Field{Type: graphql.String},
		"route_id":                &graphql.Field{Type: graphql.String},
		"service_id":              &graphql.Field{Type: graphql.String},
		"trip_headsign":           &graphql.Field{Type: graphql.String},
		"direction_id":            &graphql.Field{Type: graphql.String},
		"shape_id":                &graphql.Field{Type: graphql.String},
		"block_id":                &graphql.Field{Type: graphql.String},
		"trip_short_name":         &graphql.Field{Type: graphql.String},
		"wheelchair_accessible":   &graphql.Field{Type: graphql.String},
		"bikes_allowed":           &graphql.Field{Type: graphql.String},
	},
})

// trip detail types
var tripDetailType = graphql.NewObject(graphql.ObjectConfig{
	Name: "TripDetail",
	Fields: graphql.Fields{
		"trip_id":         &graphql.Field{Type: graphql.String},
		"route_id":        &graphql.Field{Type: graphql.String},
		"service_id":      &graphql.Field{Type: graphql.String},
		"trip_headsign":   &graphql.Field{Type: graphql.String},
		"direction_id":    &graphql.Field{Type: graphql.String},
		"shape_id":        &graphql.Field{Type: graphql.String},
		"block_id":        &graphql.Field{Type: graphql.String},
		"trip_short_name": &graphql.Field{Type: graphql.String},
		"shape":           &graphql.Field{Type: graphql.NewList(graphql.NewList(graphql.Float))},
		"schedule":        &graphql.Field{Type: graphql.NewList(stopTimeType)},
	},
})

var stopTimeType = graphql.NewObject(graphql.ObjectConfig{
	Name: "StopTime",
	Fields: graphql.Fields{
		"stop_id":        &graphql.Field{Type: graphql.String},
		"stop_sequence":  &graphql.Field{Type: graphql.Int},
		"arrival_time":   &graphql.Field{Type: graphql.String},
		"departure_time": &graphql.Field{Type: graphql.String},
		"stop_name":      &graphql.Field{Type: graphql.String},
		"stop_lat":       &graphql.Field{Type: graphql.String},
		"stop_lon":       &graphql.Field{Type: graphql.String},
	},
})

// resolvers
func resolveAgencies(p graphql.ResolveParams) (interface{}, error) {
	path := filepath.Join(tripDataDir, "agency.json")
	return readJSONArray(path)
}

func resolveRoutes(p graphql.ResolveParams) (interface{}, error) {
	path := filepath.Join(tripDataDir, "routes.json")
	return readJSONArray(path)
}

func resolveStops(p graphql.ResolveParams) (interface{}, error) {
	path := filepath.Join(tripDataDir, "stops.json")
	return readJSONArray(path)
}

func resolveTrips(p graphql.ResolveParams) (interface{}, error) {
	path := filepath.Join(tripDataDir, "trips.json")
	return readJSONArray(path)
}

func resolveVehicleFeed(p graphql.ResolveParams) (interface{}, error) {
	vehiclePositionsCacheMu.RLock()
	payload := vehiclePositionsCachePayload
	cacheTime := vehiclePositionsCacheTime
	vehiclePositionsCacheMu.RUnlock()

	if payload == nil {
		return nil, nil
	}

	var data interface{}
	if err := json.Unmarshal(payload, &data); err != nil {
		return nil, err
	}

	return map[string]interface{}{
		"fetchedAt": cacheTime.UTC().Format(time.RFC3339Nano),
		"data":      data,
	}, nil
}

func resolveTripDetail(p graphql.ResolveParams) (interface{}, error) {
	tripID, _ := p.Args["tripId"].(string)
	if tripID == "" {
		return nil, nil
	}

	// Try pre-computed file first
	if tripDetailsDir != "" {
		tripPath := filepath.Join(tripDetailsDir, tripID+".json")
		if data, err := os.ReadFile(tripPath); err == nil {
			var result map[string]interface{}
			if err := json.Unmarshal(data, &result); err == nil {
				return result, nil
			}
		}
	}

	// Build from GTFS data
	trips := loadTripsData()
	stops := loadStopsData()

	trip, ok := trips[tripID]
	if !ok {
		return nil, nil
	}

	times := loadStopTimesForTrip(tripID)
	schedule := make([]map[string]interface{}, 0, len(times))
	for _, st := range times {
		stop := stops[st.stop_id]
		schedule = append(schedule, map[string]interface{}{
			"stop_id":        st.stop_id,
			"stop_sequence":  st.stop_sequence,
			"arrival_time":   st.arrival_time,
			"departure_time": st.departure_time,
			"stop_name":      stop.stop_name,
			"stop_lat":       stop.stop_lat,
			"stop_lon":       stop.stop_lon,
		})
	}

	var shapeCoords [][2]float64
	if trip.shape_id != "" {
		coords, err := loadShapeForTrip(trip.shape_id)
		if err == nil {
			shapeCoords = coords
		}
	}

	shape := make([][]float64, len(shapeCoords))
	for i, c := range shapeCoords {
		shape[i] = []float64{c[0], c[1]}
	}

	return map[string]interface{}{
		"trip_id":         trip.trip_id,
		"route_id":        trip.route_id,
		"service_id":      trip.service_id,
		"trip_headsign":   trip.trip_headsign,
		"direction_id":    trip.direction_id,
		"shape_id":        trip.shape_id,
		"block_id":        trip.block_id,
		"trip_short_name": trip.trip_short_name,
		"shape":           shape,
		"schedule":        schedule,
	}, nil
}

func resolveShape(p graphql.ResolveParams) (interface{}, error) {
	shapeID, _ := p.Args["shapeId"].(string)
	if shapeID == "" {
		return [][]float64{}, nil
	}

	coords, err := loadShapeForTrip(shapeID)
	if err != nil || coords == nil {
		return [][]float64{}, nil
	}

	shape := make([][]float64, len(coords))
	for i, c := range coords {
		shape[i] = []float64{c[0], c[1]}
	}
	return shape, nil
}

func readJSONArray(path string) ([]interface{}, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return []interface{}{}, nil
	}
	var arr []interface{}
	if err := json.Unmarshal(data, &arr); err != nil {
		return []interface{}{}, nil
	}
	return arr, nil
}

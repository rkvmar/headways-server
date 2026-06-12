package main

import (
	"archive/zip"
	"compress/gzip"
	"compress/zlib"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	gtfs "github.com/MobilityData/gtfs-realtime-bindings/golang/gtfs"
	"github.com/joho/godotenv"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

const (
	baseURL             = "http://api.511.org/transit/tripupdates"
	vehiclePositionsURL = "http://api.511.org/transit/vehiclepositions"
	datafeedsBase       = "http://api.511.org/transit/datafeeds"
	agencyParam         = "RG"
	operatorParam       = "RG"
)

const (
	vehiclePositionsCacheInterval = 1 * time.Minute
	tripUpdatesCacheInterval      = 1 * time.Minute
	datafeedsRefreshInterval      = 1 * time.Hour
)

var datafeedsFilePath string
var datafeedsDir string
var vehicleTypesFilePath string
var datafeedsJSONFiles []string
var datafeedsFiles []string
var datafeedsFileMap map[string]datafeedEntry
var routeShapesCache []byte
var routeShapesCacheMu sync.RWMutex

var (
	vehiclePositionsCacheMu      sync.RWMutex
	vehiclePositionsCachePayload []byte
	vehiclePositionsCacheErr     error
	vehiclePositionsCacheTime    time.Time
)

var (
	tripUpdatesCacheMu      sync.RWMutex
	tripUpdatesCachePayload []byte
	tripUpdatesCacheErr     error
	tripUpdatesCacheTime    time.Time
)

func main() {
	if err := godotenv.Load(); err != nil {
		log.Println(".env not found or could not be loaded, falling back to existing env")
	}

	workingDir, err := os.Getwd()
	if err != nil {
		log.Fatalf("failed to get working directory: %v", err)
	}
	datafeedsFilePath = filepath.Join(workingDir, "data", "gtfs.zip")
	datafeedsDir = filepath.Join(workingDir, "data", "gtfs")
	vehicleTypesFilePath = filepath.Join(workingDir, "vehicle_types.json")

	if err := refreshDatafeeds(); err != nil {
		log.Fatalf("failed to load datafeeds: %v", err)
	}

	if os.Getenv("LOCATIONS_API_KEY") == "" {
		log.Fatalln("LOCATIONS_API_KEY not set")
	}

	go runDatafeedsRefresher()
	go runVehiclePositionsRefresher()
	go runTripUpdatesRefresher()

	mux := http.NewServeMux()
	mux.HandleFunc("/tripupdates", tripUpdatesHandler)
	mux.HandleFunc("/vehiclepositions", vehiclePositionsHandler)
	mux.HandleFunc("/routeshapes", routeShapesHandler)
	mux.HandleFunc("/tripdetail", tripDetailHandler)
	mux.HandleFunc("/blockschedule", blockScheduleHandler)
	mux.HandleFunc("/datafeeds/zip", datafeedsZipHandler)
	mux.HandleFunc("/datafeeds", datafeedsGTFSIndexHandler)
	mux.HandleFunc("/datafeeds/json", datafeedsJSONIndexHandler)
	mux.HandleFunc("/datafeeds/json/", datafeedsJSONFileHandler)
	mux.HandleFunc("/datafeeds/", datafeedsGTFSFileHandler)
	mux.HandleFunc("/vehicletypes", vehicleTypesHandler)

	corsHandler := func(h http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Access-Control-Allow-Origin", "*")
			w.Header().Set("Access-Control-Allow-Methods", "GET, OPTIONS")
			w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
			if r.Method == "OPTIONS" {
				w.WriteHeader(http.StatusOK)
				return
			}
			h.ServeHTTP(w, r)
		})
	}

	server := &http.Server{
		Addr:              ":8081",
		Handler:           corsHandler(mux),
		ReadHeaderTimeout: 5 * time.Second,
	}

	log.Println("listening on :8081")
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("server error: %v", err)
	}
}

func tripUpdatesHandler(w http.ResponseWriter, r *http.Request) {
	tripUpdatesCacheMu.RLock()
	payload := tripUpdatesCachePayload
	cacheErr := tripUpdatesCacheErr
	cacheTime := tripUpdatesCacheTime
	tripUpdatesCacheMu.RUnlock()

	if cacheErr != nil {
		http.Error(w, fmt.Sprintf("trip updates cache unavailable: %v", cacheErr), http.StatusBadGateway)
		return
	}
	if payload == nil {
		http.Error(w, "trip updates cache not yet populated", http.StatusServiceUnavailable)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Cache-Time", cacheTime.UTC().Format(http.TimeFormat))
	if _, err := w.Write(payload); err != nil {
		http.Error(w, "failed to write response", http.StatusInternalServerError)
		return
	}
}

func runTripUpdatesRefresher() {
	if err := refreshTripUpdates(); err != nil {
		log.Printf("trip updates refresh failed: %v", err)
	}

	ticker := time.NewTicker(tripUpdatesCacheInterval)
	defer ticker.Stop()
	for range ticker.C {
		if err := refreshTripUpdates(); err != nil {
			log.Printf("trip updates refresh failed: %v", err)
		}
	}
}

func refreshTripUpdates() error {
	apiKey := os.Getenv("TRIP_UPDATES_API_KEY")
	if apiKey == "" {
		return fmt.Errorf("TRIP_UPDATES_API_KEY env var is not set")
	}

	client := &http.Client{Timeout: 15 * time.Second}
	url := fmt.Sprintf("%s?api_key=%s&agency=%s", baseURL, apiKey, agencyParam)

	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Accept", "application/x-protobuf")

	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to fetch trip updates: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("upstream error: %s", string(body))
	}

	body, err := readResponseBody(resp)
	if err != nil {
		return fmt.Errorf("failed to read upstream response: %w", err)
	}

	var feed gtfs.FeedMessage
	if err := proto.Unmarshal(body, &feed); err != nil {
		return fmt.Errorf("invalid GTFS-realtime protobuf from upstream: %w", err)
	}

	marshaler := protojson.MarshalOptions{Indent: "  "}
	payload, err := marshaler.Marshal(&feed)
	if err != nil {
		return fmt.Errorf("failed to encode response: %w", err)
	}

	tripUpdatesCacheMu.Lock()
	tripUpdatesCachePayload = payload
	tripUpdatesCacheErr = nil
	tripUpdatesCacheTime = time.Now()
	tripUpdatesCacheMu.Unlock()

	return nil
}

func enrichVehiclePositions(payload []byte) []byte {
	stopsData := loadStopsData()

	var feed map[string]interface{}
	if err := json.Unmarshal(payload, &feed); err != nil {
		return payload
	}

	entities, ok := feed["entity"].([]interface{})
	if !ok {
		return payload
	}

	for _, e := range entities {
		entity, ok := e.(map[string]interface{})
		if !ok {
			continue
		}
		vehicle, ok := entity["vehicle"].(map[string]interface{})
		if !ok {
			continue
		}

		// Resolve stop name from stop ID
		if stopID, ok := vehicle["stopId"].(string); ok && stopID != "" {
			if stop, ok := stopsData[stopID]; ok && stop.stop_name != "" {
				vehicle["stopName"] = stop.stop_name
			}
		}

		// Inject delay if missing (mock data won't have it; real API data may be dropped by protojson)
		trip, ok := vehicle["trip"].(map[string]interface{})
		if ok {
			if _, hasDelay := trip["delay"]; !hasDelay {
				// Random delay: 60% on time, 25% late (1-5 min), 15% early (0.5-2 min)
				r := rand.Intn(100)
				var delay int32
				switch {
				case r < 60:
					delay = 0
				case r < 85:
					delay = int32(rand.Intn(300) + 60)
				default:
					delay = -int32(rand.Intn(120) + 30)
				}
				trip["delay"] = delay
			}
		}

		// Resolve vehicle type from vehicle ID using vehicle types map
		if vehObj, ok := vehicle["vehicle"].(map[string]interface{}); ok {
			vehicleID, _ := vehObj["id"].(string)
			if vehicleID == "" {
				// Fall back to entity ID
				vehicleID, _ = entity["id"].(string)
			}

			// Try to determine agency code from trip
			agencyCode := ""
			if trip, ok := vehicle["trip"].(map[string]interface{}); ok {
				if tripID, ok := trip["tripId"].(string); ok && tripID != "" {
					if idx := strings.Index(tripID, ":"); idx >= 0 {
						agencyCode = tripID[:idx]
					} else if idx := strings.LastIndex(tripID, "~"); idx >= 0 {
						agencyCode = tripID[:idx]
					}
				}
			}

			if agencyCode != "" && vehicleID != "" {
				if vti := lookupVehicleType(agencyCode, vehicleID); vti != nil {
					vehicle["vehicleYear"] = vti.Year
					vehicle["vehicleMake"] = vti.Make
					vehicle["vehicleModel"] = vti.Model
					vehicle["vehicleFuel"] = vti.Fuel
					vehicle["vehicleLength"] = vti.Length
					vehicle["vehicleIconCode"] = vti.IconCode
				}
			}
		}
	}

	enriched, err := json.Marshal(feed)
	if err != nil {
		return payload
	}
	return enriched
}

func vehiclePositionsHandler(w http.ResponseWriter, r *http.Request) {
	vehiclePositionsCacheMu.RLock()
	payload := vehiclePositionsCachePayload
	cacheErr := vehiclePositionsCacheErr
	cacheTime := vehiclePositionsCacheTime
	vehiclePositionsCacheMu.RUnlock()

	if cacheErr != nil {
		http.Error(w, fmt.Sprintf("vehicle positions cache unavailable: %v", cacheErr), http.StatusBadGateway)
		return
	}
	if payload == nil {
		http.Error(w, "vehicle positions cache not yet populated", http.StatusServiceUnavailable)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Cache-Time", cacheTime.UTC().Format(http.TimeFormat))
	if _, err := w.Write(payload); err != nil {
		http.Error(w, "failed to write response", http.StatusInternalServerError)
		return
	}
}

func blockScheduleHandler(w http.ResponseWriter, r *http.Request) {
	vehicleID := r.URL.Query().Get("vehicle_id")
	tripID := r.URL.Query().Get("trip_id")

	var targetTripID string

	if tripID != "" {
		targetTripID = tripID
	} else if vehicleID != "" {
		vehiclePositionsCacheMu.RLock()
		vehiclePayload := vehiclePositionsCachePayload
		vehiclePositionsCacheMu.RUnlock()

		if vehiclePayload == nil {
			http.Error(w, "vehicle positions not available", http.StatusServiceUnavailable)
			return
		}

		var vehicleFeed gtfs.FeedMessage
		if err := proto.Unmarshal(vehiclePayload, &vehicleFeed); err != nil {
			http.Error(w, "failed to parse vehicle positions", http.StatusInternalServerError)
			return
		}

		for _, entity := range vehicleFeed.Entity {
			vehicle := entity.GetVehicle()
			if vehicle == nil {
				continue
			}
			v := vehicle.GetVehicle()
			if v == nil {
				continue
			}
			if v.GetId() == vehicleID || v.GetLabel() == vehicleID || entity.GetId() == vehicleID {
				trip := vehicle.GetTrip()
				if trip != nil {
					targetTripID = trip.GetTripId()
				}
				break
			}
		}

		if targetTripID == "" {
			http.Error(w, "vehicle not found", http.StatusNotFound)
			return
		}
	} else {
		http.Error(w, "vehicle_id or trip_id query parameter required", http.StatusBadRequest)
		return
	}

	tripsData := loadTripsData()
	tripInfo, ok := tripsData[targetTripID]
	if !ok {
		http.Error(w, "trip not found in GTFS", http.StatusNotFound)
		return
	}

	blockID := tripInfo.block_id
	serviceID := tripInfo.service_id

	if blockID == "" {
		http.Error(w, "vehicle has no block assignment", http.StatusNotFound)
		return
	}

	blockTrips := []TripInfo{}
	for _, t := range tripsData {
		if t.block_id == blockID && t.service_id == serviceID {
			blockTrips = append(blockTrips, t)
		}
	}

	sort.Slice(blockTrips, func(i, j int) bool {
		return blockTrips[i].trip_start_time < blockTrips[j].trip_start_time
	})

	stopsData := loadStopsData()

	type BlockScheduleResponse struct {
		BlockInfo struct {
			BlockID   string `json:"block_id"`
			ServiceID string `json:"service_id"`
		} `json:"block_info"`
		Schedule []BlockScheduleEntry `json:"schedule"`
	}

	blockSchedule := BlockScheduleResponse{}
	blockSchedule.BlockInfo.BlockID = blockID
	blockSchedule.BlockInfo.ServiceID = serviceID

	for _, trip := range blockTrips {
		stopTimes := loadStopTimesForTrip(trip.trip_id)
		sort.Slice(stopTimes, func(i, j int) bool {
			return stopTimes[i].stop_sequence < stopTimes[j].stop_sequence
		})

		var startStopName, endStopName string
		if len(stopTimes) > 0 {
			if startStop, ok := stopsData[stopTimes[0].stop_id]; ok {
				startStopName = startStop.stop_name
			}
			if endStop, ok := stopsData[stopTimes[len(stopTimes)-1].stop_id]; ok {
				endStopName = endStop.stop_name
			}
		}

		entry := BlockScheduleEntry{
			TripID:               trip.trip_id,
			RouteID:              trip.route_id,
			RouteShortName:       getRouteShortName(trip.route_id),
			RouteLongName:        getRouteLongName(trip.route_id),
			DirectionID:          trip.direction_id,
			StartTime:            trip.trip_start_time,
			EndTime:              trip.trip_end_time,
			HeadSign:             trip.trip_headsign,
			StartStopName:        startStopName,
			EndStopName:          endStopName,
			ShapeID:              trip.shape_id,
			WheelchairAccessible: trip.wheelchair_accessible,
			BikesAllowed:         trip.bikes_allowed,
		}
		blockSchedule.Schedule = append(blockSchedule.Schedule, entry)
	}

	w.Header().Set("Content-Type", "application/json")
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	if err := enc.Encode(blockSchedule); err != nil {
		http.Error(w, "failed to encode response", http.StatusInternalServerError)
		return
	}
}

func runVehiclePositionsRefresher() {
	if err := refreshVehiclePositions(); err != nil {
		log.Printf("vehicle positions refresh failed: %v", err)
	}

	ticker := time.NewTicker(vehiclePositionsCacheInterval)
	defer ticker.Stop()
	for range ticker.C {
		if err := refreshVehiclePositions(); err != nil {
			log.Printf("vehicle positions refresh failed: %v", err)
		}
	}
}

func refreshVehiclePositions() error {
	apiKey := os.Getenv("LOCATIONS_API_KEY")
	if apiKey == "" {
		return fmt.Errorf("LOCATIONS_API_KEY not set")
	}

	client := &http.Client{Timeout: 15 * time.Second}
	url := fmt.Sprintf("%s?api_key=%s&agency=%s", vehiclePositionsURL, apiKey, agencyParam)

	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Accept", "application/x-protobuf")

	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to fetch vehicle positions: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("upstream error: %s", string(body))
	}

	body, err := readResponseBody(resp)
	if err != nil {
		return fmt.Errorf("failed to read upstream response: %w", err)
	}

	var feed gtfs.FeedMessage
	if err := proto.Unmarshal(body, &feed); err != nil {
		return fmt.Errorf("invalid GTFS-realtime protobuf from upstream: %w", err)
	}

	marshaler := protojson.MarshalOptions{Indent: "  "}
	marshaled, err := marshaler.Marshal(&feed)
	if err != nil {
		return fmt.Errorf("failed to encode response: %w", err)
	}

	payload := enrichVehiclePositions(marshaled)

	vehiclePositionsCacheMu.Lock()
	vehiclePositionsCachePayload = payload
	vehiclePositionsCacheErr = nil
	vehiclePositionsCacheTime = time.Now()
	vehiclePositionsCacheMu.Unlock()

	return nil
}

func parseDirection(dir string) int32 {
	var d int32
	fmt.Sscanf(dir, "%d", &d)
	return d
}

func datafeedsZipHandler(w http.ResponseWriter, r *http.Request) {
	if datafeedsFilePath == "" {
		http.Error(w, "datafeed not initialized", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/zip")
	w.Header().Set("Content-Disposition", "attachment; filename=gtfs.zip")
	http.ServeFile(w, r, datafeedsFilePath)
}

func datafeedsJSONIndexHandler(w http.ResponseWriter, r *http.Request) {
	if datafeedsJSONFiles == nil {
		http.Error(w, "datafeed json index not initialized", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	if err := enc.Encode(datafeedsJSONFiles); err != nil {
		http.Error(w, "failed to write response", http.StatusInternalServerError)
		return
	}
}

func datafeedsJSONFileHandler(w http.ResponseWriter, r *http.Request) {
	if datafeedsDir == "" {
		http.Error(w, "datafeed not initialized", http.StatusInternalServerError)
		return
	}

	relPath := strings.TrimPrefix(r.URL.Path, "/datafeeds/json/")
	if relPath == "" {
		http.NotFound(w, r)
		return
	}
	if !strings.HasSuffix(relPath, ".json") {
		http.NotFound(w, r)
		return
	}

	cleanRel := filepath.Clean(relPath)
	fullPath := filepath.Join(datafeedsDir, cleanRel)
	if !isSubpath(datafeedsDir, fullPath) {
		http.Error(w, "invalid path", http.StatusBadRequest)
		return
	}

	if _, err := os.Stat(fullPath); err != nil {
		http.NotFound(w, r)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	http.ServeFile(w, r, fullPath)
}

var (
	shapesCache   map[string][][2]float64
	shapesOnce    sync.Once
	shapesCacheMu sync.RWMutex
)

func loadShapeForTrip(shapeID string) ([][2]float64, error) {
	shapesCacheMu.RLock()
	if shapesCache != nil {
		if coords, ok := shapesCache[shapeID]; ok {
			shapesCacheMu.RUnlock()
			return coords, nil
		}
		shapesCacheMu.RUnlock()
		// Load just one shape from disk if cache is populated but shape isn't in it
		return loadShapeFromDisk(shapeID)
	}
	shapesCacheMu.RUnlock()

	// Build the full cache once
	shapesOnce.Do(func() {
		entry, ok := datafeedsFileMap["shapes"]
		if !ok {
			return
		}

		file, err := os.Open(entry.path)
		if err != nil {
			log.Printf("failed to open shapes.txt: %v", err)
			return
		}
		defer file.Close()

		reader := csv.NewReader(file)
		reader.FieldsPerRecord = -1
		reader.LazyQuotes = true

		headers, err := reader.Read()
		if err != nil {
			log.Printf("failed to read shapes.txt headers: %v", err)
			return
		}

		type point struct {
			lat float64
			lon float64
			seq int
		}
		shapePoints := make(map[string][]point)
		for {
			record, err := reader.Read()
			if err == io.EOF {
				break
			}
			if err != nil {
				log.Printf("error reading shapes.txt: %v", err)
				continue
			}

			rec := make(map[string]string, len(headers))
			for i, h := range headers {
				if i < len(record) {
					rec[h] = record[i]
				}
			}

			sid := rec["shape_id"]
			if sid == "" {
				continue
			}

			lat, _ := strconv.ParseFloat(rec["shape_pt_lat"], 64)
			lon, _ := strconv.ParseFloat(rec["shape_pt_lon"], 64)
			seq, _ := strconv.Atoi(rec["shape_pt_sequence"])

			shapePoints[sid] = append(shapePoints[sid], point{lat: lat, lon: lon, seq: seq})
		}

		result := make(map[string][][2]float64, len(shapePoints))
		for sid, pts := range shapePoints {
			sort.Slice(pts, func(i, j int) bool {
				return pts[i].seq < pts[j].seq
			})
			coords := make([][2]float64, len(pts))
			for i, p := range pts {
				coords[i] = [2]float64{p.lat, p.lon}
			}
			result[sid] = coords
		}

		shapesCacheMu.Lock()
		shapesCache = result
		shapesCacheMu.Unlock()
		log.Printf("Loaded %d shapes into cache", len(result))
	})

	shapesCacheMu.RLock()
	defer shapesCacheMu.RUnlock()
	if shapesCache == nil {
		return loadShapeFromDisk(shapeID)
	}
	return shapesCache[shapeID], nil
}

func loadShapeFromDisk(shapeID string) ([][2]float64, error) {
	entry, ok := datafeedsFileMap["shapes"]
	if !ok {
		return nil, fmt.Errorf("shapes data not available")
	}

	file, err := os.Open(entry.path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	reader := csv.NewReader(file)
	reader.FieldsPerRecord = -1
	reader.LazyQuotes = true

	headers, err := reader.Read()
	if err != nil {
		return nil, err
	}

	type point struct {
		lat float64
		lon float64
		seq int
	}
	var points []point
	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}

		rec := make(map[string]string, len(headers))
		for i, h := range headers {
			if i < len(record) {
				rec[h] = record[i]
			}
		}

		if rec["shape_id"] != shapeID {
			continue
		}

		lat, _ := strconv.ParseFloat(rec["shape_pt_lat"], 64)
		lon, _ := strconv.ParseFloat(rec["shape_pt_lon"], 64)
		seq, _ := strconv.Atoi(rec["shape_pt_sequence"])

		points = append(points, point{lat: lat, lon: lon, seq: seq})
	}

	if len(points) == 0 {
		return nil, nil
	}

	sort.Slice(points, func(i, j int) bool {
		return points[i].seq < points[j].seq
	})

	coords := make([][2]float64, len(points))
	for i, p := range points {
		coords[i] = [2]float64{p.lat, p.lon}
	}

	return coords, nil
}

func tripDetailHandler(w http.ResponseWriter, r *http.Request) {
	tripID := r.URL.Query().Get("trip_id")
	if tripID == "" {
		http.Error(w, "trip_id query param required", http.StatusBadRequest)
		return
	}

	trips := loadTripsData()
	stops := loadStopsData()

	trip, ok := trips[tripID]
	if !ok {
		http.Error(w, "trip not found", http.StatusNotFound)
		return
	}

	times := loadStopTimesForTrip(tripID)
	if times == nil {
		times = []StopTimeInfo{}
	}

	schedule := make([]map[string]interface{}, 0, len(times))
	for _, st := range times {
		stop := stops[st.stop_id]
		entry := map[string]interface{}{
			"stop_id":        st.stop_id,
			"stop_sequence":  st.stop_sequence,
			"arrival_time":   st.arrival_time,
			"departure_time": st.departure_time,
			"stop_name":      stop.stop_name,
			"stop_lat":       stop.stop_lat,
			"stop_lon":       stop.stop_lon,
		}
		schedule = append(schedule, entry)
	}

	var shapeCoords [][2]float64
	if trip.shape_id != "" {
		coords, err := loadShapeForTrip(trip.shape_id)
		if err == nil {
			shapeCoords = coords
		}
	}

	result := map[string]interface{}{
		"trip_id":         trip.trip_id,
		"route_id":        trip.route_id,
		"service_id":      trip.service_id,
		"trip_headsign":   trip.trip_headsign,
		"direction_id":    trip.direction_id,
		"shape_id":        trip.shape_id,
		"block_id":        trip.block_id,
		"trip_short_name": trip.trip_short_name,
		"shape":           shapeCoords,
		"schedule":        schedule,
	}

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "public, max-age=60")
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	enc.Encode(result)
}

func routeShapesHandler(w http.ResponseWriter, r *http.Request) {
	shapeID := r.URL.Query().Get("shape_id")
	if shapeID == "" {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte("{}"))
		return
	}

	coords, err := loadShapeForTrip(shapeID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if coords == nil {
		coords = [][2]float64{}
	}

	payload, err := json.Marshal(coords)
	if err != nil {
		http.Error(w, "failed to encode shape", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "public, max-age=3600")
	w.Write(payload)
}

func datafeedsGTFSIndexHandler(w http.ResponseWriter, r *http.Request) {
	if datafeedsFileMap == nil {
		http.Error(w, "datafeed file map not initialized", http.StatusInternalServerError)
		return
	}

	var names []string
	for name := range datafeedsFileMap {
		names = append(names, name)
	}
	sort.Strings(names)

	w.Header().Set("Content-Type", "application/json")
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	if err := enc.Encode(names); err != nil {
		http.Error(w, "failed to write response", http.StatusInternalServerError)
		return
	}
}

func datafeedsGTFSFileHandler(w http.ResponseWriter, r *http.Request) {
	if datafeedsFileMap == nil {
		http.Error(w, "datafeed file map not initialized", http.StatusInternalServerError)
		return
	}

	name := strings.TrimPrefix(r.URL.Path, "/datafeeds/")
	name = strings.TrimSpace(name)
	if name == "" {
		http.NotFound(w, r)
		return
	}
	if name == "zip" || strings.HasPrefix(name, "json") {
		http.NotFound(w, r)
		return
	}

	entry, ok := datafeedsFileMap[name]
	if !ok {
		http.NotFound(w, r)
		return
	}

	switch entry.ext {
	case ".txt", ".csv":
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "public, max-age=3600")
		if err := streamCSVAsJSON(entry.path, w); err != nil {
			log.Printf("error streaming %s as json: %v", entry.path, err)
		}
	case ".md":
		content, err := os.ReadFile(entry.path)
		if err != nil {
			http.Error(w, "failed to read markdown", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		payload := map[string]string{"content": string(content)}
		enc := json.NewEncoder(w)
		enc.SetIndent("", "  ")
		if err := enc.Encode(payload); err != nil {
			http.Error(w, "failed to write response", http.StatusInternalServerError)
			return
		}
	default:
		http.Error(w, "unsupported file type", http.StatusBadRequest)
	}
}

func downloadDatafeed(destination string) error {
	apiKey := os.Getenv("API_KEY")
	if apiKey == "" {
		return fmt.Errorf("API_KEY env var is not set")
	}

	if err := os.MkdirAll(filepath.Dir(destination), 0o755); err != nil {
		return fmt.Errorf("failed to create data directory: %w", err)
	}

	client := &http.Client{Timeout: 60 * time.Second}
	url := fmt.Sprintf("%s?api_key=%s&operator_id=%s", datafeedsBase, apiKey, operatorParam)

	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Accept", "application/zip")

	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to fetch datafeeds: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("upstream error: %s", string(body))
	}

	body, err := readResponseBody(resp)
	if err != nil {
		return fmt.Errorf("failed to read upstream response: %w", err)
	}

	if err := os.WriteFile(destination, body, 0o644); err != nil {
		return fmt.Errorf("failed to write datafeed file: %w", err)
	}

	return nil
}

func unzipDatafeed(zipPath, destination string) error {
	if err := os.RemoveAll(destination); err != nil {
		return fmt.Errorf("failed to clear destination: %w", err)
	}
	if err := os.MkdirAll(destination, 0o755); err != nil {
		return fmt.Errorf("failed to create destination: %w", err)
	}

	reader, err := zip.OpenReader(zipPath)
	if err != nil {
		return fmt.Errorf("failed to open zip: %w", err)
	}
	defer reader.Close()

	for _, file := range reader.File {
		if err := extractZipFile(file, destination); err != nil {
			return err
		}
	}

	return nil
}

func extractZipFile(file *zip.File, destination string) error {
	cleanName := filepath.Clean(file.Name)
	if strings.HasPrefix(cleanName, "..") {
		return fmt.Errorf("invalid zip entry: %s", file.Name)
	}

	fullPath := filepath.Join(destination, cleanName)
	if !isSubpath(destination, fullPath) {
		return fmt.Errorf("invalid zip entry path: %s", file.Name)
	}

	if file.FileInfo().IsDir() {
		if err := os.MkdirAll(fullPath, file.Mode()); err != nil {
			return fmt.Errorf("failed to create directory: %w", err)
		}
		return nil
	}

	if err := os.MkdirAll(filepath.Dir(fullPath), 0o755); err != nil {
		return fmt.Errorf("failed to create directory: %w", err)
	}

	src, err := file.Open()
	if err != nil {
		return fmt.Errorf("failed to open zip entry: %w", err)
	}
	defer src.Close()

	dst, err := os.OpenFile(fullPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, file.Mode())
	if err != nil {
		return fmt.Errorf("failed to create file: %w", err)
	}
	defer dst.Close()

	if _, err := io.Copy(dst, src); err != nil {
		return fmt.Errorf("failed to write file: %w", err)
	}

	return nil
}

func indexJSONFiles(root string) ([]string, error) {
	var files []string
	if err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		if strings.HasSuffix(strings.ToLower(d.Name()), ".json") {
			rel, relErr := filepath.Rel(root, path)
			if relErr != nil {
				return relErr
			}
			files = append(files, filepath.ToSlash(rel))
		}
		return nil
	}); err != nil {
		return nil, err
	}

	return files, nil
}

func indexDatafeedFiles(root string) ([]string, error) {
	var files []string
	if err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}

		rel, relErr := filepath.Rel(root, path)
		if relErr != nil {
			return relErr
		}
		files = append(files, filepath.ToSlash(rel))
		return nil
	}); err != nil {
		return nil, err
	}

	return files, nil
}

type datafeedEntry struct {
	path string
	ext  string
}

func buildDatafeedFileMap(root string) (map[string]datafeedEntry, error) {
	files, err := os.ReadDir(root)
	if err != nil {
		return nil, err
	}

	fileMap := make(map[string]datafeedEntry)
	for _, entry := range files {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		ext := strings.ToLower(filepath.Ext(name))
		if ext != ".txt" && ext != ".csv" && ext != ".md" {
			continue
		}
		base := strings.TrimSuffix(name, ext)
		if _, exists := fileMap[base]; exists {
			continue
		}
		fileMap[base] = datafeedEntry{path: filepath.Join(root, name), ext: ext}
	}

	return fileMap, nil
}

func streamCSVAsJSON(path string, w io.Writer) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()

	reader := csv.NewReader(file)
	reader.FieldsPerRecord = -1
	reader.LazyQuotes = true

	headers, err := reader.Read()
	if err != nil {
		return err
	}

	if _, err := w.Write([]byte("[")); err != nil {
		return err
	}

	first := true
	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		row := make(map[string]string, len(headers))
		for i, header := range headers {
			if i < len(record) {
				row[header] = record[i]
			} else {
				row[header] = ""
			}
		}

		payload, err := json.Marshal(row)
		if err != nil {
			return err
		}

		if !first {
			if _, err := w.Write([]byte(",")); err != nil {
				return err
			}
		}
		first = false
		if _, err := w.Write(payload); err != nil {
			return err
		}
	}

	if _, err := w.Write([]byte("]")); err != nil {
		return err
	}

	return nil
}

func isSubpath(basePath, targetPath string) bool {
	rel, err := filepath.Rel(basePath, targetPath)
	if err != nil {
		return false
	}
	return rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator))
}

func readResponseBody(resp *http.Response) ([]byte, error) {
	encoding := strings.ToLower(strings.TrimSpace(resp.Header.Get("Content-Encoding")))

	switch encoding {
	case "", "identity":
		return io.ReadAll(resp.Body)
	case "gzip":
		reader, err := gzip.NewReader(resp.Body)
		if err != nil {
			return nil, err
		}
		defer reader.Close()
		return io.ReadAll(reader)
	case "deflate":
		reader, err := zlib.NewReader(resp.Body)
		if err != nil {
			return nil, err
		}
		defer reader.Close()
		return io.ReadAll(reader)
	default:
		return nil, fmt.Errorf("unsupported content-encoding: %s", encoding)
	}
}

func refreshDatafeeds() error {
	routeShapesCacheMu.Lock()
	routeShapesCache = nil
	routeShapesCacheMu.Unlock()

	shapesCacheMu.Lock()
	shapesCache = nil
	shapesOnce = sync.Once{}
	shapesCacheMu.Unlock()

	stopTimesOnce = sync.Once{}
	stopTimesCache = nil

	if _, err := os.Stat(datafeedsFilePath); err == nil {
		log.Println("Using existing datafeed file")
	} else {
		if err := downloadDatafeed(datafeedsFilePath); err != nil {
			return fmt.Errorf("failed to download datafeed: %w", err)
		}
	}

	if _, err := os.Stat(datafeedsDir); err == nil {
		log.Println("Using existing extracted datafeed directory")
	} else {
		if err := unzipDatafeed(datafeedsFilePath, datafeedsDir); err != nil {
			return fmt.Errorf("failed to unzip datafeed: %w", err)
		}
	}

	jsonFiles, err := indexJSONFiles(datafeedsDir)
	if err != nil {
		return fmt.Errorf("failed to index json files: %w", err)
	}

	files, err := indexDatafeedFiles(datafeedsDir)
	if err != nil {
		return fmt.Errorf("failed to index datafeed files: %w", err)
	}

	fileMap, err := buildDatafeedFileMap(datafeedsDir)
	if err != nil {
		return fmt.Errorf("failed to build datafeed file map: %w", err)
	}

	datafeedsJSONFiles = jsonFiles
	datafeedsFiles = files
	datafeedsFileMap = fileMap
	return nil
}

type TripInfo struct {
	trip_id               string
	route_id              string
	service_id            string
	trip_headsign         string
	direction_id          string
	shape_id              string
	block_id              string
	trip_short_name       string
	wheelchair_accessible string
	bikes_allowed         string
	trip_start_time       string
	trip_end_time         string
}

type StopTimeInfo struct {
	trip_id        string
	arrival_time   string
	departure_time string
	stop_id        string
	stop_sequence  int
}

type VehicleTypeInfo struct {
	Year     int    `json:"year"`
	Make     string `json:"make"`
	Model    string `json:"model"`
	Fuel     string `json:"fuel"`
	Length   int    `json:"length"`
	IconCode string `json:"icon_code"`
}

type VehicleTypeRange struct {
	Start string          `json:"start"`
	End   string          `json:"end"`
	Type  VehicleTypeInfo `json:"-"`
}

type AgencyVehicleMap struct {
	Ranges []struct {
		Start string `json:"start"`
		End   string `json:"end"`
		VehicleTypeInfo
	} `json:"ranges"`
	Exact map[string]VehicleTypeInfo `json:"exact"`
}

type VehicleTypeConfig struct {
	Agencies map[string]AgencyVehicleMap `json:"agencies"`
}

var (
	vehicleTypeConfig  *VehicleTypeConfig
	vehicleTypeOnce    sync.Once
	vehicleTypeCacheMu sync.RWMutex
)

func loadVehicleTypeConfig() *VehicleTypeConfig {
	vehicleTypeOnce.Do(func() {
		filePath := vehicleTypesFilePath
		if _, err := os.Stat(filePath); os.IsNotExist(err) {
			log.Println("vehicle_types.json not found, vehicle type enrichment disabled")
			vehicleTypeConfig = &VehicleTypeConfig{Agencies: make(map[string]AgencyVehicleMap)}
			return
		}

		data, err := os.ReadFile(filePath)
		if err != nil {
			log.Printf("failed to read vehicle_types.json: %v", err)
			vehicleTypeConfig = &VehicleTypeConfig{Agencies: make(map[string]AgencyVehicleMap)}
			return
		}

		var config VehicleTypeConfig
		if err := json.Unmarshal(data, &config); err != nil {
			log.Printf("failed to parse vehicle_types.json: %v", err)
			vehicleTypeConfig = &VehicleTypeConfig{Agencies: make(map[string]AgencyVehicleMap)}
			return
		}

		vehicleTypeConfig = &config
		log.Printf("Loaded vehicle types for %d agencies", len(config.Agencies))
	})

	return vehicleTypeConfig
}

func lookupVehicleType(agencyCode, vehicleID string) *VehicleTypeInfo {
	config := loadVehicleTypeConfig()
	if config == nil {
		return nil
	}

	agencyMap, ok := config.Agencies[agencyCode]
	if !ok {
		return nil
	}

	// Check exact matches first
	if exact, ok := agencyMap.Exact[vehicleID]; ok {
		return &exact
	}

	// Check ranges
	for _, r := range agencyMap.Ranges {
		if vehicleID >= r.Start && vehicleID <= r.End {
			return &VehicleTypeInfo{
				Year:     r.Year,
				Make:     r.Make,
				Model:    r.Model,
				Fuel:     r.Fuel,
				Length:   r.Length,
				IconCode: r.IconCode,
			}
		}
	}

	return nil
}

type StopInfo struct {
	stop_id   string
	stop_name string
	stop_lat  string
	stop_lon  string
}

type BlockScheduleEntry struct {
	TripID               string `json:"trip_id"`
	RouteID              string `json:"route_id"`
	RouteShortName       string `json:"route_short_name"`
	RouteLongName        string `json:"route_long_name"`
	DirectionID          string `json:"direction_id"`
	StartTime            string `json:"start_time"`
	EndTime              string `json:"end_time"`
	HeadSign             string `json:"headsign"`
	StartStopName        string `json:"start_stop_name"`
	EndStopName          string `json:"end_stop_name"`
	ShapeID              string `json:"shape_id"`
	WheelchairAccessible string `json:"wheelchair_accessible"`
	BikesAllowed         string `json:"bikes_allowed"`
}

var (
	tripsCache     map[string]TripInfo
	stopsCache     map[string]StopInfo
	routesCache    map[string]RouteInfo
	stopTimesCache map[string][]StopTimeInfo
	tripsOnce      sync.Once
	stopsOnce      sync.Once
	routesOnce     sync.Once
	stopTimesOnce  sync.Once
)

type RouteInfo struct {
	shortName string
	longName  string
}

func loadTripsData() map[string]TripInfo {
	tripsOnce.Do(func() {
		tripsCache = make(map[string]TripInfo)
		filePath := filepath.Join(datafeedsDir, "trips.txt")
		if _, err := os.Stat(filePath); err == nil {
			file, err := os.Open(filePath)
			if err == nil {
				defer file.Close()
				reader := csv.NewReader(file)
				reader.FieldsPerRecord = -1
				reader.LazyQuotes = true
				headers, _ := reader.Read()
				headerMap := make(map[string]int)
				for i, h := range headers {
					headerMap[h] = i
				}
				for {
					record, err := reader.Read()
					if err == io.EOF {
						break
					}
					if err != nil {
						continue
					}
					get := func(field string) string {
						if idx, ok := headerMap[field]; ok && idx < len(record) {
							return record[idx]
						}
						return ""
					}
					trip := TripInfo{
						trip_id:               get("trip_id"),
						route_id:              get("route_id"),
						service_id:            get("service_id"),
						trip_headsign:         get("trip_headsign"),
						direction_id:          get("direction_id"),
						shape_id:              get("shape_id"),
						block_id:              get("block_id"),
						trip_short_name:       get("trip_short_name"),
						wheelchair_accessible: get("wheelchair_accessible"),
						bikes_allowed:         get("bikes_allowed"),
						trip_start_time:       "",
						trip_end_time:         "",
					}
					if trip.trip_id != "" {
						tripsCache[trip.trip_id] = trip
					}
				}
			}
		}
		// Trigger the shared stop_times cache load, then populate start/end times
		_ = loadStopTimesForTrip("")
		for tid, times := range stopTimesCache {
			if len(times) > 0 {
				if trip, ok := tripsCache[tid]; ok {
					trip.trip_start_time = times[0].departure_time
					trip.trip_end_time = times[len(times)-1].arrival_time
					tripsCache[tid] = trip
				}
			}
		}
		log.Printf("Updated trips with start/end times from stop_times cache")
	})

	return tripsCache
}

func loadStopTimesForTrip(tripID string) []StopTimeInfo {
	stopTimesOnce.Do(func() {
		stopTimesCache = make(map[string][]StopTimeInfo)
		filePath := filepath.Join(datafeedsDir, "stop_times.txt")
		if _, err := os.Stat(filePath); err != nil {
			return
		}

		file, err := os.Open(filePath)
		if err != nil {
			log.Printf("failed to open stop_times.txt: %v", err)
			return
		}
		defer file.Close()

		reader := csv.NewReader(file)
		reader.FieldsPerRecord = -1
		reader.LazyQuotes = true

		headers, err := reader.Read()
		if err != nil {
			log.Printf("failed to read stop_times.txt headers: %v", err)
			return
		}
		headerMap := make(map[string]int)
		for i, h := range headers {
			headerMap[h] = i
		}

		get := func(field string, rec []string) string {
			if idx, ok := headerMap[field]; ok && idx < len(rec) {
				return rec[idx]
			}
			return ""
		}

		for {
			var rec []string
			var err error
			func() {
				defer func() {
					if r := recover(); r != nil {
						log.Printf("panic reading stop_times.csv: %v", r)
						err = fmt.Errorf("panic: %v", r)
					}
				}()
				rec, err = reader.Read()
			}()
			if err == io.EOF {
				break
			}
			if err != nil {
				continue
			}

			tid := get("trip_id", rec)
			if tid == "" {
				continue
			}

			seq := 0
			if s := get("stop_sequence", rec); s != "" {
				fmt.Sscanf(s, "%d", &seq)
			}
			stopTimesCache[tid] = append(stopTimesCache[tid], StopTimeInfo{
				trip_id:        tid,
				arrival_time:   get("arrival_time", rec),
				departure_time: get("departure_time", rec),
				stop_id:        get("stop_id", rec),
				stop_sequence:  seq,
			})
		}

		log.Printf("Loaded %d stop_times entries for %d trips", len(stopTimesCache), len(stopTimesCache))
	})

	return stopTimesCache[tripID]
}

func loadStopsData() map[string]StopInfo {
	stopsOnce.Do(func() {
		stopsCache = make(map[string]StopInfo)
		filePath := filepath.Join(datafeedsDir, "stops.txt")
		if _, err := os.Stat(filePath); err == nil {
			file, err := os.Open(filePath)
			if err == nil {
				defer file.Close()
				reader := csv.NewReader(file)
				reader.FieldsPerRecord = -1
				reader.LazyQuotes = true
				headers, _ := reader.Read()
				headerMap := make(map[string]int)
				for i, h := range headers {
					headerMap[h] = i
				}
				for {
					record, err := reader.Read()
					if err == io.EOF {
						break
					}
					if err != nil {
						continue
					}
					get := func(field string) string {
						if idx, ok := headerMap[field]; ok && idx < len(record) {
							return record[idx]
						}
						return ""
					}
					stop := StopInfo{
						stop_id:   get("stop_id"),
						stop_name: get("stop_name"),
						stop_lat:  get("stop_lat"),
						stop_lon:  get("stop_lon"),
					}
					if stop.stop_id != "" {
						stopsCache[stop.stop_id] = stop
					}
				}
			}
		}
	})
	return stopsCache
}

func loadRoutesData() {
	routesOnce.Do(func() {
		routesCache = make(map[string]RouteInfo)
		filePath := filepath.Join(datafeedsDir, "routes.txt")
		if _, err := os.Stat(filePath); err == nil {
			file, err := os.Open(filePath)
			if err == nil {
				defer file.Close()
				reader := csv.NewReader(file)
				reader.FieldsPerRecord = -1
				reader.LazyQuotes = true
				headers, _ := reader.Read()
				headerMap := make(map[string]int)
				for i, h := range headers {
					headerMap[h] = i
				}
				for {
					record, err := reader.Read()
					if err == io.EOF {
						break
					}
					if err != nil {
						continue
					}
					get := func(field string) string {
						if idx, ok := headerMap[field]; ok && idx < len(record) {
							return record[idx]
						}
						return ""
					}
					routeID := get("route_id")
					routeShortName := get("route_short_name")
					routeLongName := get("route_long_name")
					if routeID != "" && routeShortName != "" {
						routesCache[routeID] = RouteInfo{shortName: routeShortName, longName: routeLongName}
					}
				}
			}
		}
	})
}

func getRouteShortName(routeID string) string {
	loadRoutesData()
	if route, ok := routesCache[routeID]; ok {
		return route.shortName
	}
	if idx := strings.LastIndex(routeID, ":"); idx >= 0 {
		return routeID[idx+1:]
	}
	return routeID
}

func getRouteLongName(routeID string) string {
	loadRoutesData()
	if route, ok := routesCache[routeID]; ok {
		return route.longName
	}
	if idx := strings.LastIndex(routeID, ":"); idx >= 0 {
		return routeID[idx+1:]
	}
	return routeID
}

func vehicleTypesHandler(w http.ResponseWriter, r *http.Request) {
	config := loadVehicleTypeConfig()
	if config == nil {
		http.Error(w, "vehicle types not loaded", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "public, max-age=3600")
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	if err := enc.Encode(config); err != nil {
		http.Error(w, "failed to encode response", http.StatusInternalServerError)
		return
	}
}

func runDatafeedsRefresher() {
	ticker := time.NewTicker(datafeedsRefreshInterval)
	defer ticker.Stop()
	for range ticker.C {
		if err := refreshDatafeeds(); err != nil {
			log.Printf("datafeeds refresh failed: %v", err)
		}
	}
}

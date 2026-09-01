# Headways API

**Endpoint:** `POST http://headwaysapi.rkmr.dev/api`

## Request Format

```json
{
  "query": "GraphQL query string",
  "operationName": "optional, for named operations",
  "variables": { "key": "value" }
}
```

## Response Format

```json
{
  "data": { ... },
  "errors": [{ "message": "...", "locations": [...], "path": [...] }]
}
```

---

## Quick Examples

### All Agencies

```bash
curl -X POST http://localhost:8081/api \
  -H 'Content-Type: application/json' \
  -d '{"query": "{ agencies { agency_id agency_name agency_url } }"}'
```

### Live Vehicle Feed

```bash
curl -X POST http://localhost:8081/api \
  -H 'Content-Type: application/json' \
  -d '{"query": "{ vehicleFeed { fetchedAt data { id vehicle { trip { tripId routeId delay } position { latitude longitude } routeShortName } } } }"}'
```

### Trip Detail with Schedule and Shape

```bash
curl -X POST http://localhost:8081/api \
  -H 'Content-Type: application/json' \
  -d '{"query": "{ tripDetail(tripId: \"12345\") { trip_id route_id shape schedule { stop_id stop_name arrival_time } } }"}'
```

### Stop Departures

```bash
curl -X POST http://localhost:8081/api \
  -H 'Content-Type: application/json' \
  -d '{"query": "{ stop(stopId: \"70011\") { stop_name departures { route_short_name trip_headsign departure_time departure_timestamp } } }"}'
```

## Queries

### `agencies`

Returns all transit agencies. No arguments.

```graphql
{
  agencies {
    agency_id
    agency_name
    agency_url
    agency_timezone
    agency_lang
    agency_phone
    agency_fare_url
    agency_email
  }
}
```

### `routes`

Returns all transit routes. No arguments.

```graphql
{
  routes {
    route_id
    agency_id
    route_short_name
    route_long_name
    route_type
    route_color
    route_text_color
    route_url
    route_desc
  }
}
```

### `stops`

Returns all stops. Coordinates are strings (e.g. `"37.7749"`). No arguments.

```graphql
{
  stops {
    stop_id
    stop_name
    stop_lat
    stop_lon
  }
}
```

### `stopGroups`

Returns nearby stops merged into station groups. `stop_lat`/`stop_lon` are floats. No arguments.

```graphql
{
  stopGroups {
    group_id
    group_name
    stop_lat
    stop_lon
    route_id
  }
}
```

### `stop`

Returns a single stop with up to 30 upcoming departures. If `stopId` matches a station group, departures are merged across all member stops.

| Argument | Type      | Required | Description         |
| -------- | --------- | -------- | ------------------- |
| `stopId` | `String!` | Yes      | Stop ID or group ID |

```graphql
{
  stop(stopId: "70011") {
    stop_id
    stop_name
    stop_lat
    stop_lon
    departures {
      trip_id
      route_id
      route_short_name
      trip_headsign
      direction_id
      arrival_time
      departure_time
      departure_timestamp
    }
  }
}
```

`departure_timestamp` is a Unix epoch integer (seconds). `arrival_time`/`departure_time` are GTFS strings (e.g. `"14:30:00"`, may exceed `24:00` for overnight service).

### `trips`

Returns all trips. Cached in memory after first call. No arguments.

```graphql
{
  trips {
    trip_id
    route_id
    service_id
    trip_headsign
    direction_id
    shape_id
    block_id
    trip_short_name
    wheelchair_accessible
    bikes_allowed
  }
}
```

### `vehicleFeed`

Returns current cached vehicle positions from the 511 GTFS-RT feed, enriched with GTFS trip data and vehicle type info. No arguments.

```graphql
{
  vehicleFeed {
    fetchedAt
    data {
      entity {
        id
        vehicle {
          trip {
            tripId
            routeId
            directionId
            delay
            startTime
            startDate
            scheduleRelationship
            tripInfoFound
            tripHeadsign
            serviceId
            shapeId
            blockId
            tripShortName
          }
          position {
            latitude
            longitude
            bearing
            speed
          }
          timestamp
          stopId
          currentStopSequence
          occupancyStatus
          stopName
          vehicle {
            id
            label
          }
          vehicleYear
          vehicleMake
          vehicleModel
          vehicleFuel
          vehicleLength
          vehicleIconCode
          routeShortName
        }
      }
    }
  }
}
```

`fetchedAt` is RFC3339Nano. `delay` is seconds (positive = late, negative = early). `occupancyStatus` is one of: `EMPTY`, `MANY_SEATS_AVAILABLE`, `FEW_SEATS_AVAILABLE`, `STANDING_ROOM_ONLY`, `CRUSHED_STANDING_ROOM_ONLY`, `NOT_ACCEPTING_PASSENGERS`, `NO_DATA_SOURCE`. Vehicle type fields come from `vehicle_types.json` matched by vehicle ID.

### `tripDetail`

Returns full trip details: metadata, shape polyline, and scheduled stops. Tries a pre-computed JSON file first, falls back to raw GTFS data.

| Argument | Type      | Required | Description |
| -------- | --------- | -------- | ----------- |
| `tripId` | `String!` | Yes      | Trip ID     |

```graphql
{
  tripDetail(tripId: "12345") {
    trip_id
    route_id
    service_id
    trip_headsign
    direction_id
    shape_id
    block_id
    trip_short_name
    shape
    schedule {
      stop_id
      stop_sequence
      arrival_time
      departure_time
      stop_name
      stop_lat
      stop_lon
    }
  }
}
```

`shape` is `[[lat, lon], ...]`. `schedule` is ordered by `stop_sequence`.

### `shape`

Returns shape polyline coordinates. No match returns `[]`.

| Argument  | Type      | Required | Description        |
| --------- | --------- | -------- | ------------------ |
| `shapeId` | `String!` | Yes      | Shape ID from GTFS |

```graphql
{
  shape(shapeId: "shape_123")
}
```

Returns `[[lat, lon], ...]`.

---

## Types

```graphql
type Agency {
  agency_id: String
  agency_name: String
  agency_url: String
  agency_timezone: String
  agency_lang: String
  agency_phone: String
  agency_fare_url: String
  agency_email: String
}

type Route {
  route_id: String
  agency_id: String
  route_short_name: String
  route_long_name: String
  route_type: String
  route_color: String
  route_text_color: String
  route_url: String
  route_desc: String
}

type Stop {
  stop_id: String
  stop_name: String
  stop_lat: String
  stop_lon: String
}

type StopGroup {
  group_id: String
  group_name: String
  stop_lat: Float
  stop_lon: Float
  route_id: String
}

type StopDetail {
  stop_id: String
  stop_name: String
  stop_lat: String
  stop_lon: String
  departures: [Departure]
}

type Departure {
  trip_id: String
  route_id: String
  route_short_name: String
  trip_headsign: String
  direction_id: String
  arrival_time: String
  departure_time: String
  departure_timestamp: Int
}

type Trip {
  trip_id: String
  route_id: String
  service_id: String
  trip_headsign: String
  direction_id: String
  shape_id: String
  block_id: String
  trip_short_name: String
  wheelchair_accessible: String
  bikes_allowed: String
}

type TripDetail {
  trip_id: String
  route_id: String
  service_id: String
  trip_headsign: String
  direction_id: String
  shape_id: String
  block_id: String
  trip_short_name: String
  shape: [[Float]]
  schedule: [StopTime]
}

type StopTime {
  stop_id: String
  stop_sequence: Int
  arrival_time: String
  departure_time: String
  stop_name: String
  stop_lat: String
  stop_lon: String
}

type VehicleFeedEnvelope {
  fetchedAt: String
  data: VehicleFeedData
}

type VehicleFeedData {
  entity: [Entity]
}

type Entity {
  id: String
  vehicle: VehicleData
}

type VehicleData {
  trip: TripData
  position: Position
  timestamp: Int
  stopId: String
  currentStopSequence: Int
  occupancyStatus: String
  stopName: String
  vehicle: VehicleDescriptor
  vehicleYear: Int
  vehicleMake: String
  vehicleModel: String
  vehicleFuel: String
  vehicleLength: Int
  vehicleIconCode: String
  routeShortName: String
}

type TripData {
  tripId: String
  routeId: String
  directionId: Int
  delay: Int
  startTime: String
  startDate: String
  scheduleRelationship: String
  tripInfoFound: Boolean
  tripHeadsign: String
  serviceId: String
  shapeId: String
  blockId: String
  tripShortName: String
}

type Position {
  latitude: Float
  longitude: Float
  bearing: Float
  speed: Float
}

type VehicleDescriptor {
  id: String
  label: String
}
```

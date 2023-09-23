module bahn.data

open System
open System.IO
open System.Runtime.InteropServices
open Microsoft.Data.Sqlite

[<Literal>]
let createTablesScript = (*lang=sql*)
    """
  BEGIN;
  CREATE TABLE IF NOT EXISTS agencies (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    url TEXT NOT NULL,
    timezone TEXT NOT NULL,
    language TEXT,
    phone TEXT,
    fare_url TEXT,
    email TEXT
  );

  CREATE TABLE IF NOT EXISTS stops (
    id TEXT PRIMARY KEY,
    code TEXT,
    name TEXT,
    text_to_speech_name TEXT,
    description TEXT,
    latitude TEXT,
    longitude TEXT,
    zone_id TEXT,
    url TEXT,
    location_type INTEGER,
    parent_station TEXT,
    timezone TEXT,
    wheelchair_boarding INTEGER,
    level_id TEXT,
    platform_code TEXT--,
    --FOREIGN KEY(level_id) REFERENCES levels(id),
  );

  CREATE TABLE IF NOT EXISTS routes (
    id TEXT PRIMARY KEY,
    agency_id TEXT,
    short_name TEXT,
    long_name TEXT,
    description TEXT,
    type INTEGER NOT NULL,
    url TEXT,
    color TEXT,
    text_color TEXT,
    sort_order INTEGER,
    continuous_pickup INTEGER,
    continuous_drop_off INTEGER,
    network_id TEXT,
    FOREIGN KEY(agency_id) REFERENCES agencies(id)
  );

  CREATE TABLE IF NOT EXISTS trips (
    id TEXT PRIMARY KEY,
    route_id TEXT NOT NULL,
    service_id TEXT NOT NULL,
    headsign TEXT,
    short_name TEXT,
    direction BOOLEAN,
    block_id TEXT,
    shape_id TEXT,
    wheelchair_accessible INTEGER,
    bikes_allowed INTEGER,
    FOREIGN KEY(route_id) REFERENCES routes(id)--,
    --FOREIGN KEY(service_id) REFERENCES services(id),
    --FOREIGN KEY(shape_id) REFERENCES shapes(id)
  );

  CREATE TABLE IF NOT EXISTS stop_times (
    trip_id TEXT,
    arrival_time TEXT,
    departure_time TEXT,
    stop_id TEXT,
    stop_sequence INTEGER,
    stop_headsign TEXT,
    pickup_type INTEGER,
    drop_off_type INTEGER,
    continuous_pickup INTEGER,
    continuous_drop_off INTEGER,
    shape_distance_traveled REAL,
    timepoint INTEGER,
    PRIMARY KEY (trip_id, stop_id, stop_sequence),
    FOREIGN KEY(trip_id) REFERENCES trips(id),
    FOREIGN KEY(stop_id) REFERENCES stops(id)
  );

  COMMIT;
  """

let openDatabase () =
    task {
        let path = Path.Combine(__SOURCE_DIRECTORY__, "./database.db")
        let builder = SqliteConnectionStringBuilder(ForeignKeys = true, DataSource = path)
        let connection = new SqliteConnection(builder.ConnectionString)

        do! connection.OpenAsync()
        use command = new SqliteCommand(createTablesScript, connection)
        let! _ = command.ExecuteNonQueryAsync()

        return connection
    }

open bahn.csv


let add name value (parameters: SqliteParameterCollection) =

    parameters.AddWithValue(name, value) |> ignore
    parameters

let addOption name value (parameters: SqliteParameterCollection) =
    match value with
    | Some value -> parameters.AddWithValue(name, value) |> ignore
    | None -> parameters.AddWithValue(name, DBNull.Value) |> ignore

    parameters

[<Literal>]
let insertAgencyQuery =
    """
    INSERT OR IGNORE INTO agencies
    VALUES (
      :id,
      :name,
      :url,
      :timezone,
      :language,
      :phone,
      :fare_url,
      :email)
    """

type Agency =
    { id: string
      name: string
      url: string
      timezone: string
      language: string option
      phone: string option
      fareUrl: string option
      email: string option }

let saveAgency (agency: Agency) (connection: SqliteConnection) =
    task {
        use command = new SqliteCommand(insertAgencyQuery, connection)

        command.Parameters
        |> add ":id" agency.id
        |> add ":name" agency.name
        |> add ":url" agency.url
        |> add ":timezone" agency.timezone
        |> addOption ":language" agency.language
        |> addOption ":phone" agency.phone
        |> addOption ":fare_url" agency.fareUrl
        |> addOption ":email" agency.email
        |> ignore

        let! _ = command.ExecuteNonQueryAsync()
        return ()
    }

[<Literal>]
let insertRouteQuery =
    """
    INSERT OR IGNORE INTO routes
    VALUES (
        :id,
        :agency_id,
        :short_name,
        :long_name,
        :description,
        :type,
        :url,
        :color,
        :text_color,
        :sort_order,
        :continuous_pickup,
        :continuous_drop_off,
        :network_id)
    """

type Route =
    { id: string
      agencyId: string option
      shortName: string option
      longName: string option
      description: string option
      ``type``: uint8
      url: string option
      color: string option
      textColor: string option
      sort_order: uint option
      continuousPickup: uint8 option
      continuousDropOff: uint8 option
      networkId: string option }

let saveRoute (route: Route) (connection: SqliteConnection) =
    task {
        use command = new SqliteCommand(insertRouteQuery, connection)

        command.Parameters
        |> add ":id" route.id
        |> addOption ":agency_id" route.agencyId
        |> addOption ":short_name" route.shortName
        |> addOption ":long_name" route.longName
        |> addOption ":description" route.description
        |> add ":type" route.``type``
        |> addOption ":url" route.url
        |> addOption ":color" route.color
        |> addOption ":text_color" route.textColor
        |> addOption ":sort_order" route.sort_order
        |> addOption ":continuous_pickup" route.continuousPickup
        |> addOption ":continuous_drop_off" route.continuousDropOff
        |> addOption ":network_id" route.networkId
        |> ignore

        let! _ = command.ExecuteNonQueryAsync()
        return ()
    }

[<Literal>]
let insertStopQuery =
    """
    INSERT OR IGNORE INTO stops
    VALUES (
        :id,
        :code,
        :name,
        :text_to_speech_name,
        :description,
        :latitude,
        :longitude,
        :zone_id,
        :url,
        :location_type,
        :parent_station,
        :timezone,
        :wheelchair_boarding,
        :level_id,
        :platform_code)
    """

type Stop =
    { id: string
      code: string option
      name: string option
      textToSpeechName: string option
      description: string option
      latitude: string option
      longitude: string option
      zoneId: string option
      url: string option
      locationType: uint8 option
      parentStation: string option
      timezone: string option
      wheelchairBoarding: uint8 option
      levelId: string option
      platformCode: string option }


let saveStop (stop: Stop) (connection: SqliteConnection) =
    task {
        use command = new SqliteCommand(insertStopQuery, connection)

        command.Parameters
        |> add ":id" stop.id
        |> addOption ":code" stop.code
        |> addOption ":name" stop.name
        |> addOption ":text_to_speech_name" stop.textToSpeechName
        |> addOption ":description" stop.description
        |> addOption ":latitude" stop.latitude
        |> addOption ":longitude" stop.longitude
        |> addOption ":zone_id" stop.zoneId
        |> addOption ":url" stop.url
        |> addOption ":location_type" stop.locationType
        |> addOption ":parent_station" stop.parentStation
        |> addOption ":timezone" stop.timezone
        |> addOption ":wheelchair_boarding" stop.wheelchairBoarding
        |> addOption ":level_id" stop.levelId
        |> addOption ":platform_code" stop.platformCode
        |> ignore

        let! _ = command.ExecuteNonQueryAsync()
        return ()
    }


type StopTime =
    { tripId: string
      arrivalTime: string option
      departureTime: string option
      stopId: string
      stopSequence: uint
      stopHeadsign: string option
      pickupType: uint8 option
      dropOffType: uint8 option
      continuousPickup: uint8 option
      continuousDropOff: uint8 option
      shapeDistanceTravelled: float option
      timepoint: uint8 option }

[<Literal>]
let insertStopTimeQuery =
    """
    INSERT OR IGNORE INTO stop_times
    VALUES (
        :trip_id,
        :arrival_time,
        :departure_time,
        :stop_id,
        :stop_sequence,
        :stop_headsign,
        :pickup_type,
        :drop_off_type,
        :continuous_pickup,
        :continuous_drop_off,
        :shape_distance_traveled,
        :timepoint)
    """

let saveStopTime (stopTime: StopTime) (connection: SqliteConnection) =
    task {
        use command = new SqliteCommand(insertStopTimeQuery, connection)

        command.Parameters
        |> add ":trip_id" stopTime.tripId
        |> addOption ":arrival_time" stopTime.arrivalTime
        |> addOption ":departure_time" stopTime.departureTime
        |> add ":stop_id" stopTime.stopId
        |> add ":stop_sequence" stopTime.stopSequence
        |> addOption ":stop_headsign" stopTime.stopHeadsign
        |> addOption ":pickup_type" stopTime.pickupType
        |> addOption ":drop_off_type" stopTime.dropOffType
        |> addOption ":continuous_pickup" stopTime.continuousPickup
        |> addOption ":continuous_drop_off" stopTime.continuousDropOff
        |> addOption ":shape_distance_traveled" stopTime.shapeDistanceTravelled
        |> addOption ":timepoint" stopTime.timepoint
        |> ignore

        let! _ = command.ExecuteNonQueryAsync()
        return ()
    }

type Trip =
    { routeId: string
      serviceId: string
      id: string
      headsign: string option
      shortName: string option
      directionId: uint8 option
      blockId: string option
      shapeId: string option
      wheelchairAccessible: uint8 option
      bikesAllowed: uint8 option }

[<Literal>]
let insertTripQuery =
    """
    INSERT OR IGNORE INTO trips
    VALUES (
        :id,
        :route_id,
        :service_id,
        :headsign,
        :short_name,
        :direction,
        :block_id,
        :shape_id,
        :wheelchair_accessible,
        :bikes_allowed)
    """


let saveTrip (trip: Trip) (connection: SqliteConnection) =
    task {
        use command = new SqliteCommand(insertTripQuery, connection)

        command.Parameters
        |> add ":id" trip.id
        |> add ":route_id" trip.routeId
        |> add ":service_id" trip.serviceId
        |> addOption ":headsign" trip.headsign
        |> addOption ":short_name" trip.shortName
        |> addOption ":direction" trip.directionId
        |> addOption ":block_id" trip.blockId
        |> addOption ":shape_id" trip.shapeId
        |> addOption ":wheelchair_accessible" trip.wheelchairAccessible
        |> addOption ":bikes_allowed" trip.bikesAllowed
        |> ignore

        let! _ = command.ExecuteNonQueryAsync()
        return ()
    }

let testQuery = (*lang=sql*) """
-- Get all stops and their departure time of trips that run through desired station
SELECT
	routes.short_name,
	trips.headsign,
	stops.name,
	stop_times.departure_time,
	stop_times.stop_sequence
FROM stop_times
JOIN stops
	ON stops.id = stop_times.stop_id
	-- Only look at trips that have desired station in them
	AND stop_times.trip_id IN (
	-- Get trips that have the desired station in them
	SELECT trips.id
	FROM trips
	JOIN stop_times
		ON stop_times.trip_id = trips.id
	JOIN stops
		ON stops.id = stop_times.stop_id
		AND stops.name = "Sankt Augustin Fährstr."
)

JOIN trips
	ON trips.id = stop_times.trip_id
JOIN routes
	ON routes.id = trips.route_id
	
ORDER BY stop_times.trip_id,  stop_times.stop_sequence
"""
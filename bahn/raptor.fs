module bahn.raptor

open System
open System.Collections.Generic
open System.IO
open System.Reflection.Emit
open Fumble
open Microsoft.Data.Sqlite
open Microsoft.FSharp.Core
open bahn.data

// Experiment with implementing RAPTOR algorithms

let unwrap<'T, 'E> (result: Result<'T, 'E>) =
    match result with
    | Ok value -> value
    | Error error -> failwith $"Failed to unwrap: {error}"

/// <summary>
/// The number of trips taken in a given journey. The number of trips - 1 is the number of transfers between trips taken
/// </summary>
[<Measure>]
type NumberOfTrips

[<Measure>]
type Time

let infiniteTime = infinity * 1.0<Time>

[<Measure>]
type StopSequence


module NaiveRaptor =
    type Trip = { id: string }
    type Stop = { id: string }
    /// <summary>
    /// Describes a Line or Route in a public transport network which can have trips to different times of the day.
    /// Trips belong to a route if they visit stops in the same sequence.
    /// </summary>
    /// <remarks>
    /// The concept of route differs from the GTFS definition of a route. In GTFS a route can have trips which can take
    /// different paths and has different stops whereas with RAPTOR each trip that takes a different path is its own route.
    /// Additionally a route can visit the same stops but in a different direction where the route in GTFS is directionless
    /// </remarks>
    type Route = { id: string; direction: uint8 }


    module Stop =
        let getRoutesAtStop (connection: Sql.SqlProps) (stop: Stop) : Route array =
            connection
            |> Sql.query
                """
                SELECT DISTINCT 
                    trips.route_id,
                    trips.direction
                FROM
                    trips
                JOIN stop_times 
                    ON trips.id = stop_times.trip_id
                    AND stop_times.stop_id = :stop_id
                """
            |> Sql.parameters [ ":stop_id", Sql.string stop.id ]
            |> Sql.execute (fun read ->
                { Route.id = read.string "route_id"
                  direction = read.int "direction" |> uint8 })
            |> unwrap
            |> Array.ofList

    module Trip =
        /// <summary>
        /// Gets the stops and their arrival times after the specified stop for the specified trip
        /// </summary>
        let getStopsAfter (stop: Stop) (connection: Sql.SqlProps) (trip: Trip) : (Stop * float<Time>) array =

            connection
            |> Sql.query
                """
                SELECT
                    stop_times.stop_id,
                    stop_times.departure_time_seconds
                FROM
                    stop_times
                WHERE
                    stop_times.trip_id = :trip_id
                    AND stop_times.stop_sequence > (SELECT stop_sequence FROM stop_times WHERE stop_id = :stop_id)
                """
            |> Sql.parameters [ ":trip_id", Sql.string trip.id; ":stop_id", Sql.string stop.id ]
            |> Sql.execute (fun read -> ({Stop.id = read.string "stop_id"}, read.float "departure_time_seconds" * 1.0<Time>))
            |> unwrap
            |> Array.ofList


        /// <summary>
        /// Is it possible to exit the trip at the stop
        /// </summary>
        let isDropOff stop trip : bool =
            //TODO implement
            true
            
    module Route =
        /// <summary>
        /// Gets the earliest trip that can be boarded from a stop for the route
        /// </summary>
        let getEarliestTrip (stop: Stop) (after: float<Time>) (connection: Sql.SqlProps) (route: Route) : Trip =
            // The earliest trip is when departure time for that trip at the stop is greater than the arrival time for
            // the last round for that stop (...it departs after we arrive)

            connection
            |> Sql.query
                """
                SELECT 
                    trips.id
                FROM
                    trips
                JOIN stop_times 
                    ON trips.id = stop_times.trip_id
                    AND departure_time_seconds > :departure_time
                    AND stop_times.stop_id = :stop_id
                WHERE
                    trips.route_id = :route_id
                    AND trips.direction = :direction
                LIMIT 1
                """
            |> Sql.parameters
                // There should only be full seconds
                [ ":departure_time", after |> int |> Sql.int
                  ":stop_id", Sql.string stop.id
                  ":route_id", Sql.string route.id
                  ":direction", route.direction |> int |> Sql.int ]
            |> Sql.execute (fun read -> { Trip.id = read.string "id" })
            |> unwrap
            |> List.head

        let getStopSequence (stop:Stop) (connection: Sql.SqlProps) (route:Route) : uint =
            connection
            |> Sql.query
                """
                SELECT
                    stop_sequence
                FROM
                    stop_times
                JOIN
                    trips
                    ON stop_times.trip_id = trips.id
                    AND trips.direction = :direction
                    AND trips.route_id = :route_id
                WHERE
                    stop_id = :stop_id
                    
                    
                LIMIT 1
                """
            |> Sql.parameters
                // There should only be full seconds
                [ ":departure_time", after |> int |> Sql.int
                  ":stop_id", Sql.string stop.id
                  ":route_id", Sql.string route.id
                  ":direction", route.direction |> int |> Sql.int ]
            |> Sql.execute (fun read -> read.int )
            |> unwrap
            |> List.head

    type MultiLabel = Dictionary<uint<NumberOfTrips>, float<Time>>

    module MultiLabel =

        // let infinity = infinity<Time>
        let set numberOfTrips time (multiLabel: MultiLabel) =
            multiLabel[numberOfTrips] <- time
            multiLabel

        let get numberOfTrips (multiLabel: MultiLabel) =
            multiLabel.GetValueOrDefault(numberOfTrips, infiniteTime)

        ()

    /// <summary>
    /// A connection between two stops
    /// </summary>
    type Connection =
        /// <summary>
        /// By using a trip
        /// </summary>
        | Connection of trip: Trip * enter: Stop * exit: Stop
        /// <summary>
        /// By walking between stops
        /// </summary>
        | FootPath of source: Stop * target: Stop

    let raptorNaive
        (source: Stop)
        (target: Stop)
        (departure: float<Time>)
        (footPathsByStop: Dictionary<Stop, Dictionary<Stop, float<Time>>>)
        (connection: Sql.SqlProps)
        =

        let labelsByStop = Dictionary<Stop, MultiLabel>()

        labelsByStop.Add(source, MultiLabel() |> MultiLabel.set 0u<NumberOfTrips> departure)

        let earliestKnownArrivalByStop = Dictionary<Stop, float<Time>>()
        let connections = Dictionary<Stop, Dictionary<uint<NumberOfTrips>, Connection>>()

        // The number of trips
        let mutable k = 0u<NumberOfTrips>

        // Marked stops are stops for which we improved the arrival time
        // they determine which routes we inspect for the next trip

        let markedStops = HashSet<Stop>()
        markedStops.Add source |> ignore


        let queue = Dictionary<Route, Stop>()

        while markedStops.Count > 0 do
            k <- k + 1u<NumberOfTrips>
            // Accumulate routes serving marked stops from previous round
            // Could fetch the routes for the stop inside the markedStops loop to maybe parallelize it
            let routesByStop =
                markedStops |> Seq.map (fun stop -> stop, Stop.getRoutesAtStop connection stop) |> Map

            // Routes to scan and their earliest marked stop from which to start scanning
            queue.Clear()

            for p in markedStops do
                let routesServingP = routesByStop |> Map.find p

                for route in routesServingP do
                    if queue.ContainsKey route then
                        //
                        let p' = queue[route]
                        //TODO check if "comes before/after" is meant with stop sequence or stop time for a trip
                        // If p' comes after p (stop in markedStops) replace p' with p
                        // This ensures we only scan a route starting at its first marked stop
                        if route |> Route.getStopSequence p < (route |> Route.getStopSequence p') then
                            queue[route] <- p
                    else
                        queue.Add(route, p)


                markedStops.Remove p |> ignore

            for KeyValue(route, stop) in queue do

                // Trip and a pointer to when we boarded the trip to reconstruct the journey

                let earliestArrival = labelsByStop[stop] |> MultiLabel.get (k - 1u<NumberOfTrips>)
                // Find earliest trip
                let mutable currentTrip = route |> Route.getEarliestTrip stop earliestArrival connection
                let mutable boardedAt = stop

                // "hop on"
                // Traverse
                let subsequentStop = currentTrip |> Trip.getStopsAfter stop connection

                //TODO add minimum transfer time to arrival as it would probably not be possible to catch otherwise
                for tripStop, arrival in subsequentStop do
                    let previousArrival =
                        earliestKnownArrivalByStop.GetValueOrDefault(tripStop, infiniteTime)
                    // Target pruning: If we know the best arrival time for target stop already, then we don't need to
                    // follow the trip anymore if we arrive at all future stops later than the found optimal time
                    let targetStopArrival =
                        earliestKnownArrivalByStop.GetValueOrDefault(target, infiniteTime)

                    // We don't need to check the stops before the stop we hopped on as we got the earliest stop for
                    // the route in the loop that added route by stop to the queue

                    if
                        currentTrip |> Trip.isDropOff tripStop
                        && arrival < min previousArrival targetStopArrival
                    then
                        // Update earliest arrival for stop
                        labelsByStop[tripStop] <-
                            labelsByStop.GetValueOrDefault(tripStop, MultiLabel())
                            |> MultiLabel.set k arrival

                        earliestKnownArrivalByStop[tripStop] <- arrival
                        let connectionsAtStop = connections.GetValueOrDefault(tripStop, Dictionary())
                        connectionsAtStop[k] <- Connection(currentTrip, boardedAt, tripStop)
                        connections[tripStop] <- connectionsAtStop

                        // Mark stop as improved for next round
                        markedStops.Add tripStop |> ignore

                    // Can we catch an earlier trip?
                    if previousArrival <= arrival then
                        // If there was a trip that arrived earlier at the stop, then we take that trip and check for
                        // the next trip we can take from that stop on our route
                        currentTrip <- route |> Route.getEarliestTrip tripStop previousArrival connection
                        boardedAt <- tripStop



            for p in markedStops do
                if footPathsByStop.ContainsKey p then
                    let timeByDestination = footPathsByStop[p]
                    let startTime = labelsByStop[p][k]

                    for KeyValue(destination, footPathTime) in timeByDestination do
                        let arrivalByWalking = startTime + footPathTime
                        let currentBestArrival = labelsByStop[destination] |> MultiLabel.get k

                        let targetStopArrival =
                            earliestKnownArrivalByStop.GetValueOrDefault(target, infiniteTime)

                        if arrivalByWalking < min currentBestArrival targetStopArrival then
                            labelsByStop[destination] <-
                                labelsByStop.GetValueOrDefault(destination, MultiLabel())
                                |> MultiLabel.set k arrivalByWalking

                            earliestKnownArrivalByStop[destination] <- arrivalByWalking
                            let connectionsAtStop = connections.GetValueOrDefault(destination, Dictionary())
                            connectionsAtStop[k] <- FootPath(p, destination)
                            connections[destination] <- connectionsAtStop


    let test () =
        let path = Path.Combine(__SOURCE_DIRECTORY__, "./database.db")

        let builder = SqliteConnectionStringBuilder(ForeignKeys = true, DataSource = path)



        let connection = Sql.connect builder.ConnectionString

        let sourceStop =
            connection
            |> Sql.query
                """
                SELECT
                    id
                FROM 
                    stops
                WHERE 
                    stops.name = 'Sankt Augustin Fährstr.'
                """
            |> Sql.execute (fun read -> { id = read.string "id" })
            |> unwrap

        let targetStop =
            connection
            |> Sql.query
                """
                SELECT
                    id
                FROM 
                    stops
                WHERE 
                    stops.name = 'Siegburg Bf'
                """
            |> Sql.execute (fun read -> { id = read.string "id" })
            |> unwrap

        printfn $"{sourceStop} {targetStop}"

        let departure = TimeOnly(12, 00).ToTimeSpan().TotalSeconds


        ()

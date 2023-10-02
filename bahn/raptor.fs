module bahn.raptor

open System
open System.Collections.Generic
open System.Reflection.Emit
open Microsoft.FSharp.Core
open bahn.data

// Experiment with implementing RAPTOR algorithms

// k - number of trips
// Round k computes arrival times with k trips


// type Label = Label of numberOfTrips: int * earliestKnownArrivalTime: Time
//
// module Label =
//     // All values in all labels are initialized to ∞
//     let create numberOfTrips =
//         Label(numberOfTrips, Time Double.PositiveInfinity)
//
//     let earliestKnownArrivalTime (Label(_, time)) = time


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


let raptor (source: Stop) (target: Stop) (departure: float<Time>) =

    // More precisely, the algorithm associates with
    // each stop p a multilabel (τ0(p), τ1(p), ... , τK(p)), where
    // τi(p) represents the earliest known arrival time at p with up to i trips.
    // All values in all labels are initialized to ∞
    let mutable labels = Dictionary<Stop, Dictionary<int<NumberOfTrips>, float<Time>>>()
    // τi(p) represents the earliest known arrival time at p with up to i trips.
    let t i p = labels[p][i]

    let setT i p time =
        let multiLabel = labels.GetValueOrDefault(p, Dictionary<_, _>())
        multiLabel[i] <- time
        labels[p] <- multiLabel

    // We then set τ0(ps) = τ
    labels.Add(source, Dictionary<_, _>([| KeyValuePair(0<NumberOfTrips>, departure) |]))

    // Number of trips
    let mutable k = 0<NumberOfTrips>
    // Start round
    while true do
        k <- k + 1<NumberOfTrips>
        // Compute the fastest way to get to every stop with at most k - 1 transfers (i.e. by taking at most k trips)
        // The goal of round k is to compute τk(p) for all p. It does so in three stages.

        // The first stage of round k sets τk(p) = τk−1(p) for all stops p: this sets an upper bound on the earliest
        // arrival time at p with at most k trips.
        // setT k p
        ()

    ()

module NaiveRaptor =
    type Trip = Trip
    type Stop = Stop
    /// <summary>
    /// Describes a Line or Route in a public transport network which can have trips to different times of the day.
    /// Trips belong to a route if they visit stops in the same sequence.
    /// </summary>
    /// <remarks>
    /// The concept of route differs from the GTFS definition of a route. In GTFS a route can have trips which can take
    /// different paths and has different stops whereas with RAPTOR each trip that takes a different path is its own route.
    /// Additionally a route can visit the same stops but in a different direction where the route in GTFS is directionless
    /// </remarks>
    type Route = Route


    module Stop =
        let getRoutesAtStop (stop: Stop) : Route array = failwith "Not implemented"

    module Trip =
        /// <summary>
        /// Gets the stops and their arrival times after the specified stop for the specified trip
        /// </summary>
        let getStopsAfter stop trip : (Stop * float<Time>) seq = failwith "Not implemented"
        /// <summary>
        /// Is it possible to exit the trip at the stop
        /// </summary>
        let isDropOff stop trip : bool = failwith "Not Implemented"

    module Route =
        /// <summary>
        /// Gets the earliest trip that can be boarded from a stop for the route
        /// </summary>
        let getEarliestTrip stop (after: float<Time>) (route: Route) : Trip =
            // The earliest trip is when departure time for that trip at the stop is greater than the arrival time for
            // the last round for that stop (...it departs after we arrive)
            failwith "Not implemented"

        let getStopSequence stop route : uint = failwith "Not implemented"

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
                markedStops |> Seq.map (fun stop -> stop, Stop.getRoutesAtStop stop) |> Map

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
                let mutable currentTrip = route |> Route.getEarliestTrip stop earliestArrival
                let mutable boardedAt = stop

                // "hop on"
                // Traverse
                let subsequentStop = currentTrip |> Trip.getStopsAfter stop

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
                        currentTrip <- route |> Route.getEarliestTrip tripStop previousArrival
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

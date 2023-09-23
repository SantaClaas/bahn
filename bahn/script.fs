module bahn.script

open System
open FSharp.Data
open System.IO
open FSharp.Data.Runtime.XmlSchema
open bahn.csv


// Custom types
type LocationType =
    /// <summary>
    /// 0 (or blank) - Stop (or Platform). A location where passengers board or disembark from a transit vehicle.
    /// Is called a platform when defined within a parent_station.
    /// </summary>
    | StopOrPlatform = 0
    /// <summary>
    /// Station. A physical structure or area that contains one or more platform.
    /// </summary>
    | Station = 1
    /// <summary>
    /// Entrance/Exit. A location where passengers can enter or exit a station from the street.
    /// If an entrance/exit belongs to multiple stations, it may be linked by pathways to both, but the data provider
    /// must pick one of them as parent.
    /// </summary>
    | EntranceOrExit = 2
    /// <summary>
    /// Generic Node. A location within a station, not matching any other location_type, that may be used to link
    /// together pathways define in pathways.txt.
    /// </summary>
    | GenericNode = 3
    /// <summary>
    /// Boarding Area. A specific location on a platform, where passengers can board and/or alight vehicles.
    /// </summary>
    | BoardingArea = 4

// type Stop = {
//     id: uint
//     stop_name: string
// }


let getPath name =
    Path.Combine(__SOURCE_DIRECTORY__, "GTFS_VRS_mit_SPNV", $"{name}.txt")

let stops = Stops.Load(getPath "stops").Cache()
let trips = Trips.Load(getPath "trips").Cache()
let routes = Routes.Load(getPath "routes").Cache()
let shapes = Shapes.Load(getPath "shapes").Cache()
let stopTimes = StopTimes.Load(getPath "stop_times").Cache()



let trip = trips.Rows |> Seq.head

printfn $"Trip {trip.Trip_id}  '{trip.Trip_headsign}'"
//
// let tripStopTimes =
//     stopTimes.Rows
//     |> Seq.filter (fun stop -> stop.Trip_id = trip.Trip_id)
//     |> Seq.sortBy (fun stop -> stop.Stop_sequence)
//
// for tripStop in tripStopTimes do
//     let stop = stops.Rows |> Seq.find (fun stop -> stop.Stop_id = tripStop.Stop_id)
//     printfn $"{tripStop.Stop_sequence} {stop.Stop_name} {tripStop.Arrival_time} {tripStop.Departure_time}"
//
// printfn "My trip"
//
// let start =
//     stops.Rows |> Seq.find (fun s -> s.Stop_name = "Sankt Augustin Fährstr.")
//
// // As an example, find the first trip that stops at our start
// let startStopTime =
//     stopTimes.Rows |> Seq.find (fun stopTime -> stopTime.Stop_id = start.Stop_id)
//
// let myTrip =
//     trips.Rows |> Seq.find (fun trip -> trip.Trip_id = startStopTime.Trip_id)
//
// // Find all stops for the trip we hop on
// let myTripStopTimes =
//     stopTimes.Rows
//     |> Seq.filter (fun stopTime -> stopTime.Trip_id = myTrip.Trip_id)
//     |> Seq.sortBy (fun stopTime -> stopTime.Stop_sequence)
//     
// for tripStop in myTripStopTimes do
//     let stop = stops.Rows |> Seq.find (fun stop -> stop.Stop_id = tripStop.Stop_id)
//     printfn $"{tripStop.Stop_sequence} {stop.Stop_name} {tripStop.Arrival_time} {tripStop.Departure_time}"




// let destination = stops.Rows |> Seq.find (fun s -> s.Stop_name = "Siegburg Bf")

// Dijkstra's algorithm to find fastest route
// Before that, we need to find the edges and their weight
// The weight might need to not just be travel time but also factor in amount of changes, time to change

// Find routes at stop


// printfn $"{destination.Stop_id} {start.Location_type}"

// Start and stop are simulated user inputs for start and destination
let stopStop = "Siegburg Bf"


printfn "Hello from F#"

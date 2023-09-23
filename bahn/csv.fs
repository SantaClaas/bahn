module bahn.csv

open FSharp.Data


[<Literal>]
let ResolutionFolder = __SOURCE_DIRECTORY__ + "/GTFS_VRS_mit_SPNV"
// Required
/// <summary>
/// Transit agencies with service represented in this dataset.
/// </summary>
// type Agencies = CsvProvider<"agency.txt",IgnoreErrors=true, ResolutionFolder=ResolutionFolder,PreferOptionals=true, Schema="agency_id,agency_name,agency_url,agency_timezone,agency_lang,agency_phone,agency_fare_url, agency_email (string option)">

/// <summary>
/// Stops where vehicles pick up or drop off riders. Also defines stations and station entrances.
/// </summary>
type Stops =
    CsvProvider<"stops.txt", ResolutionFolder=ResolutionFolder>

/// <summary>
/// Transit routes. A route is a group of trips that are displayed to riders as a single service.
/// </summary>
type Routes = CsvProvider<"routes.txt", ResolutionFolder=ResolutionFolder>
/// <summary>
/// Trips for each route. A trip is a sequence of two or more stops that occur during a specific time period.
/// </summary>
type Trips = CsvProvider<"trips.txt", ResolutionFolder=ResolutionFolder>

/// <summary>
/// Times that a vehicle arrives at and departs from stops for each trip.
/// </summary>
type StopTimes =
    CsvProvider<"stop_times.txt", Schema="trip_id,arrival_time=string,departure_time=string,stop_id,stop_sequence,stop_headsign,pickup_type,drop_off_type,shape_dist_traveled", ResolutionFolder=ResolutionFolder>


type Calendar = CsvProvider<"calendar.txt", ResolutionFolder=ResolutionFolder>
type Shapes = CsvProvider<"shapes.txt", ResolutionFolder=ResolutionFolder>

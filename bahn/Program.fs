open System
open System.Collections.Generic
open System.IO
open System.IO.Compression

open System.Net.Http
open FSharp.Data
open FSharp.Data.CsvExtensions
open Microsoft.FSharp.Core
open bahn.data


let requiredFiles =
    [|
       // Order is important for foreign key constraints
       "agency.txt"
       "stops.txt"
       "routes.txt"
       "trips.txt"
       "stop_times.txt" |]


let replace (text: string) =
    Console.Clear()
    Console.WriteLine text

[<EntryPoint>]
let main _ =

    let task =
        task {
            use! connection = openDatabase ()

            // Download data if not already downloaded
            let dataFile =  "GTFS_VRS_mit_SPNV.zip"
            let url = $"https://download.vrsinfo.de/gtfs/{dataFile}"
            let path =  Path.Combine(__SOURCE_DIRECTORY__, dataFile)
            if
                File.Exists path
                |> not
            then
                use client = new  HttpClient()
                use! stream = client.GetStreamAsync(url)
                use fileStream = File.OpenWrite(path)
                do! stream.CopyToAsync(fileStream)

            let path = Path.Combine(__SOURCE_DIRECTORY__, "GTFS_VRS_mit_SPNV.zip")
            use archive = ZipFile.OpenRead path

            let entries = String.Join(",", archive.Entries)
            printfn $"Entries: {entries}"


            for fileName in requiredFiles do

                let file = archive.GetEntry fileName

                if file = null then
                    failwith $"""Required file "{fileName}" not provided"""

                use stream = file.Open()
                use csv = CsvFile.Load stream

                let tryGetColumn name (row: CsvRow) =
                    csv.TryGetColumnIndex name
                    |> Option.bind (fun index ->
                        match row[index] with
                        | "" -> None
                        | value -> Some value)

                match file.Name with
                | "agency.txt" ->
                    for row in csv.Rows do
                        let agency: Agency =
                            { id = row.GetColumn "agency_id"
                              name = row.GetColumn "agency_name"
                              url = row.GetColumn "agency_url"
                              timezone = row.GetColumn "agency_timezone"
                              language = row |> tryGetColumn "agency_lang"
                              phone = row |> tryGetColumn "agency_phone"
                              fareUrl = row |> tryGetColumn "agency_fare_url"
                              email = row |> tryGetColumn "agency_email" }

                        do! saveAgency agency connection

                | "routes.txt" ->
                    for row in csv.Rows do
                        let route: Route =
                            { id = row.GetColumn "route_id"
                              agencyId = row |> tryGetColumn "agency_id"
                              shortName = row |> tryGetColumn "route_short_name"
                              longName = row |> tryGetColumn "route_long_name"
                              description = row |> tryGetColumn "route_desc"
                              ``type`` = row.GetColumn("route_type") |> uint8
                              url = row |> tryGetColumn "route_url"
                              color = row |> tryGetColumn "route_color"
                              textColor = row |> tryGetColumn "route_text_color"
                              sort_order = (tryGetColumn "route_sort_order" row |> Option.map uint)
                              continuousPickup = (tryGetColumn "continuous_pickup" row |> Option.map uint8)
                              continuousDropOff = (tryGetColumn "continuous_drop_off" row |> Option.map uint8)
                              networkId = row |> tryGetColumn "network_id" }

                        do! saveRoute route connection
                | "stop_times.txt" ->
                    printfn "Transferring stop times"

                    for row in csv.Rows do
                        let stopTime: StopTime =
                            { tripId = row.GetColumn "trip_id"
                              arrivalTime = row |> tryGetColumn "arrival_time"
                              departureTime = row |> tryGetColumn "departure_time"
                              stopId = row.GetColumn "stop_id"
                              stopSequence = (row.GetColumn "stop_sequence") |> uint
                              stopHeadsign = row |> tryGetColumn "stop_headsign"
                              pickupType = (tryGetColumn "pickup_type" row |> Option.map uint8)
                              dropOffType = (tryGetColumn "drop_off_type" row |> Option.map uint8)
                              continuousPickup = (tryGetColumn "continuous_pickup" row |> Option.map uint8)
                              continuousDropOff = (tryGetColumn "continuous_drop_off" row |> Option.map uint8)
                              shapeDistanceTravelled = (tryGetColumn "shape_dist_traveled" row |> Option.map float)
                              timepoint = (tryGetColumn "timepoint" row |> Option.map uint8) }

                        do! saveStopTime stopTime connection
                | "trips.txt" ->

                    printfn "Transferring trips"

                    for row in csv.Rows do
                        let trip: Trip =
                            { id = row.GetColumn "trip_id"
                              routeId = row.GetColumn "route_id"
                              serviceId = row.GetColumn "service_id"
                              headsign = row |> tryGetColumn "trip_headsign"
                              shortName = row |> tryGetColumn "trip_short_name"
                              directionId = (tryGetColumn "direction_id" row |> Option.map uint8)
                              blockId = row |> tryGetColumn "block_id"
                              shapeId = row |> tryGetColumn "shape_id"
                              wheelchairAccessible = (tryGetColumn "wheelchair_accessible" row |> Option.map uint8)
                              bikesAllowed = (tryGetColumn "bikes_allowed" row |> Option.map uint8) }

                        do! saveTrip trip connection
                | "stops.txt" ->
                    printfn "Transferring stops"

                    for row in csv.Rows do
                        let stop: Stop =
                            { id = row.GetColumn "stop_id"
                              code = row |> tryGetColumn "stop_code"
                              name = row |> tryGetColumn "stop_name"
                              textToSpeechName = row |> tryGetColumn "tts_stop_name"
                              description = row |> tryGetColumn "stop_desc"
                              latitude = row |> tryGetColumn "stop_lat"
                              longitude = row |> tryGetColumn "stop_lon"
                              zoneId = row |> tryGetColumn "zone_id"
                              url = row |> tryGetColumn "stop_url"
                              locationType = (tryGetColumn "stop_url" row |> Option.map uint8)
                              parentStation = row |> tryGetColumn "parent_station"
                              timezone = row |> tryGetColumn "stop_timezone"
                              wheelchairBoarding = (tryGetColumn "wheelchair_boarding" row |> Option.map uint8)
                              levelId = row |> tryGetColumn "level_id"
                              platformCode = row |> tryGetColumn "platform_code" }

                        do! saveStop stop connection
                | _ -> ()


        }

    task.Wait()
    0

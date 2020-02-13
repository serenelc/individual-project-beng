import urllib.request
from urllib.error import HTTPError, URLError
from pathlib import Path
import json 
import time
import datetime as dt
from socket import timeout
import csv

def main():

    bus_452 = "452"
    bus_9 = "9"

    bus_stop_info = get_stop_info(bus_9)
    ids = [stop.get("stopID") for stop in bus_stop_info]
    
    bus_information = []
    for bus_stop_id in ids:
        expected_arrival_times = get_expected_arrival_times(bus_stop_id)
        if len(expected_arrival_times) == 0:
            print("invalid ID")
            ids.remove(bus_stop_id)
        else:
            bus_information.append(expected_arrival_times)
    
    print(bus_information)

    # get_expected_arrival_times("490007705F")


def get_stop_info(bus_route_id: str):
    bus_stop_info = []

    try:
        with urllib.request.urlopen("https://api.tfl.gov.uk/line/"+ bus_route_id +"/stoppoints") as api:
            data = json.loads(api.read().decode())
            for stop in data:
                info = {
                    "stopName": stop.get("commonName"),
                    "stopID": stop.get("naptanId")
                }
                bus_stop_info.append(info)

            return bus_stop_info
    except (HTTPError, URLError) as error:
        print("error: ", error)
    except timeout:
        print("timeout error")


def get_expected_arrival_times(stop_code: str):

    url =  "http://countdown.api.tfl.gov.uk/interfaces/ura/instant_V1?Stopcode2=" + stop_code + "&LineName=9&ReturnList=StopPointName,LineName,DestinationText,EstimatedTime,ExpireTime,VehicleID,DirectionID"
    bus_information = []

    try:
        with urllib.request.urlopen(url) as api:
            data = api.read().decode()
            for line in data.splitlines():
                line = line[1:]
                line = line[:-1]
                line = line.replace("\"", "")
                line_info = list(line.split(","))
                bus_information.append(line_info)
            return bus_information
    except (HTTPError, URLError) as error:
        # print("error: ", error)
        return bus_information
    except timeout:
        print("timeout error")


def evaluate_data(data, route_id):

    for info in data:
        if info.get("lineId") == route_id:
            vehicle_id = info.get("vehicleId")
            expected_arrival = info.get("expectedArrival")
            # timestamp is when countdown last updates the predicted arrival times
            timestamp = info.get("timestamp")

            vehicle_info = {
                "vehicle_id": vehicle_id,  
                "direction": info.get("direction"),
                "timestamp": timestamp,
                "expected_arrival": expected_arrival,
                "arrived": "False"
            }
            
            found, index = vehicle_already_found(vehicle_id, bus_info)
            if found:
                # If this vehicle is already in the dictionary, update the estimated arrival time
                vehicle_info = bus_info[index]
                print("New expected arrival time for bus {}: ".format(vehicle_id), expected_arrival)
                vehicle_info["expected_arrival"] = expected_arrival 
                vehicle_info["timestamp"] = timestamp
                bus_info[index] = vehicle_info
            else:
                print("New bus id: ", vehicle_id)
                # If this vehicle is not in the dictionary, then add it to the dictionary.
                bus_info.append(vehicle_info)

    return bus_info


def vehicle_already_found(vehicle_id, dictionary):
    found = False
    j = -1

    for i, vehicle in enumerate(dictionary):
        if vehicle.get("vehicle_id") == vehicle_id:
            found = True 
            j = i
            break 
    
    return found, j


def check_if_bus_is_due(buses, info):
    now = dt.datetime.now()
    for index, bus in enumerate(buses):
        eta = bus.get("expected_arrival")
        eta_as_dt = convert_time_to_datetime(eta)
        vehicle_id = bus.get("vehicle_id")
        if now < eta_as_dt:
            print("Vehicle hasn't arrived yet: ", vehicle_id)
        else:
            print("Vehicle is due to arrive: ", vehicle_id)

            # wait for 3 minutes after the bus is due to arrive
            if eta_as_dt < now.replace(minute = now.minute - 3):
                print("3 minutes after bus is due to arrive")
                info = check_if_bus_has_arrived(vehicle_id, info, now, eta_as_dt, index)
    return info


def check_if_bus_has_arrived(vehicle_id, info, time_now, time_due, index):
    """ 
    wait for 3 minutes after the bus is due to arrive. If the id shows back up in the 
    API call, this implies that it hasn't arrived yet. If the id does not show back up
    in the the API call, this implies that the bus arrived at the predicted time.
    """

    this_bus = info[index]
    timestamp = this_bus.get("timestamp")
    timestamp = convert_time_to_datetime(timestamp)

    # check when the eta for this bus was last updated 
    # (should only be updated if it was returned in the API call)
    if timestamp < time_now.replace(minute = time_now.minute - 3):
        print("Bus has arrived at predicted time")
        this_bus["arrived"] = "True"
        info[index] = this_bus

    return info


def convert_time_to_datetime(given_time):
    year = int(given_time[:4])
    month = int(given_time[5:7])
    day = int(given_time[8:10])
    hour = int(given_time[11:13])
    minute = int(given_time[14:16])
    second = int(given_time[17:19])

    date_time = dt.datetime(year, month, day, hour, minute, second)
    return date_time


main()
"""
I need the TfL predicted journey times between 2 stops at the same request times as I have 
in the historical model i.e. every 30 minutes (but this could be dropped to every 15 minutes)
so to be safe, I need the TfL predicted journey times between 2 stops every 15 minutes from the hour.

TfL gives predicted arrival times, so in order to get predicted journey times I need to do the following:
- I am at stop A and I want to get the predicted journey time from stop A to stop B
- request Countdown for stop A and stop B.
- get the time of the earliest bus that will leave A.
- find the corresponding vehicle in the Countdown response for stop B
- The difference in these times is TfL's predicted journey time for a bus from stop A to stop B 
at this request time.

For these specific stops and routes
1) 52: Chesterton Road -> Nottinghill Gate
2) 52: Willesden Bus Garage -> Okehampton Road
3) 52: Willesden Bus Garage -> Harrow Road / Kilburn Lane
4) 52: Willesden Bus Garage -> Chesterton Road
5) 52: Willesden Bus Garage -> Notting Hill Gate Station
6) 52: Willesden Bus Garage -> Palace Gate
7) 52: Willesden Bus Garage -> Knightsbridge Station / Harrods
8) 52: Willesden Bus Garage -> Victoria Bus Station
9) 9: North End Road -> Phillimore Gardens
10) 52: All Souls Avenue -> Notting Hill Gate Station
11) 69: Florence Road -> Star Lane
"""

import datetime as dt
import time
from pathlib import Path
from os import path
import json
import csv
import urllib.request
from urllib.error import HTTPError, URLError

def main():

    stop_info = read_csv()

    s = stop_info[0]

    etas = get_expected_arrival_times(s.get("stop_id"), s.get("route_id"))

    earliest_bus_to_leave = evaluate_bus_data(etas)

    #now find this bus in the countdown response for the destination stop.
    

def read_csv():
    stop_info = []
    file_name = 'tfl_stop_ids.csv'
    stop_file = Path.cwd() / file_name

    if stop_file.is_file():
        try:
            with open(file_name) as csv_file:
                csv_reader = csv.reader(csv_file, delimiter = ',')
                line_count = 0

                for row in csv_reader:
                    if line_count != 0:
                        route_id = row[0]
                        stop_name = row[1]
                        stop_id = row[2]

                        info = {
                            "route_id": route_id,
                            "stop_name": stop_name,
                            "stop_id": stop_id
                        }

                        stop_info.append(info)
                    line_count += 1
        except IOError:
            print("I/O error in loading information from csv file")
    
    return stop_info


def evaluate_bus_data(bus_data):

    if len(bus_data) <=1 :
        #i.e. not buses arriving at this stop in the next 30 minutes
        return -1

    for i, bus_stop in enumerate(bus_data):
        if i == 0:
            continue

        bus_stop_name = bus_stop[1]
        eta = dt.datetime.fromtimestamp(int(bus_stop[6])/1000.0)
        vehicle_id = bus_stop[5]

        leave_time = {
            "bus_stop_name": bus_stop_name,
            "leave_time": eta,
            "vehicle_id": vehicle_id
        }

        return leave_time


def get_expected_arrival_times(stop_code: str, route_id: str):
    start = time.time()
    url =  "http://countdown.api.tfl.gov.uk/interfaces/ura/instant_V1?Stopcode2=" + stop_code + "&LineName=" + route_id + "&ReturnList=StopPointName,LineName,DestinationText,EstimatedTime,ExpireTime,VehicleID,DirectionID"
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
        # Invalid stop code, so ignore error. 
        print("Invalid stop code")
        return bus_information
    except http.client.RemoteDisconnected as disconnect:
        # remote end closed connection without response. Try again later.
        print("remote disconnected: ", disconnect)
        return bus_information
    except timeout:
        print("timeout error when getting expected arrival times")
    comp_time = time.time() - start
    print("Get expected arrival times: ", comp_time)

main()
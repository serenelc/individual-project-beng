import csv
import datetime as dt
from pathlib import Path
from os import path
import time
import urllib.request
from urllib.error import HTTPError, URLError
import json
from socket import timeout

def get_valid_stop_ids(bus_route):
    start = time.time()
    route_information = []
    file_name = 'valid_stops/valid_stop_ids_' + bus_route + '.csv'
    bus_file = Path.cwd() / file_name

    if bus_file.is_file():
        try:
            with open(file_name) as csv_file:
                csv_reader = csv.reader(csv_file, delimiter = ',')
                line_count = 0
                for row in csv_reader:
                    if line_count != 0:
                        stop_name = row[0]
                        stop_id = row[1]

                        valid_stop = {
                            "stop_name": stop_name,
                            "stop_id": stop_id
                        }

                        route_information.append(valid_stop)
                    line_count += 1
        except IOError:
            print("I/O error in loading valid stop information from csv file")
    
    comp_time = time.time() - start
    print("Get valid stop ids: ", comp_time)
    return route_information


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
        print(stop_code)
        return bus_information
    except http.client.RemoteDisconnected as disconnect:
        # remote end closed connection without response. Try again later.
        print("remote disconnected: ", disconnect)
        return bus_information
    except timeout:
        print("timeout error when getting expected arrival times")
    comp_time = time.time() - start
    print("Get expected arrival times: ", comp_time)


valid_stops = get_valid_stop_ids("452")
new_bus_info = []
for bus_stop in valid_stops:
    # bus_stop is a tuple (stop_id, stop_name)
    bus_stop_id = bus_stop.get("stop_id")
    new_arrival_info = get_expected_arrival_times(bus_stop_id, "452")
    new_bus_info.append(new_arrival_info)


# print("New data gathered: ", len(new_bus_info))
# # Evaluate the new data with respect to the old gathered data
# for bus_stop in new_bus_info:
#     print(bus_stop)

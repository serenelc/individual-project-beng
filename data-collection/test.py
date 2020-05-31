import json
import csv
import urllib.request
from urllib.error import HTTPError, URLError
import json
from socket import timeout
from pathlib import Path

def write_to_csv(route, valid_stops):

    csv_name = "valid_stops/valid_stop_ids_" + route + ".csv"
    csv_columns = ['stop_id', 'stop_name', 'direction']
    try:
        with open(csv_name, 'w') as csv_file:
            writer = csv.DictWriter(csv_file, fieldnames = csv_columns)
            writer.writeheader()
            for data in valid_stops:
                writer.writerow(data)
    except IOError:
        print("I/O error in loading information into csv file")
        
        
def get_stop_info(bus_route_id: str):
    bus_stop_info = []

    try:
        with urllib.request.urlopen("https://api.tfl.gov.uk/line/"+ bus_route_id +"/stoppoints") as api:
            print("Making API call to get stop info")
            data = json.loads(api.read().decode())
            for stop in data:
                info = {
                    "stop_name": stop.get("commonName"),
                    "stop_id": stop.get("naptanId")
                }
                bus_stop_info.append(info)

            return bus_stop_info
    except (HTTPError, URLError) as error:
        print("error in getting bus stop info: ", error)
    except timeout:
        print("timeout error")
        

def get_expected_arrival_times(stop_code: str, route_id: str):
    url =  "http://countdown.api.tfl.gov.uk/interfaces/ura/instant_V1?Stopcode2=" + stop_code + "&LineName=" + route_id + "&ReturnList=StopPointName,DirectionID"
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
        # Invalid bus ID, so ignore error
        print("Invalid ID: ", stop_code)
        return []
    except timeout:
        print("timeout error when getting expected arrival times")


def get_valid_bus_stop_ids(bus_route):
    bus_stop_info = get_stop_info(bus_route)
    
    print("Getting list of all bus stop IDs on route {}".format(bus_route))
    print("All stop id count: ", len(bus_stop_info))
    
    for i, bus_stop in enumerate(bus_stop_info):
        bus_stop_id = bus_stop.get("stop_id")
        expected_arrival_times = get_expected_arrival_times(bus_stop_id, bus_route)
        
        if len(expected_arrival_times) <= 1:
            bus_stop_info.remove(bus_stop)
        else:
            direction = expected_arrival_times[1][2]
            if direction == '2':
                # inbound
                bus_stop["direction"] = "inbound"
            else:
                # outbound
                bus_stop["direction"] = "outbound"

    print("Valid stop id count: ", len(bus_stop_info))
    return bus_stop_info


def main():
    bus_routes = ["9", "452", "52", "328", "277", "267", "7", "6", "35", "37", "69", "14"]
    bus_routes = ["452", "328", "277", "267", "7", "35", "37", "69", "14"]

    for i, route in enumerate(bus_routes):
        valid_stops = get_valid_bus_stop_ids(bus_routes[i])
        write_to_csv(bus_routes[i], valid_stops)

main()

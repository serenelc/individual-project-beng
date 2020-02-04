import urllib.request
from urllib.error import HTTPError, URLError
import json 
import time
import datetime as dt
from socket import timeout

bus_information = []

def main():

    bus_452 = "452"
    bus_9 = "9"
    york_house_place = '490010536K'
    high_street_ken = '490000110F'

    # repeat_call_api(5, bus_9, high_street_ken)
    call_countdown_api(bus_9, high_street_ken)
    # time.sleep(30)
    # call_countdown_api(bus_9, high_street_ken) 

def repeat_call_api(num_calls, bus_route_id, bus_stop_id):
    for i in range (0, num_calls):
        i += 1

        print(dt.datetime.now())
        print("tick")
        # call_countdown_api(bus_route_id, bus_stop_id)
        time.sleep(30)


def call_countdown_api(route_id, stop_id):
    try:
        with urllib.request.urlopen("https://api.tfl.gov.uk/StopPoint/" + stop_id + "/arrivals") as api:
            data = json.loads(api.read().decode())
            arrival_times = get_relevant_info(data, route_id, bus_information)
            check_if_bus_has_arrived(arrival_times)
            print(arrival_times)

    except (HTTPError, URLError) as error:
        print("error: ", error)
    except timeout:
        print("timeout error")

def get_relevant_info(data, route_id, bus_info):
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
                print("New expected arrival time: ", expected_arrival)
                vehicle_info["expected_arrival"] = expected_arrival 
                vehicle_info["timestamp"] = timestamp
                bus_info[index] = vehicle_info
            else:
                print("New bus")
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

def check_if_bus_has_arrived(buses):
    now = dt.datetime.now()
    for bus in buses:
        eta = bus.get("expected_arrival")
        eta_as_dt = convert_time_to_datetime(eta)
        if now < eta_as_dt:
            print("Vehicle hasn't arrived yet: ", bus.get("vehicle_id"))
        else:
            print("Vehicle is due to arrive: ", bus.get("vehicle_id"))


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
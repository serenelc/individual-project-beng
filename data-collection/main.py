import urllib.request
import json 
import sched
import time

s = sched.scheduler(time.time, time.sleep)
bus_information = []

def main():

    bus_route_id = "452"
    # s.enter(30, 1, call_countdown_api, argument=(bus_route_id))
    s.enter(5, 1, print_time, argument=(0))
    s.run()
    # call_countdown_api(bus_route_id) 

def print_time(count):
    current_time = time.asctime(time.gmtime(time.time()))
    count += 1
    print("From print_time", current_time)
    s.enter(5, 1, print_time, count)
    
    if count == 5:
        exit

def call_countdown_api(route_id):
    with urllib.request.urlopen("https://api.tfl.gov.uk/StopPoint/490010536K/arrivals") as api:
        data = json.loads(api.read().decode())
        arrival_times = get_relevant_info(data, route_id, bus_information)
        print(arrival_times)


def get_relevant_info(data, route_id, bus_info):

    for info in data:
        if info.get("lineId") == route_id:
            vehicle_id = info.get("vehicleId")
            expected_arrival = info.get("expectedArrival")

            vehicle_info = {
                "vehicle_id": vehicle_id,  
                "direction": info.get("direction"),
                "timestamp": info.get("timestamp"),
                "expected_arrival": expected_arrival ,
                "arrived": False
            }
            
            found, index = vehicle_already_found(vehicle_id, bus_info)
            if found:
                # If this vehicle is already in the dictionary, update the estimated arrival time
                vehicle_info = bus_info[index]
                vehicle_info["expected_arrival"] = expected_arrival 
                bus_info[index] = vehicle_info
            else:
                # If this vehicle is not yet in the directionary, add it to the dictionary
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

main()
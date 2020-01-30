import urllib.request, json 

def main():

    bus_route_id = "452"
    bus_information = []
    
    with urllib.request.urlopen("https://api.tfl.gov.uk/StopPoint/490010536K/arrivals") as api:
        data = json.loads(api.read().decode())
        arrival_times = get_relevant_info(data, bus_route_id, bus_information)
        print(arrival_times)


def get_relevant_info(data, route_id, bus_info):

    for info in data:
        if info.get("lineId") == route_id:
            vehicle_id = info.get("vehicleId")
            
            # Check if this vehicle has already been added to the dictionary
            if vehicle_already_found(vehicle_id, bus_info):

            else:


            vehicle_info = {
                "vehicle_id": vehicle_id,  
                "direction": info.get("direction"),
                "timestamp": info.get("timestamp"),
                "expected_arrival": info.get("expectedArrival")
            }

            bus_info.append(vehicle_info)

    return bus_info

def vehicle_already_found(vehicle_id, dictionary):
    found = False
    for vehicle in dictionary:
        if vehicle.get("vehicle_id") == vehicle_id:
            found = True 
            break 
    
    return found

main()
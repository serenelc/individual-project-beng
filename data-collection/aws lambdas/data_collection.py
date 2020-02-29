import urllib.request
from urllib.error import HTTPError, URLError
import json
from socket import timeout
import datetime as dt
from utils import Utilities
import time

class Data_Collection(object):
            
    def get_expected_arrival_times(self, stop_code: str, route_id: str):
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
            # Invalid bus ID, so ignore error
            return bus_information
        except timeout:
            print("timeout error when getting expected arrival times")
            
        comp_time = time.time() - start
        print("Get expected arrival times: ", comp_time)


    def get_stop_code(self, bus_stop_name, all_stops):
        for stop in all_stops:
            if bus_stop_name == stop.get("stop_name"):
                return stop.get("stop_id")
        return "NOT_FOUND"


    def evaluate_bus_data(self, new_data, old_data, stop_info, route):
        # start = time.time()
        print("Evaluating new bus arrival information")
        today = dt.datetime.today().strftime('%Y-%m-%d')
        helper = Utilities()

        for bus_stop in new_data:
            if len(bus_stop) <= 1:
                continue

            ura_array = bus_stop[0]
            time_of_req = int(ura_array[2])
            time_of_request = dt.datetime.fromtimestamp(time_of_req/1000.0)

            for info in bus_stop[1:]:
                bus_stop_name = info[1]
                stop_code = self.get_stop_code(bus_stop_name, stop_info)
                if stop_code == "NOT_FOUND":
                    break
                
                direction = 1 if info[3] == '2' else 0
                vehicle_id = info[5] + "_" + stop_code + "_" + today + "_" + str(direction) + "_0"
                eta = dt.datetime.fromtimestamp(int(info[6])/1000.0)

                # incoming vehicle info
                new_vehicle_info = {
                    "vehicle_id": vehicle_id,
                    "bus_stop_name": bus_stop_name,
                    "direction": direction,
                    "expected_arrival": eta,
                    "time_of_req": time_of_request,
                    "arrived": False
                }

                found, first_journey, index = self.vehicle_already_found(new_vehicle_info, old_data)

                # if this vehicle is already in the dictionary
                if found:
                    # old vehicle found already in dictionary
                    found_vehicle = old_data[index]
                    
                    current_id = new_vehicle_info.get("vehicle_id")

                    # if this is the first journey update the eta
                    if first_journey:
                        found_vehicle["expected_arrival"] = eta 
                        found_vehicle["time_of_req"] = time_of_request
                        old_data[index] = found_vehicle

                    # if this is not the first journey, change vehicle ID to indicate trip number
                    else:
                        trip_num = int(new_vehicle_info.get("vehicle_id")[-1]) + 1
                        new_id = new_vehicle_info.get("vehicle_id")[:-1] + str(trip_num)
                        new_vehicle_info["vehicle_id"] = new_id
                        old_data.append(new_vehicle_info)

                else:
                    # If this vehicle is not in the dictionary, then add it to the dictionary.
                    # print("New vehicle, add to dictionary")
                    old_data.append(new_vehicle_info)
        
        # comp_time = time.time() - start
        # print("Evaluate bus data: ", comp_time)
        return old_data


    def vehicle_already_found(self, current_vehicle, old_data):
        # start = time.time()
        helper = Utilities()

        found = False
        first_journey = True
        index = -1
        current_id = current_vehicle.get("vehicle_id")
        
        for i, old_bus in enumerate(old_data):
            old_id = old_bus.get("vehicle_id")
            old_direction = old_bus.get("direction")
            same_vehicle = current_id == old_id
            
            if same_vehicle:
                found_vehicle = old_bus
                same_direction = old_direction == current_vehicle.get("direction")
                
                # check that this isn't the 1st trip of the day for that vehicle
                if not same_direction:
                    # print("This vehicle is travelling in a new direction")
                    first_journey = False
    
                # assume that a bus takes 2 hours to run its full route
                two_hours_before = current_vehicle.get("time_of_req") - dt.timedelta(hours = 2)
                found_time_of_req = found_vehicle.get("time_of_req")
                
                if found_time_of_req < two_hours_before:
                    # print("This vehicle has already done at least 1 journey today!")
                    first_journey = False
                
                found = True
                index = i
                break
        
        # comp_time = time.time() - start
        # print("Vehicle already found: ", comp_time)
        return found, first_journey, index


    def check_if_bus_is_due(self, bus_information):
        start = time.time()

        now = dt.datetime.now()
        buses_not_arrived = []
        buses_arrived = []

        for index, bus in enumerate(bus_information):
            eta = bus.get("expected_arrival")
            vehicle_id = bus.get("vehicle_id")

            if now >= eta:
                # wait for 3 minutes after the bus is due to arrive
                three_minutes_ago = now - dt.timedelta(minutes = 3)
                
                if eta < three_minutes_ago:
                    this_bus = bus_information[index]
                    this_bus, arrived = self.check_if_bus_has_arrived(now, this_bus)
                    
                    if arrived:
                        buses_arrived.append(this_bus)
                    else:
                        buses_not_arrived.append(this_bus)

        comp_time = time.time() - start
        print("Check if bus is due: ", comp_time)
        return buses_not_arrived, buses_arrived


    def check_if_bus_has_arrived(self, time_now, this_bus):
        
        time_of_req = this_bus.get("time_of_req")
        arrived = False

        # check that the eta for this bus was last updated more than 3 minutes ago, i.e. it wasn't returned
        # in the most recent API call
        three_minutes_ago = time_now - dt.timedelta(minutes = 3)
        if time_of_req < three_minutes_ago:
            # print("Bus has arrived at predicted time")
            arrived = True
            this_bus["arrived"] = arrived
        
        return this_bus, arrived

import urllib.request
from urllib.error import HTTPError, URLError
import json
from socket import timeout
import datetime as dt
from local_helper import Utilities
import time
import http.client

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


    def get_stop_code(self, bus_stop_name, all_stops):
        # all_stops = list of tuples (stop_id, stop_name)
        
        start = time.time()
        for stop in all_stops:
            if bus_stop_name == stop[1]:
                return stop[0]
        comp_time = time.time() - start
        print("Get stop code: ", comp_time)
        return "NOT_FOUND"


    def evaluate_bus_data(self, new_data, old_data, stop_info):
        start = time.time()
        print("Evaluating new bus arrival information")
        helper = Utilities()

        for bus_stop in new_data:

            # no eta returned so skip
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
                
                # 2 should be in and 1 should be out: data before 12th may needs to be swapped
                direction = "inbound" if info[3] == '2' else "outbound"
                eta = dt.datetime.fromtimestamp(int(info[6])/1000.0)
                # so that requests made at 11.50 pm for buses arriving after midnight on the 
                # next day have vehicle ids with eta's date instead of request's date
                date = dt.datetime.fromtimestamp(int(info[6][:10])).strftime('%Y-%m-%d')
                vehicle_id = info[5] + "_" + stop_code + "_" + date + "_" + direction + "_0"

                # incoming vehicle info
                new_vehicle_info = {
                    "vehicle_id": vehicle_id,
                    "bus_stop_id": stop_code,
                    "direction": direction,
                    "expected_arrival": eta,
                    "time_of_req": time_of_request
                }

                found, index = self.vehicle_already_found(new_vehicle_info, old_data)

                # if this vehicle is already in the dictionary
                if found:
                    # old vehicle found already in dictionary
                    found_vehicle = old_data[index]
                    
                    current_id = new_vehicle_info.get("vehicle_id")

                    # update the eta if it has changed
                    if found_vehicle["expected_arrival"] != eta:
                        found_vehicle["expected_arrival"] = eta 
                        found_vehicle["time_of_req"] = time_of_request
                        old_data[index] = found_vehicle

                else:
                    # If this vehicle is not in the dictionary, then add it to the dictionary.
                    old_data.append(new_vehicle_info)
        
        comp_time = time.time() - start
        print("Evaluate bus data: ", comp_time)
        return old_data


    def vehicle_already_found(self, current_vehicle, old_data):
        start = time.time()
        helper = Utilities()

        found = False
        index = -1
        if len(old_data) == 0:
            return found, index
        
        # can do this because this automatically has only 0 appended
        current_id = current_vehicle.get("vehicle_id")[:-1] 
        
        for i, old_bus in enumerate(old_data):
            # should technically all end in 0s because I don't change the index until I write to bus_arrivals anyway
            [a, b, c, d, _] = old_bus.get("vehicle_id").split('_')
            old_id = a + "_" + b + "_" + c + "_" + d + "_"
            
            if current_id == old_id:
                found = True
                index = i
                break
        
        comp_time = time.time() - start
        # print("Vehicle already found: ", comp_time)
        return found, index


    def check_if_bus_is_due(self, bus_information):
        start = time.time()
        bst = dt.timezone(dt.timedelta(hours = 1))
        gmt = dt.timezone.utc

        #the api returns the expected arrival times in GMT but should I convert the times into BST so +1?

        now = dt.datetime.now(tz = gmt)
        buses_not_arrived = []
        buses_arrived = []

        for index, this_bus in enumerate(bus_information):
            eta = this_bus.get("expected_arrival")
            eta_aware = eta.replace(tzinfo=gmt)
            vehicle_id = this_bus.get("vehicle_id")

            # if the current time is after the expected arrival time of the bus, then it is due
            if now >= eta_aware:
                five_minutes_ago = now - dt.timedelta(minutes = 5)
                five_minutes_ago = five_minutes_ago.replace(tzinfo=gmt)
                # wait for 5 minutes after the bus is due to arrive
                if eta_aware < five_minutes_ago:
                    buses_arrived.append(this_bus)
                else:
                    buses_not_arrived.append(this_bus)
            else:
                buses_not_arrived.append(this_bus)

        comp_time = time.time() - start
        print("Check if bus is due: ", comp_time)
        return buses_not_arrived, buses_arrived
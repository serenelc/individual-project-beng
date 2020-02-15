# def repeat_call_api(num_calls: int, bus_route_id: str, bus_stop_id: str, info):
#     arrival_info = call_countdown_api(bus_route_id, bus_stop_id, info)
#     for i in range (0, num_calls):
#         i += 1
#         time.sleep(30)

#         print("======================================================")
#         print(dt.datetime.now())
#         arrival_info = call_countdown_api(bus_route_id, bus_stop_id, info)
        
#     write_to_csv(arrival_info, bus_route_id, bus_stop_id)

import urllib.request
from urllib.error import HTTPError, URLError
import json
from socket import timeout
import datetime as dt
from helper import Helper

class Data_Collection(object):

    def get_stop_info(self, bus_route_id: str):
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


    def get_expected_arrival_times(self, stop_code: str, route_id: str):
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
            print("timeout error")


    def evaluate_bus_data(self, new_data, old_data):
        print("Evaluating new bus arrival information")
        for bus_stop in new_data:

            ura_array = bus_stop[0]
            time_of_req = int(ura_array[2])
            time_of_request = dt.datetime.fromtimestamp(time_of_req/1000.0)

            for info in bus_stop[1:]:
                vehicle_id = info[5] + "_" + ura_array[2]
                # 2 == 'outbound', 1 == 'inbound'
                direction = "outbound" if info[3] == '2' else "inbound"
                eta = dt.datetime.fromtimestamp(int(info[6])/1000.0)
                bus_stop_name = info[1]

                vehicle_info = {
                    "vehicle_id": vehicle_id,
                    "bus_stop_name": bus_stop_name,
                    "direction": direction,
                    "expected_arrival": eta,
                    "timestamp": time_of_request,
                    "arrived": "False"
                }

                found, index = self.vehicle_already_found(vehicle_id, old_data)
                if found:
                    # If this vehicle is already in the dictionary, update the estimated arrival time
                    # print("New expected arrival time for bus {}: ".format(vehicle_id), expected_arrival)
                    vehicle_info = bus_stop[index]
                    vehicle_info["expected_arrival"] = eta 
                    vehicle_info["timestamp"] = time_of_request
                    old_data[index] = vehicle_info

                else:
                    # If this vehicle is not in the dictionary, then add it to the dictionary.
                    old_data.append(vehicle_info)

        return old_data


    def vehicle_already_found(self, vehicle_id, dictionary):
        found = False
        j = -1

        for i, vehicle in enumerate(dictionary):
            if vehicle.get("vehicle_id") == vehicle_id:
                found = True 
                j = i
                break 
        
        return found, j


    def check_if_bus_is_due(self, bus_information):

        helper = Helper()

        now = dt.datetime.now()
        for index, bus in enumerate(bus_information):
            eta = bus.get("expected_arrival")
            eta_as_dt = helper.convert_time_to_datetime(eta)
            vehicle_id = bus.get("vehicle_id")

            # if now < eta_as_dt:
            #     print("Vehicle {} hasn't arrived yet: ".format(vehicle_id))
            # else:
            #     print("Vehicle {} is due to arrive: ".format(vehicle_id))
            #     # wait for 3 minutes after the bus is due to arrive
            #     if eta_as_dt < now.replace(minute = now.minute - 3):
            #         print("It is now 3 minutes after bus is due to arrive")
            #         # info = check_if_bus_has_arrived(vehicle_id, info, now, eta_as_dt, index)
        return bus_information


    def check_if_bus_has_arrived(self, vehicle_id, info, time_now, time_due, index):
        """ 
        wait for 3 minutes after the bus is due to arrive. If the id shows back up in the 
        API call, this implies that it hasn't arrived yet. If the id does not show back up
        in the the API call, this implies that the bus arrived at the predicted time.
        """

        helper = Helper()

        this_bus = info[index]
        timestamp = this_bus.get("timestamp")
        timestamp = helper.convert_time_to_datetime(timestamp)

        # check when the eta for this bus was last updated 
        # (should only be updated if it was returned in the API call)
        if timestamp < time_now.replace(minute = time_now.minute - 3):
            print("Bus has arrived at predicted time")
            this_bus["arrived"] = "True"
            info[index] = this_bus

        return info
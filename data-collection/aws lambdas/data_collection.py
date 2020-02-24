import urllib.request
from urllib.error import HTTPError, URLError
import json
from socket import timeout
import datetime as dt
from utils import Utilities

class Data_Collection(object):
            
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
            print("timeout error when getting expected arrival times")


    def get_stop_code(self, bus_stop_name, all_stops):
        for stop in all_stops:
            if bus_stop_name == stop.get("stop_name").get("S"):
                return stop.get("stop_id").get("S")
        return "NOT_FOUND"


    def evaluate_bus_data(self, new_data, stop_info, route):
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

                found, first_journey = self.vehicle_already_found(new_vehicle_info, route)
                table_name = "bus_arrivals_" + route

                # if this vehicle is already in the dictionary
                if found:
                    # old vehicle found already in dictionary
                    current_id = new_vehicle_info.get("vehicle_id")

                    # if this is the first journey update the eta
                    if first_journey:
                        updated_info = {"expected_arrival": eta, "time_of_req": time_of_request}
                        helper.update_dynamo(table_name, current_id, updated_info)

                    # if this is not the first journey, change vehicle ID to indicate trip number
                    else:
                        trip_num = int(new_vehicle_info.get("vehicle_id")[-1]) + 1
                        new_id = new_vehicle_info.get("vehicle_id")[:-1] + str(trip_num)
                        new_vehicle_info["vehicle_id"] = new_id
                        helper.write_to_db(table_name, new_vehicle_info)

                else:
                    # If this vehicle is not in the dictionary, then add it to the dictionary.
                    # print("New vehicle, add to dictionary")
                    helper.write_to_db(table_name, new_vehicle_info)


    def vehicle_already_found(self, current_vehicle, route):
        helper = Utilities()

        found = False
        first_journey = True
        current_id = current_vehicle.get("vehicle_id")
        table_name = "bus_arrivals_" + route

        response = helper.check_if_vehicle_exists(table_name, current_id)

        if (len(response) > 0):
            # print("Found the same vehicle id in the database")

            found_vehicle = response[0]
            same_direction = int(found_vehicle.get("direction").get("N")) == current_vehicle.get("direction")

            # check that this isn't the 1st trip of the day for that vehicle
            if not same_direction:
                # print("This vehicle is travelling in a new direction")
                first_journey = False

            # assume that a bus takes 2 hours to run its full route
            two_hours_before = current_vehicle.get("time_of_req") - dt.timedelta(hours = 2)
            found_time_of_req = helper.convert_time_to_datetime(found_vehicle.get("time_of_req").get("S"))
            
            if found_time_of_req < two_hours_before:
                # print("This vehicle has already done at least 1 journey today!")
                first_journey = False
            
            found = True
        
        return found, first_journey


    def check_if_bus_is_due(self, route):
        helper = Utilities()
        
        table_name = "bus_arrivals_" + route
        bus_information = helper.get_all_buses_not_arrived(table_name)

        now = dt.datetime.now()

        for index, bus in enumerate(bus_information):
            eta = bus.get("expected_arrival")
            vehicle_id = bus.get("vehicle_id")

            if now < eta:
                print("Vehicle {} hasn't arrived yet: ".format(vehicle_id))
            else:
                print("Vehicle {} is due to arrive: ".format(vehicle_id))

                # wait for 3 minutes after the bus is due to arrive
                three_minutes_ago = now - dt.timedelta(minutes = 3)
                if eta < three_minutes_ago:
                    print("It is now 3 minutes after bus is due to arrive")
                    self.check_if_bus_has_arrived(now, bus, table_name)


    def check_if_bus_has_arrived(self, time_now, bus, table_name):
        helper = Utilities()
        time_of_req = bus.get("time_of_req")

        # check that the eta for this bus was last updated more than 3 minutes ago, i.e. it wasn't returned
        # in the most recent API call
        three_minutes_ago = time_now - dt.timedelta(minutes = 3)
        if time_of_req < three_minutes_ago:
            print("Bus has arrived at predicted time")
            bus["arrived"] = True
            helper.write_to_db(table_name, bus)

import urllib.request
from urllib.error import HTTPError, URLError
import json
from socket import timeout
import datetime as dt

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


    def get_stop_code(self, bus_stop_name, all_stops):
        for stop in all_stops:
            if bus_stop_name == stop.get("stopName"):
                return stop.get("stopID")
        return "NOT_FOUND"


    def evaluate_bus_data(self, new_data, old_data, stop_info):
        print("Evaluating new bus arrival information")
        today = dt.datetime.today().strftime('%Y-%m-%d')

        for bus_stop in new_data:

            ura_array = bus_stop[0]
            time_of_req = int(ura_array[2])
            time_of_request = dt.datetime.fromtimestamp(time_of_req/1000.0)

            for info in bus_stop[1:]:
                bus_stop_name = info[1]
                stop_code = self.get_stop_code(bus_stop_name, stop_info)
                vehicle_id = info[5] + "_" + stop_code + "_" + today + "_0"
                direction = 1 if info[3] == '2' else 0
                eta = dt.datetime.fromtimestamp(int(info[6])/1000.0)

                # incoming vehicle info
                new_vehicle_info = {
                    "vehicle_id": vehicle_id,
                    "bus_stop_name": bus_stop_name,
                    "direction": direction,
                    "expected_arrival": eta,
                    "timestamp": time_of_request,
                    "arrived": False
                }

                found, first_journey, index = self.vehicle_already_found(new_vehicle_info, old_data)

                # if this vehicle is already in the dictionary
                if found:
                    # old vehicle found already in dictionary
                    found_vehicle = old_data[index]

                    # if this is the first journey update the eta
                    if first_journey:
                        found_vehicle["expected_arrival"] = eta 
                        found_vehicle["timestamp"] = time_of_request
                        old_data[index] = found_vehicle

                    # if this is not the first journey, change vehicle ID to indicate trip number
                    else:
                        trip_num = int(new_vehicle_info.get("vehicle_id")[-1]) + 1
                        new_id = new_vehicle_info.get("vehicle_id")[:-1] + str(trip_num)
                        new_vehicle_info["vehicle_id"] = new_id
                        old_data.append(new_vehicle_info)

                else:
                    # If this vehicle is not in the dictionary, then add it to the dictionary.
                    old_data.append(new_vehicle_info)

        return old_data


    def vehicle_already_found(self, current_vehicle, dictionary):
        found = False
        first_journey = True
        j = -1

        for i, old_vehicle in enumerate(dictionary):
            # check if that vehicle is already in the dictionary
            same_vehicle = old_vehicle.get("vehicle_id") == current_vehicle.get("vehicle_id")
            same_direction = old_vehicle.get("direction") == current_vehicle.get("direction")
            if same_vehicle:
                print("Found the same vehicle id in the csv file!")
                # check that this isn't the 1st trip of the day for that vehicle
                
                if not same_direction:
                    first_journey = False
                
                # assume that a bus takes 2 hours to run its full route
                two_hours_before = current_vehicle.get("timestamp") - dt.timedelta(hours = 2)
                if old_vehicle.get("timestamp") < two_hours_before:
                    print("This vehicle has already done at least 1 journey today!")
                    first_journey = False
                
                found = True 
                j = i
                break 
        
        return found, first_journey, j


    def check_if_bus_is_due(self, bus_information):

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
                    bus_information = self.check_if_bus_has_arrived(bus_information, now, index)

        return bus_information


    def check_if_bus_has_arrived(self, bus_info, time_now, index):
        """ 
        wait for 3 minutes after the bus is due to arrive. If the id shows back up in the 
        API call, this implies that it hasn't arrived yet. If the id does not show back up
        in the the API call, this implies that the bus arrived at the predicted time.
        """

        this_bus = bus_info[index]
        timestamp = this_bus.get("timestamp")

        # check that the eta for this bus was last updated more than 3 minutes ago, i.e. it wasn't returned
        # in the most recent API call
        three_minutes_ago = time_now - dt.timedelta(minutes = 3)
        if timestamp < three_minutes_ago:
            print("Bus has arrived at predicted time")
            this_bus["arrived"] = True
            bus_info[index] = this_bus

        return bus_info
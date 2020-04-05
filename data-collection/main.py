import datetime as dt
import time
import csv
from local_helper import Helper
from local_data_collection import Data_Collection
from urllib.error import HTTPError, URLError
from os import path
from pathlib import Path

def main(bus_route):

    helper = Helper()
    data = Data_Collection()

    today = dt.datetime.today().strftime('%Y-%m-%d')
    
    # Get valid stops for this bus route
    valid_stops = helper.get_valid_stop_ids(bus_route)

    # Get old gathered information on buses not yet arrived from dynamo
    old_bus_info = helper.read_bus_info_from_csv(bus_route)
    print("Old information gathered: {}".format(len(old_bus_info)))

    comp_time = 0

    try:
        start = time.time()
        print("Getting expected arrival time of buses on route {}".format(bus_route))
        new_bus_info = []

        # Get expected arrival times for each stop on the route
        start_1 = time.time()
        for bus_stop in valid_stops:
            bus_stop_id = bus_stop.get("stop_id")
            new_arrival_info = data.get_expected_arrival_times(bus_stop_id, bus_route)
            new_bus_info.append(new_arrival_info)
        end = time.time() - start_1
        print("Get expected arrival times: ", end)

        # Evaluate the new data with respect to the old gathered data
        evaluated_data = data.evaluate_bus_data(new_bus_info, old_bus_info, valid_stops)
        # Check which buses have arrived
        not_arrived, arrived = data.check_if_bus_is_due(evaluated_data)

        csv_name_arrived = "past_data/bus_arrivals_" + bus_route + ".csv"
        csv_name_gathering = "past_data/bus_information_" + bus_route + ".csv"
        
        # Write/delete the relevant data to the relevant tables
        print(len(not_arrived), len(arrived))
        a = time.time()
        helper.write_to_csv(csv_name_gathering, not_arrived)
        helper.append_to_csv(csv_name_arrived, arrived)
        helper.delete_arrived_items(csv_name_gathering, arrived)
        b = time.time()
        print("Total time to write and delete from csvs: ", (b - a))
            
        comp_time = time.time() - start
        print("Entire function: ", comp_time)

    except (HTTPError, URLError) as error:
        # Send me a notification so I can fix it and keep it running.
        print("ERROR IN MAIN: ", error)

    return comp_time

# bus_routes = ["452", "9", "52", "328", "277", "267", "7"]
# bus_routes = ["9"]
# while True:
#     for bus_route in bus_routes:
#         comp_time = main(bus_route)
#         print("SLEEP")
#         if (comp_time < 30):
#             time.sleep(30 - comp_time)
#         main(bus_route)

item = 1586081932
print(dt.datetime.fromtimestamp(item).strftime('%Y-%m-%d'))

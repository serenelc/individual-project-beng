import datetime as dt
import time
from helper import Helper
from data_collection import Data_Collection
from urllib.error import HTTPError, URLError

def main():

    helper = Helper()
    data = Data_Collection()

    bus_routes = ["9", "452", "277", "267", "7", "14"]

    today = dt.datetime.today().strftime('%Y-%m-%d')

    bus_stop_info = data.get_stop_info(bus_routes[0])
    print("Getting list of all bus stop IDs on route {}".format(bus_routes[0]))
    stop_ids = [stop.get("stopID") for stop in bus_stop_info]

    current_info = helper.read_bus_info_from_csv(bus_routes[0], today)
    
    print("Getting expected arrival time of buses on route {}".format(bus_routes[0]))
    bus_information = []
    for bus_stop_id in stop_ids:
        expected_arrival_times = data.get_expected_arrival_times(bus_stop_id, bus_routes[0])
        if len(expected_arrival_times) == 0:
            stop_ids.remove(bus_stop_id)
        else:
            bus_information.append(expected_arrival_times)

    evaluated_info = data.evaluate_bus_data(bus_information, current_info, bus_stop_info)

    # evaluated_info = helper.read_bus_info_from_csv(bus_9, today)

    evaluated_info = data.check_if_bus_is_due(evaluated_info)

    print("Writing new information to CSV")
    helper.write_to_csv(evaluated_info, bus_routes[0])

    # while True:
    #     try:
    #         # Do the data collection
    #         # Want this to happen concurrently, but not sure how threads work in Python
    #         for bus_route in bus_routes:

    #             bus_stop_info = data.get_stop_info(bus_route)
    #             print("Getting list of all bus stop IDs on route {}".format(bus_route))
    #             stop_ids = [stop.get("stopID") for stop in bus_stop_info]

    #             current_info = helper.read_bus_info_from_csv(bus_route, today)
                
    #             print("Getting expected arrival time of buses on route {}".format(bus_route))
    #             bus_information = []
    #             for bus_stop_id in stop_ids:
    #                 expected_arrival_times = data.get_expected_arrival_times(bus_stop_id, bus_route)
    #                 if len(expected_arrival_times) == 0:
    #                     stop_ids.remove(bus_stop_id)
    #                 else:
    #                     bus_information.append(expected_arrival_times)

    #             evaluated_info = data.evaluate_bus_data(bus_information, current_info, bus_stop_info)

    #             evaluated_info = data.check_if_bus_is_due(evaluated_info)

    #             helper.write_to_csv(evaluated_info, bus_route)

    #         time.sleep(30)
    #     except (HTTPError, URLError) as error:
    #         # Send me a notification so I can fix it and keep it running.

main()
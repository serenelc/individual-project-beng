import datetime as dt
import time
from local_helper import Helper
from local_data_collection import Data_Collection
from urllib.error import HTTPError, URLError

def main():

    print(len([['4', '1.0', '1582499600064']]))
    print(len([['4', '1.0', '1582499600101'], ['1', 'Knightsbridge Station  / Harrods', '9', '1', 'Hammersmith', '15447', '1582499876000', '1582499906000'], ['1', 'Knightsbridge Station  / Harrods', '9', '1', 'Hammersmith', '15582', '1582500127000', '1582500157000'], ['1', 'Knightsbridge Station  / Harrods', '9', '1', 'Hammersmith', '15561', '1582500915000', '1582500945000']]))

    # helper = Helper()
    # data = Data_Collection()

    # bus_routes = ["9", "452", "277", "267", "7", "14"]

    # today = dt.datetime.today().strftime('%Y-%m-%d')
    # valid_stops = helper.get_valid_bus_stop_ids(bus_routes[1])

    # while True:
    #     try:
    #         # Do the data collection
    #         # Want this to happen concurrently, but not sure how threads work in Python
    #         # for bus_route in bus_routes:

    #         current_info = helper.read_bus_info_from_csv(bus_routes[1], today)
            
    #         print("Getting expected arrival time of buses on route {}".format(bus_routes[1]))
    #         bus_information = []
    #         for bus_stop in valid_stops:
    #             bus_stop_id = bus_stop.get("stopID")
    #             expected_arrival_times = data.get_expected_arrival_times(bus_stop_id, bus_routes[1])
    #             bus_information.append(expected_arrival_times)

    #         evaluated_info = data.evaluate_bus_data(bus_information, current_info, valid_stops)

    #         evaluated_info = data.check_if_bus_is_due(evaluated_info)

    #         helper.write_to_csv(evaluated_info, bus_routes[1])

    #         time.sleep(30)
    #     except (HTTPError, URLError) as error:
    #         # Send me a notification so I can fix it and keep it running.
    #         print("ERROR IN MAIN: ", error)

main()
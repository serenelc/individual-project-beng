import datetime as dt
import time
import json
from helper import Helper
from data_collection import Data_Collection
from urllib.error import HTTPError, URLError

def lambda_handler(event, context):
    helper = Helper()
    data = Data_Collection()

    bus_routes = ["9", "452", "52", "328", "277", "267", "7", "14"]

    today = dt.datetime.today().strftime('%Y-%m-%d')
    valid_stops = helper.get_valid_bus_stop_ids(bus_routes[1])

    while True:
        try:
            # Do the data collection
            # Want this to happen concurrently, but not sure how threads work in Python
            # for bus_route in bus_routes:
            
            print("Getting expected arrival time of buses on route {}".format(bus_routes[1]))
            bus_information = []
            for bus_stop in valid_stops:
                bus_stop_id = bus_stop.get("stopID")
                expected_arrival_times = data.get_expected_arrival_times(bus_stop_id, bus_routes[1])
                bus_information.append(expected_arrival_times)

            data.evaluate_bus_data(bus_information, valid_stops, bus_routes[1])

            data.check_if_bus_is_due()

            helper.write_to_csv(evaluated_info, bus_routes[1])

            time.sleep(30)
        except (HTTPError, URLError) as error:
            # Send me a notification so I can fix it and keep it running.
            print("ERROR IN MAIN: ", error)
    
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }

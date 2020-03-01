import datetime as dt
import time
import json
from utils import Utilities
from data_collection import Data_Collection
from urllib.error import HTTPError, URLError

def handler(event, context):
    helper = Utilities()
    data = Data_Collection()

    bus_routes = ["452", "9", "52", "328", "277", "267", "7", "14"]

    today = dt.datetime.today().strftime('%Y-%m-%d')
    
    valid_stops = helper.get_valid_stop_ids(bus_routes[0])
    old_info = helper.get_old_info(bus_routes[0])
    print("Old information gathered: {}".format(len(old_info)))

    try:
        # Do the data collection
        # Want this to happen concurrently, but not sure how threads work in Python
        # for bus_route in bus_routes:
        
        start = time.time()
        print("Getting expected arrival time of buses on route {}".format(bus_routes[0]))
        bus_information = []
        for bus_stop in valid_stops:
            bus_stop_id = bus_stop.get("stop_id").get("S")
            new_arrival_info = data.get_expected_arrival_times(bus_stop_id, bus_routes[0])
            bus_information.append(new_arrival_info)

        evaluated_data = data.evaluate_bus_data(bus_information, old_info, valid_stops, bus_routes[0])

        not_arrived, arrived = data.check_if_bus_is_due(evaluated_data)
        
        table_name_arrived = "bus_arrivals_" + bus_routes[0]
        table_name_gathering = "bus_information_" + bus_routes[0]
        
        print(len(not_arrived), len(arrived))
        a = time.time()
        helper.write_to_db(table_name_gathering, not_arrived)
        helper.batch_write_to_db(table_name_arrived, arrived)
        helper.delete_arrived_items(table_name_gathering, arrived)
        b = time.time()
        print("Total time to write and delete from db: ", (b - a))
            
        comp_time = time.time() - start
        print("Entire function: ", comp_time)

    except (HTTPError, URLError) as error:
        # Send me a notification so I can fix it and keep it running.
        print("ERROR IN MAIN: ", error)
    
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
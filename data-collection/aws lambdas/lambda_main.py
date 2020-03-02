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
    
    # Get valid stops for this bus route
    valid_stops = helper.get_valid_stop_ids(bus_routes[0])

    # Get old gathered information on buses not yet arrived from dynamo
    old_bus_info = helper.get_old_info(bus_routes[0])
    print("Old information gathered: {}".format(len(old_bus_info)))

    try:
        
        start = time.time()
        print("Getting expected arrival time of buses on route {}".format(bus_routes[0]))
        new_bus_info = []
        # Get expected arrival times for each stop on the route
        for bus_stop in valid_stops:
            bus_stop_id = bus_stop.get("stop_id").get("S")
            new_arrival_info = data.get_expected_arrival_times(bus_stop_id, bus_routes[0])
            new_bus_info.append(new_arrival_info)

        # Evaluate the new data with respect to the old gathered data
        evaluated_data = data.evaluate_bus_data(new_bus_info, old_bus_info, valid_stops)
        print("##################################################")
        print(evaluated_data)
        print("##################################################")

        # Check which buses have arrived
        not_arrived, arrived = data.check_if_bus_is_due(evaluated_data)
        
        table_name_arrived = "bus_arrivals_" + bus_routes[0]
        table_name_gathering = "bus_information_" + bus_routes[0]
        
        # Write/delete the relevant data to the relevant tables
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
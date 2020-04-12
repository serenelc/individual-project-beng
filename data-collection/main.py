import datetime as dt
import time
import json
import psycopg2
from local_helper import Utilities
from local_data_collection import Data_Collection
from urllib.error import HTTPError, URLError

def main(bus_route):
    helper = Utilities()
    data = Data_Collection()
    
    # Get valid stops for this bus route
    valid_stops = helper.get_valid_stop_ids(bus_route)

    # Get old gathered information on buses not yet arrived from table
    old_bus_info = helper.get_old_info(bus_route)
    print("Old information gathered: {}".format(len(old_bus_info)))

    try:
        
        start = time.time()
        print("Getting expected arrival time of buses on route {}".format(bus_route))
        new_bus_info = []
        
        # Get expected arrival times for each stop on the route
        start_1 = time.time()
        for bus_stop in valid_stops:
            # bus_stop is a tuple (stop_id, stop_name)
            bus_stop_id = bus_stop[0]
            new_arrival_info = data.get_expected_arrival_times(bus_stop_id, bus_route)
            new_bus_info.append(new_arrival_info)
        end = time.time() - start_1
        print("Get expected arrival times: ", end)
        print("New data gathered: ", len(new_bus_info))

    #     # Evaluate the new data with respect to the old gathered data
    #     evaluated_data = data.evaluate_bus_data(new_bus_info, old_bus_info, valid_stops)

    #     not_arrived, arrived = data.check_if_bus_is_due(evaluated_data)

    #     table_name_arrived = "bus_arrivals_" + bus_route
    #     table_name_gathering = "bus_information_" + bus_route
        
    #     # Write/delete the relevant data to the relevant tables
    #     print(len(not_arrived), len(arrived))
    #     a = time.time()
    #     helper.batch_write_to_db(table_name_gathering, not_arrived)

    #     c = time.time()
    #     conn = None
    #     try:
    #         conn = psycopg2.connect(host="localhost", database="postgres", user="postgres", password="postgres")
    #         cursor = conn.cursor()
    #         for arrived_bus in arrived:
    #             helper.write_to_db(cursor, arrived_bus)
    #         cursor.close()
    #     except (Exception, psycopg2.DatabaseError) as error:
    #         print("Error in writing arrived items: ", error)
    #     finally:
    #         if conn is not None:
    #             conn.close()
    #     d = time.time()
    #     print("Time to write arrived items to db: ", (d - c))

    #     helper.delete_arrived_items(table_name_gathering, arrived)
    #     b = time.time()
    #     print("Total time to write and delete from db: ", (b - a))
            
    #     comp_time = time.time() - start
    #     print("Entire function: ", comp_time)

    # except (HTTPError, URLError) as error:
    #     # Send me a notification so I can fix it and keep it running.
    #     print("ERROR IN MAIN: ", error)
    

connected = False
while not connected: 
    try:
        conn = psycopg2.connect(host="db", database="postgres", user="postgres", password="example", port='5432')
    except Exception as e:
        print("Waiting for database to be connected: ", e)
    else:
        connected = True
        print("Connected!!")

bus_routes = ["452", "9", "52", "267", "277", "7", "6", "14", "35", "37", "69"]
bus_routes = ["9"]
for route in bus_routes:
    main(route)
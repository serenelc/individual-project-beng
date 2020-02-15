import datetime as dt
from helper import Helper
from data_collection import Data_Collection


def main():

    helper = Helper()
    data = Data_Collection()

    bus_452 = "452"
    bus_9 = "9"
    today = dt.datetime.today().strftime('%Y-%m-%d')

    bus_stop_info = data.get_stop_info(bus_9)
    print("Getting list of all bus stop IDs on route {}".format(bus_9))
    stop_ids = [stop.get("stopID") for stop in bus_stop_info]

    current_info = helper.read_bus_info_from_csv(bus_9, today)
    
    print("Getting expected arrival time of buses on route {}".format(bus_9))
    bus_information = []
    for bus_stop_id in stop_ids:
        expected_arrival_times = get_expected_arrival_times(bus_stop_id)
        if len(expected_arrival_times) == 0:
            stop_ids.remove(bus_stop_id)
        else:
            bus_information.append(expected_arrival_times)
    
    evaluated_info = evaluate_bus_data(bus_information, current_info)
    check_if_bus_is_due(evaluated_info)

    helper.write_to_csv(evaluated_info, bus_9)

main()
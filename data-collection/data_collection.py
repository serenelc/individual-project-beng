def repeat_call_api(num_calls: int, bus_route_id: str, bus_stop_id: str, info):
    arrival_info = call_countdown_api(bus_route_id, bus_stop_id, info)
    for i in range (0, num_calls):
        i += 1
        time.sleep(30)

        print("======================================================")
        print(dt.datetime.now())
        arrival_info = call_countdown_api(bus_route_id, bus_stop_id, info)
        
    write_to_csv(arrival_info, bus_route_id, bus_stop_id)


def load_bus_information(bus_id, station):
    bus_information = []
    # file_name = 'bus_arrivals_' + bus_id + '_' + station + '.csv'
    file_name = 'bus_arrivals.csv'
    bus_file = Path.cwd() / file_name
   
    if bus_file.is_file():
        try:
            with open(file_name) as csv_file:
                csv_reader = csv.reader(csv_file, delimiter = ',')
                line_count = 0
                for row in csv_reader:
                    if line_count != 0:
                        vehicle_id = row[0]
                        direction = row[1]
                        timestamp = row[2]
                        expected_arrival = row[3]
                        arrived = row[4]
                        vehicle_info = {
                            "vehicle_id": vehicle_id,  
                            "direction": direction,
                            "timestamp": timestamp,
                            "expected_arrival": expected_arrival,
                            "arrived": arrived
                        }
                        bus_information.append(vehicle_info)
                    line_count += 1
        except IOError:
            print("I/O error in loading information from csv file")
    
    return bus_information


def write_to_csv(arrival_array, bus_id, station):
    csv_columns = ['vehicle_id', 'direction', 'timestamp', 'expected_arrival', 'arrived']
    # csv_file = 'bus_arrivals_' + bus_id + '_' + station + '.csv'
    csv_file = 'bus_arrivals.csv'
    try:
        with open(csv_file, 'w') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames = csv_columns)
            writer.writeheader()
            for data in arrival_array:
                writer.writerow(data)
    except IOError:
        print("I/O error in loading information into csv file")


def call_countdown_api(route_id: str, stop_id: str, info):
    try:
        with urllib.request.urlopen("https://api.tfl.gov.uk/StopPoint/" + stop_id + "/arrivals") as api:
            data = json.loads(api.read().decode())
            arrival_times = get_relevant_info(data, route_id, info)
            bus_info = check_if_bus_is_due(arrival_times, info)
            return arrival_times

    except (HTTPError, URLError) as error:
        print("error: ", error)
    except timeout:
        print("timeout error")
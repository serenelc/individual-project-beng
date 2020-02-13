def repeat_call_api(num_calls: int, bus_route_id: str, bus_stop_id: str, info):
    arrival_info = call_countdown_api(bus_route_id, bus_stop_id, info)
    for i in range (0, num_calls):
        i += 1
        time.sleep(30)

        print("======================================================")
        print(dt.datetime.now())
        arrival_info = call_countdown_api(bus_route_id, bus_stop_id, info)
        
    write_to_csv(arrival_info, bus_route_id, bus_stop_id)


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
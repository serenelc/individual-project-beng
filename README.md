### TO DO

- Set up a server to host the code so that I can keep the script running in the background for a minimum of a month

### Overview

To get the information on bus route 'x', first call *get_stop_info(bus_x)*. This calls the TfL API for all the stop points on bus route 'x' and returns a list of all the stop names and stop IDs for this route.

Then, the *load_bus_information(bus_x, today)* is called, where today is just a datetime object representing today's date. This function looks for a csv file with the name 'bus_arrivals_bus_x_today'. If the file doesn't exist, the function returns an empty array. If the file exists, then this implies that some information has already been collected on this bus route today and so the function opens this file and loads the information from the file into a list of dictionaries. Each dictionary contains: 
- vehicle_id (str): id of a bus
- bus_stop_name (str): colloquial bus stop name e.g. 'Green Park Station'
- direction (str): 'Outbound' or 'Inbound'
- expected_arrival (datetime): date and time when the bus is predicted to have arrived e.g. 2020-02-13 22:35:09
- timestamp (datetime): date and time when the call to the API was made to get this predicted arrival time e.g. 2020-02-13 22:27:43.224000
- arrived (str): 'True' or 'False'. 'True' means the bus has arrived at the expected_arrival time.

For each bus stop id that we got from the *get_stop_info(bus_x)* function, we call the *get_expected_arrival_times(stop_id, route_id)* function. This function calls the TfL Countdown API with the given stop_id and route_id and returns the colloquial name of the stop and the line name (== route_id). It also returns, for all buses estimated to arrive at this stop in the next 30 minutes, the destination of the route, the estimated time of arrival, the expire time of this predicted arrival time (30 seconds), the vehicle id and the direction the bus is travelling in. This information is returned in a list. If the stop code ID was invalid, then we return an empty list and then check to see if the returned list has was empty or not. If it was empty, we remove the stop id from the list of ids gotten earlier. If it is not empty, then we append this information onto a list called *bus_information*. 

We then call *evaluate_bus_data(bus_information, current_info)* with *bus_information* and the list returned from *load_bus_information(bus_x, today)* called *current_info*. This checks to see if a vehicle in *bus_information* already exists in *current_info*. If it already exists, then we simply update the expected arrival time with the new estimated time and also update the timestamp to be the new time that the request was made. If it doesn't exist yet, then we add this new vehicle to the *current_info*.

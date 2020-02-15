*TO DO*

- Set up a server to host the code so that I can keep the script running in the background for a minimum of a month


**Overview**

To get the information on bus route 'x', first call *get_stop_info(bus_x)*. This calls the TfL API for all the stop points on bus route 'x' and returns a list of all the stop names and stop IDs for this route.

Then, the *load_bus_information(bus_x, today)* is called, where today is just a datetime object representing today's date. This function looks for a csv file with the name 'bus_arrivals_bus_x_today'. If the file doesn't exist, the function returns an empty array. If the file exists, then this implies that some information has already been collected on this bus route today and so the function opens this file and loads the information from the file into a list of dictionaries. Each dictionary contains: 
- vehicle_id (str): id of a bus
- bus_stop_name (str): colloquial bus stop name e.g. 'Green Park Station'
- direction (str): 'Outbound' or 'Inbound'
- expected_arrival (str): date and time when the bus is predicted to have arrived e.g. 2020-02-13 22:35:09
- timestamp (str): date and time when the call to the API was made to get this predicted arrival time e.g. 2020-02-13 22:27:43.224000
- arrived (str): 'True' or 'False'. 'True' means the bus has arrived at the expected_arrival time.

For each bus stop id that we got from the *get_stop_info(bus_x)* function, we call the *get_expected_arrival_times(bus_stop_id) function*. 

The list returned from *load_bus_information(bus_x, today)* is 
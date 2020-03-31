## TO DO

- Bug in the code. When a bus' estimated arrival time is updated, the vehicle_id gets updated, the old one in the archive should get deleted, but currently, if it is the 2nd or more journey of the day, i.e. the vehicle_id ends in 1 instead of 0, when the bus gets the updated time, it doesn't see the old vehicle in the archive and makes it a new one, so then we end up having 2 vehicles one with BLAH_BLAH_BLAH_0 with the new updated arrival time and BLAH_BLAH_BLAH_1 with the old arrival time. Ok this seems to just be a problem with the 11th of March entries -> writing code now to get rid of this problem.
- Checking for first journey should be done when writing to the database. Because right now if the key is already found in the data, then it completely replaces the item and hence I lose data gathered earlier in the day -> but can't do conditional writes with batch write therefore need to either transition to relational database or not do batch writes for writes to 'bus_arrivals' table.
- Need to update the README with instructions that are valid
- Set up code so that locally can run the csv version while the AWS is dying from 'too many writes per minute' and then can use the batch write to write the stuff from the csv onto the database -> need to make sure the number of journeys doesn't clash though. 
- Investigate moving everything to a relational database in case it makes the data analytics easier.


## Overview

Functions to do with data processing are in the data_collection.py file. Utility functions (e.g. writing to CSVs) are in the helper.py file.

### Objective

The code should concurrently run the data collection functions for 5-10 bus routes for a minimum of a month, 24 hours a day. The code calls the APIs every 30 seconds because that is how often the information gets updated at the source, so there is no point calling the API anymore often. Each bus route should store the arrival information in its own CSV file for each 24 hour day. A new CSV file should be made for each new day, starting at 00:00. 

### What the code does

To get the information on bus route 'x', first call *get_stop_info(bus_x)*. This calls the TfL API for all the stop points on bus route 'x' and returns a list of all the stop names and stop IDs for this route.

Then, the *load_bus_information(bus_x, today)* is called, where today is just a datetime object representing today's date. This function looks for a csv file with the name 'bus_arrivals_bus_x_today'. If the file doesn't exist, the function returns an empty array. If the file exists, then this implies that some information has already been collected on this bus route today and so the function opens this file and loads the information from the file into a list of dictionaries. Each dictionary contains: 
- vehicle_id (str): id of a bus
- bus_stop_name (str): colloquial bus stop name e.g. 'Green Park Station'
- direction (int): 1 == 'Outbound' or 0 == 'Inbound'
- expected_arrival (datetime): date and time when the bus is predicted to have arrived e.g. 2020-02-13 22:35:09
- timestamp (datetime): date and time when the call to the API was made to get this predicted arrival time e.g. 2020-02-13 22:27:43.224000
- arrived (bool): True or False. 'True' means the bus has arrived at the expected_arrival time.

For each bus stop id that we get from the *get_stop_info(bus_x)* function, we call the *get_expected_arrival_times(stop_id, route_id)* function. This function calls the TfL Countdown API with the given stop_id and route_id and returns the colloquial name of the stop and the line name (== route_id). It also returns, for all buses estimated to arrive at this stop in the next 30 minutes, the destination of the route, the estimated time of arrival, the expire time of this predicted arrival time (30 seconds), the vehicle id and the direction the bus is travelling in. This information is returned in a list. If the stop code ID was invalid, then we return an empty list and then check to see if the returned list has was empty or not. If it was empty, we remove the stop id from the list of ids gotten earlier. If it is not empty, then we append this information onto a list called *bus_information*. 

We then call *evaluate_bus_data(bus_information, current_info)* with *bus_information* and the list returned from *load_bus_information(bus_x, today)* called *current_info*. This checks to see if a vehicle in *bus_information* already exists in *current_info*. A vehicle is identified by its uniqueID + stop_code + the number of the trip that this vehicle is on. For example, if bus route 9 with vehicle ID "14577" is at stop "490010984T" and is on its 3rd trip of the day, it would be identified by "14577_490010984T_3". If it already exists, then we simply update the expected arrival time with the new estimated time and also update the timestamp to be the new time that the request was made. If it doesn't exist yet, then we add this new vehicle to the *current_info*.

Then we call *check_if_bus_is_due(evaluated_info)*, which calls *check_if_bus_has_arrived(bus_info, time_now, index)*. These functions work together to compare the estimated arrival time of the bus with the current time. If the eta is before after the current time, then we can infer the vehicle hasn't arrived yet. If the eta is after the current time, then we wait for 3 minutes after the eta to make sure a new eta doesn't return when Countdown is called. If after 3 minutes after the eta, Countdown doesn't return a new eta, then we can infer that the bus has arrived at the predicted time.

Finally, we write this information back to the csv file that we read the information from.

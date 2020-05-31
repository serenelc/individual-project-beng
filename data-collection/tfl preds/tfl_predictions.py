"""
I need the TfL predicted journey times between 2 stops at the same request times as I have 
in the historical model i.e. every 30 minutes (but this could be dropped to every 15 minutes)
so to be safe, I need the TfL predicted journey times between 2 stops every 15 minutes from the hour.

TfL gives predicted arrival times, so in order to get predicted journey times I need to do the following:
- I am at stop A and I want to get the predicted journey time from stop A to stop B
- request Countdown for stop A and stop B.
- get the time of the earliest bus that will leave A.
- find the corresponding vehicle in the Countdown response for stop B
- The difference in these times is TfL's predicted journey time for a bus from stop A to stop B 
at this request time.

For these specific stops and routes
1) 52: Chesterton Road -> Nottinghill Gate in
2) 52: Willesden Bus Garage -> Okehampton Road in
3) 52: Willesden Bus Garage -> Harrow Road / Kilburn Lane in 
4) 52: Willesden Bus Garage -> Chesterton Road in
5) 52: Willesden Bus Garage -> Notting Hill Gate Station in
6) 52: Willesden Bus Garage -> Palace Gate in 
7) 52: Willesden Bus Garage -> Knightsbridge Station / Harrods in
8) 52: Willesden Bus Garage -> Victoria Bus Station in
9) 9: North End Road -> Phillimore Gardens in
10) 52: All Souls Avenue -> Notting Hill Gate Station in
11) 69: Florence Road -> Star Lane out
"""

import datetime as dt
import time
from pathlib import Path
from os import path
import json
import csv
import psycopg2
import urllib.request
from urllib.error import HTTPError, URLError

def main(req_time):
    print("starting!")

    stop_info = load_data()

    willesden_bus_garage = stop_info[0]
    chesterton_road = stop_info[4]
    north_end_road = stop_info[9]
    all_souls_avn = stop_info[1]
    florence_road = stop_info[11]

    start_stops = [willesden_bus_garage for i in range(0, 7)] + [chesterton_road, north_end_road, all_souls_avn, florence_road]
    end_stops = stop_info[2:9] + [stop_info[5], stop_info[10], stop_info[5], stop_info[12]]

    predictions = []

    for i, start_stop in enumerate(start_stops):
        etas = get_expected_arrival_times(start_stop.get("stop_id"), start_stop.get("route_id"))

        earliest_bus_to_leave = evaluate_bus_data(etas)
        # print("First bus to leave: ", earliest_bus_to_leave)

        now = dt.datetime.now()
        pred_arrival_time = find_corresponding_bus(earliest_bus_to_leave.get("vehicle_id"), end_stops[i])

        if pred_arrival_time == 0:
            print("corresponding vehicle not found")
        else:
            pred_jrny_time = pred_arrival_time - earliest_bus_to_leave.get("leave_time")
            print("Predicted journey time: ", pred_jrny_time)
            item = {
                "start_stop": start_stop.get("stop_name"),
                "end_stop": end_stops[i].get("stop_name"),
                "time_of_req": req_time,
                "pred_jrny_time": pred_jrny_time
            }
            predictions.append(item)

    print(predictions)
    # write_to_db(predictions)


def write_to_db(items_to_write):

    for item in items_to_write:
        start_stop = item.get("start_stop")
        end_stop = item.get("end_stop")
        time_of_req = item.get("time_of_req")
        pred_jrny_time = item.get("pred_jrny_time")

        tuple_item = (start_stop, end_stop, time_of_req, pred_jrny_time)

        conn = None
        try:
            conn = psycopg2.connect(host="db", database="postgres", user="postgres", password="example", port="5432")
            cursor = conn.cursor()
            sql = ''.join(("INSERT INTO TfL_predictions (start_stop, end_stop, time_of_req, pred_jrny_time) ",
                    "VALUES (%s, %s, %s, %s) "
                    ))

            cursor.execute(sql, (tuple_item))
            conn.commit()
            cursor.close()

        except (Exception, psycopg2.DatabaseError) as error:
            print("Error in writing to db: ", error)
        finally:
            if conn is not None:
                conn.close()


def find_corresponding_bus(vehicle_id, end_stop):
    etas = get_expected_arrival_times(end_stop.get("stop_id"), end_stop.get("route_id"))
    
    for i, bus_stop in enumerate(etas):
        if i == 0:
            continue

        end_stop_vehicle_id = bus_stop[5]
        if end_stop_vehicle_id == vehicle_id:
            predicted_arrival_time = dt.datetime.fromtimestamp(int(bus_stop[6])/1000.0)
            return predicted_arrival_time
    
    # corresponding bus not found => journey time is more than 30 minutes
    # so this vehicle is not due to arrive at this bus stop for more than 30 minutes.
    return 0


def load_data():

    data = [["52","Willesden Bus Garage","490014687E"], ["52","All Souls Avenue","490003256S"], ["52","Okehampton Road","490010521S"], ["52","Harrow Road / Kilburn Lane","490007860S"], ["52","Chesterton Road","490005139C"], ["52","Notting Hill Gate Station","490015039C"], ["52","Palace Gate","490010728E"], ["52","Knightsbridge Station  / Harrods","490008875KH"], ["52","Victoria Bus Station","490014050A"], ["9","North End Road","490010357F"], ["9","Phillimore Gardens","490010984U"], ["69","Florence Road","490006878N2"], ["69","Star Lane","490012596S"]]
    stop_info = []


    for row in data:
        route_id = row[0]
        stop_name = row[1]
        stop_id = row[2]

        info = {
            "route_id": route_id,
            "stop_name": stop_name,
            "stop_id": stop_id
        }

        stop_info.append(info)
    
    return stop_info


def evaluate_bus_data(bus_data):

    earliest_bus_to_leave = 0
    leave_time = dt.datetime.now() + dt.timedelta(hours = 3)

    if len(bus_data) <=1 :
        #i.e. not buses arriving at this stop in the next 30 minutes
        return -1

    for i, bus_stop in enumerate(bus_data):
        if i == 0:
            continue

        bus_stop_name = bus_stop[1]
        eta = dt.datetime.fromtimestamp(int(bus_stop[6])/1000.0)
        vehicle_id = bus_stop[5]

        bus = {
            "bus_stop_name": bus_stop_name,
            "leave_time": eta,
            "vehicle_id": vehicle_id
        }

        if eta < leave_time:
            leave_time = eta
            earliest_bus_to_leave = bus

    return earliest_bus_to_leave


def get_expected_arrival_times(stop_code: str, route_id: str):
    start = time.time()
    url =  "http://countdown.api.tfl.gov.uk/interfaces/ura/instant_V1?Stopcode2=" + stop_code + "&LineName=" + route_id + "&ReturnList=StopPointName,LineName,DestinationText,EstimatedTime,ExpireTime,VehicleID,DirectionID"
    bus_information = []

    try:
        with urllib.request.urlopen(url) as api:
            data = api.read().decode()
            for line in data.splitlines():
                line = line[1:]
                line = line[:-1]
                line = line.replace("\"", "")
                line_info = list(line.split(","))
                bus_information.append(line_info)

            return bus_information
    
    except (HTTPError, URLError) as error:
        # Invalid stop code, so ignore error. 
        print("Invalid stop code")
        return bus_information
    
    except http.client.RemoteDisconnected as disconnect:
        # remote end closed connection without response. Try again later.
        print("remote disconnected: ", disconnect)
        return bus_information
    
    except timeout:
        print("timeout error when getting expected arrival times")
    comp_time = time.time() - start
    print("Get expected arrival times: ", comp_time)


gmt = dt.timezone.utc
now = dt.datetime.now(tz = gmt)
main(now)
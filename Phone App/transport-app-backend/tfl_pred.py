import datetime as dt
import time
from pathlib import Path
from os import path
import json
import urllib.request
from urllib.error import HTTPError, URLError
import http.client
from socket import timeout

gmt = dt.timezone.utc

def find_corresponding_bus(vehicle_id, end_stop, route):

    etas = get_expected_arrival_times(end_stop, route)
    for i, bus_stop in enumerate(etas):
        if i == 0:
            continue

        end_stop_vehicle_id = bus_stop[5]
        if end_stop_vehicle_id == vehicle_id:
            predicted_arrival_time = dt.datetime.fromtimestamp(int(bus_stop[6])/1000.0)
            predicted_arrival_time = predicted_arrival_time - dt.timedelta(hours = 7)
            predicted_arrival_time = predicted_arrival_time.replace(tzinfo=gmt)

            return predicted_arrival_time
    
    # corresponding bus not found => journey time is more than 30 minutes
    # so this vehicle is not due to arrive at this bus stop for more than 30 minutes.
    return 0


def evaluate_bus_data(bus_data):

    earliest_bus_to_leave = 0
    leave_time = dt.datetime.now(tz = gmt) + dt.timedelta(hours = 3)

    if len(bus_data) <=1 :
        #i.e. not buses arriving at this stop in the next 30 minutes
        return -1

    for i, bus_stop in enumerate(bus_data):
        if i == 0:
            continue

        bus_stop_name = bus_stop[1]
        eta = dt.datetime.fromtimestamp(int(bus_stop[6])/1000.0)
        eta_aware = eta - dt.timedelta(hours = 7)
        eta_aware = eta_aware.replace(tzinfo=gmt)

        vehicle_id = bus_stop[5]

        bus = {
            "bus_stop_name": bus_stop_name,
            "leave_time": eta_aware,
            "vehicle_id": vehicle_id
        }
        
        if eta_aware < leave_time:
            leave_time = eta_aware
            earliest_bus_to_leave = bus

    # print("earliest bus to leave: ", earliest_bus_to_leave)
    return earliest_bus_to_leave


def get_expected_arrival_times(stop_code, route_id):
    url =  "http://countdown.api.tfl.gov.uk/interfaces/ura/instant_V1?Stopcode2=" + stop_code + "&LineName=" + str(route_id) + "&ReturnList=StopPointName,LineName,DestinationText,EstimatedTime,ExpireTime,VehicleID,DirectionID"
    # print(url)
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


class TfL(object):

    def tfl_predict(self, start_stop, end_stop, route):

        etas = get_expected_arrival_times(start_stop, route)

        earliest_bus_to_leave = evaluate_bus_data(etas)

        if earliest_bus_to_leave == -1:
            print("No buses due in the next 30 minutes")
            return False

        pred_arrival_time = find_corresponding_bus(earliest_bus_to_leave.get("vehicle_id"), end_stop, route)

        if pred_arrival_time == 0:
            print("corresponding vehicle not found")
            return False
        
        else:
            pred_jrny_time = pred_arrival_time - earliest_bus_to_leave.get("leave_time")
            return str(pred_jrny_time.total_seconds())
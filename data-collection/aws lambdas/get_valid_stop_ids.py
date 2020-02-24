import json
import boto3
import csv
import urllib.request
from urllib.error import HTTPError, URLError
import json
from socket import timeout
from pathlib import Path

def write_to_db(table_name, valid_stops):
    dynamodb = boto3.client('dynamodb')
    try:
        for stop in valid_stops:
            stop_name = stop.get("stopName")
            stop_id = stop.get("stopID")
            dynamodb.put_item(TableName=table_name, Item={'stop_id': {'S': stop_id},
                                                          'stop_name': {'S': stop_name}, 
                                                        })

    except IOError:
        print("I/O error in writing information into dynamodb")
        
        
def get_stop_info(bus_route_id: str):
    bus_stop_info = []

    try:
        with urllib.request.urlopen("https://api.tfl.gov.uk/line/"+ bus_route_id +"/stoppoints") as api:
            print("Making API call to get stop info")
            data = json.loads(api.read().decode())
            for stop in data:
                info = {
                    "stopName": stop.get("commonName"),
                    "stopID": stop.get("naptanId")
                }
                bus_stop_info.append(info)

            return bus_stop_info
    except (HTTPError, URLError) as error:
        print("error in getting bus stop info: ", error)
    except timeout:
        print("timeout error")
        

def get_expected_arrival_times(stop_code: str, route_id: str):
    url =  "http://countdown.api.tfl.gov.uk/interfaces/ura/instant_V1?Stopcode2=" + stop_code + "&LineName=" + route_id + "&ReturnList=StopPointName"
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
        # Invalid bus ID, so ignore error
        return bus_information
    except timeout:
        print("timeout error when getting expected arrival times")


def get_valid_bus_stop_ids(bus_route):
    bus_stop_info = get_stop_info(bus_route)
    
    print("Getting list of all bus stop IDs on route {}".format(bus_route))
    print("All stop id count: ", len(bus_stop_info))
    
    for i, bus_stop in enumerate(bus_stop_info):
        bus_stop_id = bus_stop.get("stopID")
        expected_arrival_times = get_expected_arrival_times(bus_stop_id, bus_route)
        if len(expected_arrival_times) == 0:
            bus_stop_info.remove(bus_stop)

    print("Valid stop id count: ", len(bus_stop_info))
    return bus_stop_info


def lambda_handler(event, context):
    bus_routes = ["9", "452", "52", "328", "277", "267", "7", "14"]

    valid_stops = get_valid_bus_stop_ids(bus_routes[0])
    write_to_db("valid_stop_ids_" + bus_routes[0], valid_stops)
    
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }

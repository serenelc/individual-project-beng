import json
import boto3
import botocore
import csv
import time
import datetime
from pathlib import Path
from boto3.dynamodb.conditions import Key, Attr
from botocore.exceptions import ClientError

#This reads, updates and writes to dynamodb (despite what the lambda function is called)

def read_from_dynamo(dynamo, tablename, vehicle_id):
    try:
        response = dynamo.get_item(
            TableName=tablename,
            Key={ 
                'vehicle_id': {'S': vehicle_id}
            }
        )
    except ClientError as e:
        print(e.response['Error']['Message'])
    
    else:
        item = response['Item']
        print("GetItem succeeded:")

        vehicle_id = vehicle_id
        bus_stop_name = item.get("bus_stop_name").get("S")
        direction = item.get("direction").get("S")
        eta = item.get("expected_arrival").get("S")
        time_of_req = item.get("time_of_req").get("S")
        arrived = True if item.get("arrived").get("S") else False
        
        print(vehicle_id, bus_stop_name, direction, eta, time_of_req, arrived)
        
        
def update_dynamo(dynamo, tablename, vehicle_id, info_to_update):
    eta = info_to_update.get("expected_arrival")
    ts = info_to_update.get("time_of_req")
    
    try:
        response = dynamo.update_item(
            TableName=tablename,
            Key={'vehicle_id': {'S': vehicle_id}},
            UpdateExpression="set expected_arrival = :eta, time_of_req = :t",
            ExpressionAttributeValues={
                ':eta': {'S': eta},
                ':t': {'S': ts}
            }
        )
    except ClientError as e:
        print(e.response['Error']['Message'])
        
    else:
        print("Update succeeded:")
            
            
def check_if_vehicle_exists(dynamo, tablename, vehicle_id):
    try:
        response = dynamo.query(
            TableName=tablename,
            ExpressionAttributeValues={
                ':v': {
                    'S': vehicle_id,
                },
            },
            KeyConditionExpression='vehicle_id = :v',
        )
    except ClientError as e:
        print(e.response['Error']['Message'])
        
    else:
        print(response['Items'][0])
        print(len(response['Items']))
        
        
def get_all_buses_not_arrived(dynamo, tablename):
    results = []
    
    try:
        filter_expression = 'arrived = :a'
        express_attr_val = {':a': {'S': 'False'}}
        
        response = dynamo.scan(
            TableName = tablename,
            ExpressionAttributeValues = express_attr_val,
            FilterExpression = filter_expression
        )
        
        for i in response['Items']:
            results.append(i)
        
        # Can only scan up to 1MB at a time.
        while 'LastEvaluatedKey' in response:
            response = dynamo.scan(
                TableName = tablename,
                ExpressionAttributeValues = express_attr_val,
                FilterExpression = filter_expression,
                ExclusiveStartKey=response['LastEvaluatedKey']
            )
            for i in response['Items']:
                results.append(i)

    except ClientError as e:
        print(e.response['Error']['Message'])
        
    else:
        print(len(results))
        
        
def get_valid_stop_ids(route):
    print("Reading from dynamo")
    dynamodb = boto3.client('dynamodb')
    tablename = "valid_stop_ids_" + route
    
    results = []
    
    try:
        response = dynamodb.scan(
            TableName = tablename
        )
        
        for i in response['Items']:
            results.append(i)
        
        # Can only scan up to 1MB at a time.
        while 'LastEvaluatedKey' in response:
            response = dynamodb.scan(
                TableName = tablename,
                ExclusiveStartKey=response['LastEvaluatedKey']
            )
            for i in response['Items']:
                results.append(i)

    except ClientError as e:
        print(e.response['Error']['Message'])
        
    else:
        print(results)
        
        
def convert_time_to_datetime(given_time):
    year = int(given_time[:4])
    month = int(given_time[5:7])
    day = int(given_time[8:10])
    hour = int(given_time[11:13])
    minute = int(given_time[14:16])
    second = int(given_time[17:19])

    date_time = dt.datetime(year, month, day, hour, minute, second)
    return date_time
        
        
def convert_types_db(bus):
    vehicle_id = bus.get("vehicle_id").get("S")
    bus_stop_name = bus.get("bus_stop_name").get("S")
    direction = bus.get("direction").get("S")
    eta = convert_time_to_datetime(bus.get("expected_arrival").get("S"))
    time_of_req = convert_time_to_datetime(bus.get("time_of_req").get("S"))
    arrived = True if bus.get("arrived").get("S") else False
    return vehicle_id, bus_stop_name, direction, eta, time_of_req, arrived
        

def get_vehicles_with_id_ending_in_1(route):
    start = time.time()
    dynamodb = boto3.client('dynamodb')
    tablename = "bus_information_" + route
    
    results = []
    
    try:
        response = dynamodb.scan(
            TableName = tablename
        )
        
        for i in response['Items']:
            vehicle_id = i.get("vehicle_id").get("S")
            if vehicle_id[-1] == '1':
                results.append(i)
        
        # Can only scan up to 1MB at a time.
        while 'LastEvaluatedKey' in response:
            response = dynamodb.scan(
                TableName = tablename,
                ExclusiveStartKey=response['LastEvaluatedKey']
            )
            for i in response['Items']:
                vehicle_id = i.get("vehicle_id").get("S")
                if vehicle_id[-1] == '1':
                    results.append(i)

    except ClientError as e:
        print(e.response['Error']['Message'])
        
    else:
        old_information = []
        
        for res in results:
            vehicle_id, bus_stop_name, direction, eta, time_of_req, arrived = convert_types_db(res)
        
            vehicle_info = {
                            "vehicle_id": vehicle_id,
                            "bus_stop_name": bus_stop_name,
                            "direction": direction,
                            "expected_arrival": eta,
                            "time_of_req": time_of_req,
                            "arrived": arrived
                            }
            old_information.append(vehicle_info)
        
        comp_time = time.time() - start
        print("Get old infos: ", comp_time)
        return old_information
        
        
def delete_duplicates(to_delete, route):
    table_name = "bus_information_" + route
    try:
        dynamodb = boto3.client('dynamodb')
        
        for bus in to_delete:
            vehicle_id = bus.get("vehicle_id")
            dynamodb.delete_item(
                Key={
                    'vehicle_id': {
                        'S': vehicle_id,
                    }
                },
                TableName=table_name,
            )
    
    except IOError:
        print("I/O error in deleting information from dynamodb")
        

def get_matching_vehicles(vehicle, route):
    dynamodb = boto3.client('dynamodb')
    vehicle_id = vehicle.get("vehicle_id")
    match = vehicle_id[:-1] + '0'
    tablename = "bus_information_" + route
    
    try:
        response = dynamodb.get_item(
            TableName=tablename,
            Key={ 
                'vehicle_id': {'S': match}
            }
        )
    except ClientError as e:
        print(e.response['Error']['Message'])
    
    else:
        item = response['Item']
        vehicle_id, bus_stop_name, direction, eta, time_of_req, arrived = convert_types_db(item)
        vehicle_info = {
                        "vehicle_id": match,
                        "bus_stop_name": bus_stop_name,
                        "direction": direction,
                        "expected_arrival": eta,
                        "time_of_req": time_of_req,
                        "arrived": arrived
                        }
        if time_of_req < vehicle.get("time_of_req"):
            # print("bus ending in 0 had an earlier request time: bus_0: {} bus_1: {}".format(time_of_req, vehicle.get("time_of_req")))
            vehicle_info = {
                        "vehicle_id": match,
                        "bus_stop_name": bus_stop_name,
                        "direction": direction,
                        "expected_arrival": vehicle.get("expected_arrival"),
                        "time_of_req": vehicle.get("time_of_req"),
                        "arrived": arrived
                        }
                        
        # print("Current: {}, Match: {}".format(vehicle, vehicle_info))
        return vehicle_info
        

def write_to_db(dynamodb, table_name, bus_information, retry):
    start = time.time()
    print("Writing to {}".format(table_name))
    
    if retry:
        try:
            dynamodb.put_item(TableName=table_name, Item=bus_information)
        except IOError:
            print("Error when retrying 2nd or more journey adding to table")
    else:
        vehicle_id = bus_information.get("vehicle_id")
        bus_stop_name = bus_information.get("bus_stop_name")
        direction = str(bus_information.get("direction"))
        eta = str(bus_information.get("expected_arrival"))
        # time_of_req = str(bus_information.get("time_of_req"))
        time_of_req = "test"
        arrived = "True" if bus_information.get("arrived") else "False"
        
        item = {'vehicle_id': {'S': vehicle_id},
                'bus_stop_name': {'S': bus_stop_name},
                'direction': {'S': direction},
                'expected_arrival': {'S': eta},
                'time_of_req': {'S': time_of_req},
                'arrived': {'S': arrived}}
        
        try:
            # dynamodb.put_item(TableName=table_name, 
            #                   Item=item,
            #                   ConditionExpression='attribute_not_exists(vehicle_id)')
            dynamodb.put_item(TableName = table_name, Item = item)
                              
        except ClientError as e:
            print(e)
            # if e.response['Error']['Code'] != 'ConditionalCheckFailedException':
            #     raise
        #     else: #ConditionalCheckFailedException i.e. key already exists -> 2nd journey of the day
        #         trip_num = int(vehicle_id[-1]) + 1
        #         new_id = vehicle_id[:-1] + str(trip_num)
        #         item["vehicle_id"]['S'] = new_id
        #         print("Updated vehicle id: ", item)
        #         write_to_db(dynamodb, table_name, item, True)


def lambda_handler(event, context):
    
    # vehicles_ending_in_1 = get_vehicles_with_id_ending_in_1("7")
    # print(len(vehicles_ending_in_1))
    # matching_vehicles = []
    # for vehicle in vehicles_ending_in_1:
    #     match = get_matching_vehicles(vehicle, "7")
    #     matching_vehicles.append(match)
    # print(matching_vehicles)
    # Add matching_vehicles -> this should replace all the ones ending in 0
    # Delete vehicles_ending_in_1
    # delete_duplicates(vehicles_ending_in_1, "7")
    
    to_write = [{'vehicle_id': '10429_490010531OS_2020-03-31_out_0', 'bus_stop_name': 'New Bond Street', 'direction': 'out', 'expected_arrival': datetime.datetime(2020, 3, 31, 15, 52, 50), 'time_of_req': datetime.datetime(2020, 3, 31, 15, 33, 59), 'arrived': True}, {'vehicle_id': '11142_490010398N_2020-03-31_in_0', 'bus_stop_name': 'North Pole Road', 'direction': 'in', 'expected_arrival': datetime.datetime(2020, 3, 31, 16, 3, 1), 'time_of_req': datetime.datetime(2020, 3, 31, 15, 33, 58), 'arrived': True}, {'vehicle_id': '10544_490000065C_2020-03-31_in_0', 'bus_stop_name': 'East Acton Station / Fitzneal Street', 'direction': 'in', 'expected_arrival': datetime.datetime(2020, 3, 31, 15, 32, 35), 'time_of_req': datetime.datetime(2020, 3, 31, 15, 31, 46), 'arrived': True}, {'vehicle_id': '10544_490000131A_2020-03-31_in_0', 'bus_stop_name': 'Ladbroke Grove Station', 'direction': 'in', 'expected_arrival': datetime.datetime(2020, 3, 31, 15, 44, 6), 'time_of_req': datetime.datetime(2020, 3, 31, 15, 33, 58), 'arrived': True}, {'vehicle_id': '10548_490007707E_2020-03-31_out_0', 'bus_stop_name': 'Hammersmith Hospital', 'direction': 'out', 'expected_arrival': datetime.datetime(2020, 3, 31, 15, 32, 51), 'time_of_req': datetime.datetime(2020, 3, 31, 15, 31, 47), 'arrived': True}, {'vehicle_id': '10548_490014971D_2020-03-31_out_0', 'bus_stop_name': 'Wulfstan Street', 'direction': 'out', 'expected_arrival': datetime.datetime(2020, 3, 31, 15, 33, 57), 'time_of_req': datetime.datetime(2020, 3, 31, 15, 33, 59), 'arrived': True}, {'vehicle_id': '10536_490012797N_2020-03-31_out_0', 'bus_stop_name': "St Mary's Hospital", 'direction': 'out', 'expected_arrival': datetime.datetime(2020, 3, 31, 15, 38, 36), 'time_of_req': datetime.datetime(2020, 3, 31, 15, 33, 59), 'arrived': True}, {'vehicle_id': '10430_490012645S_2020-03-31_out_0', 'bus_stop_name': 'St Charles Health & Wellbeing Centre', 'direction': 'out', 'expected_arrival': datetime.datetime(2020, 3, 31, 15, 42, 40), 'time_of_req': datetime.datetime(2020, 3, 31, 15, 34), 'arrived': True}, {'vehicle_id': '10544_490011137E1_2020-03-31_in_0', 'bus_stop_name': 'Portobello Road', 'direction': 'in', 'expected_arrival': datetime.datetime(2020, 3, 31, 15, 45, 30), 'time_of_req': datetime.datetime(2020, 3, 31, 15, 33, 58), 'arrived': True}, {'vehicle_id': '10578_490011365G_2020-03-31_out_0', 'bus_stop_name': 'Westbourne Grove / Queensway', 'direction': 'out', 'expected_arrival': datetime.datetime(2020, 3, 31, 15, 54, 37), 'time_of_req': datetime.datetime(2020, 3, 31, 15, 34), 'arrived': True}, {'vehicle_id': '10431_490G00011119_2020-03-31_in_0', 'bus_stop_name': 'Porchester Terrace North', 'direction': 'in', 'expected_arrival': datetime.datetime(2020, 3, 31, 15, 40, 53), 'time_of_req': datetime.datetime(2020, 3, 31, 15, 33, 59), 'arrived': True}, {'vehicle_id': '10536_490000131A_2020-03-31_out_0', 'bus_stop_name': 'Ladbroke Grove Station', 'direction': 'out', 'expected_arrival': datetime.datetime(2020, 3, 31, 15, 50, 34), 'time_of_req': datetime.datetime(2020, 3, 31, 15, 33, 58), 'arrived': True}, {'vehicle_id': '10544_490012948S_2020-03-31_in_0', 'bus_stop_name': 'St Stephens Gardens', 'direction': 'in', 'expected_arrival': datetime.datetime(2020, 3, 31, 15, 48, 32), 'time_of_req': datetime.datetime(2020, 3, 31, 15, 33, 58), 'arrived': True}, {'vehicle_id': '10544_490G00007167_2020-03-31_in_0', 'bus_stop_name': 'George Street', 'direction': 'in', 'expected_arrival': datetime.datetime(2020, 3, 31, 16, 1, 38), 'time_of_req': datetime.datetime(2020, 3, 31, 15, 33, 58), 'arrived': True}, {'vehicle_id': '10544_490G00011119_2020-03-31_in_0', 'bus_stop_name': 'Porchester Terrace North', 'direction': 'in', 'expected_arrival': datetime.datetime(2020, 3, 31, 15, 55, 33), 'time_of_req': datetime.datetime(2020, 3, 31, 15, 33, 59), 'arrived': True}, {'vehicle_id': '10431_490G00007167_2020-03-31_in_0', 'bus_stop_name': 'George Street', 'direction': 'in', 'expected_arrival': datetime.datetime(2020, 3, 31, 15, 50, 25), 'time_of_req': datetime.datetime(2020, 3, 31, 15, 33, 58), 'arrived': True}, {'vehicle_id': '10430_490000065H_2020-03-31_out_0', 'bus_stop_name': 'East Acton Station', 'direction': 'out', 'expected_arrival': datetime.datetime(2020, 3, 31, 15, 52, 59), 'time_of_req': datetime.datetime(2020, 3, 31, 15, 33, 59), 'arrived': True}, {'vehicle_id': '10429_490G01221H_2020-03-31_out_0', 'bus_stop_name': 'Paddington Station', 'direction': 'out', 'expected_arrival': datetime.datetime(2020, 3, 31, 16, 1, 42), 'time_of_req': datetime.datetime(2020, 3, 31, 15, 33, 59), 'arrived': True}, {'vehicle_id': '10578_490000144K_2020-03-31_out_0', 'bus_stop_name': 'Marble Arch Station', 'direction': 'out', 'expected_arrival': datetime.datetime(2020, 3, 31, 15, 42, 2), 'time_of_req': datetime.datetime(2020, 3, 31, 15, 34), 'arrived': True}, {'vehicle_id': '11142_490013349A_2020-03-31_in_0', 'bus_stop_name': 'The Fairway', 'direction': 'in', 'expected_arrival': datetime.datetime(2020, 3, 31, 15, 56, 57), 'time_of_req': datetime.datetime(2020, 3, 31, 15, 33, 59), 'arrived': True}, {'vehicle_id': '10544_490010398N_2020-03-31_in_0', 'bus_stop_name': 'North Pole Road', 'direction': 'in', 'expected_arrival': datetime.datetime(2020, 3, 31, 15, 37, 16), 'time_of_req': datetime.datetime(2020, 3, 31, 15, 33, 58), 'arrived': True}, {'vehicle_id': '10430_490004699S_2020-03-31_out_0', 'bus_stop_name': 'Oxford Gardens', 'direction': 'out', 'expected_arrival': datetime.datetime(2020, 3, 31, 15, 41, 53), 'time_of_req': datetime.datetime(2020, 3, 31, 15, 33, 59), 'arrived': True}, {'vehicle_id': '10430_490012948S_2020-03-31_out_0', 'bus_stop_name': 'St Stephens Gardens', 'direction': 'out', 'expected_arrival': datetime.datetime(2020, 3, 31, 15, 35, 30), 'time_of_req': datetime.datetime(2020, 3, 31, 15, 33, 59), 'arrived': True}, {'vehicle_id': '10429_490000144K_2020-03-31_out_0', 'bus_stop_name': 'Marble Arch Station', 'direction': 'out', 'expected_arrival': datetime.datetime(2020, 3, 31, 15, 53, 55), 'time_of_req': datetime.datetime(2020, 3, 31, 15, 34), 'arrived': True}, {'vehicle_id': '10544_490G01221H_2020-03-31_in_0', 'bus_stop_name': 'Paddington Station', 'direction': 'in', 'expected_arrival': datetime.datetime(2020, 3, 31, 16, 0, 1), 'time_of_req': datetime.datetime(2020, 3, 31, 15, 34), 'arrived': True}, {'vehicle_id': '10536_490011137E1_2020-03-31_out_0', 'bus_stop_name': 'Portobello Road', 'direction': 'out', 'expected_arrival': datetime.datetime(2020, 3, 31, 15, 48, 26), 'time_of_req': datetime.datetime(2020, 3, 31, 15, 33, 59), 'arrived': True}, {'vehicle_id': '10430_490000131A_2020-03-31_out_0', 'bus_stop_name': 'Ladbroke Grove Station', 'direction': 'out', 'expected_arrival': datetime.datetime(2020, 3, 31, 15, 40, 17), 'time_of_req': datetime.datetime(2020, 3, 31, 15, 33, 58), 'arrived': True}, {'vehicle_id': '10536_490000144K_2020-03-31_out_0', 'bus_stop_name': 'Marble Arch Station', 'direction': 'out', 'expected_arrival': datetime.datetime(2020, 3, 31, 15, 33, 35), 'time_of_req': datetime.datetime(2020, 3, 31, 15, 31, 47), 'arrived': True}, {'vehicle_id': '10544_490014402E_2020-03-31_in_0', 'bus_stop_name': 'Chepstow Road / Westbourne Grove', 'direction': 'in', 'expected_arrival': datetime.datetime(2020, 3, 31, 15, 50, 50), 'time_of_req': datetime.datetime(2020, 3, 31, 15, 33, 59), 'arrived': True}, {'vehicle_id': '10431_490012948S_2020-03-31_in_0', 'bus_stop_name': 'St Stephens Gardens', 'direction': 'in', 'expected_arrival': datetime.datetime(2020, 3, 31, 15, 36, 31), 'time_of_req': datetime.datetime(2020, 3, 31, 15, 33, 58), 'arrived': True}, {'vehicle_id': '10429_490006321D_2020-03-31_out_0', 'bus_stop_name': 'Paddington Stn / Eastbourne Terrace', 'direction': 'out', 'expected_arrival': datetime.datetime(2020, 3, 31, 16, 3, 11), 'time_of_req': datetime.datetime(2020, 3, 31, 15, 33, 58), 'arrived': True}, {'vehicle_id': '10544_490012646N_2020-03-31_in_0', 'bus_stop_name': 'St Charles Square', 'direction': 'in', 'expected_arrival': datetime.datetime(2020, 3, 31, 15, 41, 2), 'time_of_req': datetime.datetime(2020, 3, 31, 15, 34), 'arrived': True}, {'vehicle_id': '11142_490006211B_2020-03-31_in_0', 'bus_stop_name': 'East Acton Station  / Erconwald Street', 'direction': 'in', 'expected_arrival': datetime.datetime(2020, 3, 31, 15, 57, 48), 'time_of_req': datetime.datetime(2020, 3, 31, 15, 33, 58), 'arrived': True}, {'vehicle_id': '10578_490012797N_2020-03-31_out_0', 'bus_stop_name': "St Mary's Hospital", 'direction': 'out', 'expected_arrival': datetime.datetime(2020, 3, 31, 15, 48, 6), 'time_of_req': datetime.datetime(2020, 3, 31, 15, 33, 59), 'arrived': True}, {'vehicle_id': '10536_490010398N_2020-03-31_out_0', 'bus_stop_name': 'North Pole Road', 'direction': 'out', 'expected_arrival': datetime.datetime(2020, 3, 31, 15, 56, 50), 'time_of_req': datetime.datetime(2020, 3, 31, 15, 34), 'arrived': True}, {'vehicle_id': '10431_490011985E_2020-03-31_in_0', 'bus_stop_name': 'Selfridges', 'direction': 'in', 'expected_arrival': datetime.datetime(2020, 3, 31, 15, 54, 17), 'time_of_req': datetime.datetime(2020, 3, 31, 15, 33, 58), 'arrived': True}, {'vehicle_id': '10536_490012645S_2020-03-31_out_0', 'bus_stop_name': 'St Charles Health & Wellbeing Centre', 'direction': 'out', 'expected_arrival': datetime.datetime(2020, 3, 31, 15, 54, 14), 'time_of_req': datetime.datetime(2020, 3, 31, 15, 34), 'arrived': True}, {'vehicle_id': '10544_490013046N_2020-03-31_in_0', 'bus_stop_name': 'Sussex Gardens', 'direction': 'in', 'expected_arrival': datetime.datetime(2020, 3, 31, 16, 0, 25), 'time_of_req': datetime.datetime(2020, 3, 31, 15, 33, 58), 'arrived': True}, {'vehicle_id': '10578_490000131A_2020-03-31_out_0', 'bus_stop_name': 'Ladbroke Grove Station', 'direction': 'out', 'expected_arrival': datetime.datetime(2020, 3, 31, 16, 2, 17), 'time_of_req': datetime.datetime(2020, 3, 31, 15, 33, 58), 'arrived': True}, {'vehicle_id': '10544_490004702L_2020-03-31_in_0', 'bus_stop_name': 'Cambridge Gardens / Ladbroke Grove', 'direction': 'in', 'expected_arrival': datetime.datetime(2020, 3, 31, 15, 43), 'time_of_req': datetime.datetime(2020, 3, 31, 15, 33, 58), 'arrived': True}, {'vehicle_id': '10429_490013046N_2020-03-31_out_0', 'bus_stop_name': 'Sussex Gardens', 'direction': 'out', 'expected_arrival': datetime.datetime(2020, 3, 31, 15, 59, 15), 'time_of_req': datetime.datetime(2020, 3, 31, 15, 33, 59), 'arrived': True}, {'vehicle_id': '10536_490000144E_2020-03-31_out_0', 'bus_stop_name': 'Marble Arch / Edgware Road', 'direction': 'out', 'expected_arrival': datetime.datetime(2020, 3, 31, 15, 34, 42), 'time_of_req': datetime.datetime(2020, 3, 31, 15, 33, 59), 'arrived': True}, {'vehicle_id': '10578_490013046N_2020-03-31_out_0', 'bus_stop_name': 'Sussex Gardens', 'direction': 'out', 'expected_arrival': datetime.datetime(2020, 3, 31, 15, 47, 37), 'time_of_req': datetime.datetime(2020, 3, 31, 15, 33, 59), 'arrived': True}, {'vehicle_id': '10430_490010398N_2020-03-31_out_0', 'bus_stop_name': 'North Pole Road', 'direction': 'out', 'expected_arrival': datetime.datetime(2020, 3, 31, 15, 47, 17), 'time_of_req': datetime.datetime(2020, 3, 31, 15, 34), 'arrived': True}, {'vehicle_id': '11142_490014971D_2020-03-31_in_0', 'bus_stop_name': 'Wulfstan Street', 'direction': 'in', 'expected_arrival': datetime.datetime(2020, 3, 31, 15, 59, 22), 'time_of_req': datetime.datetime(2020, 3, 31, 15, 33, 58), 'arrived': True}, {'vehicle_id': '10431_490011137E1_2020-03-31_in_0', 'bus_stop_name': 'Portobello Road', 'direction': 'in', 'expected_arrival': datetime.datetime(2020, 3, 31, 15, 33, 25), 'time_of_req': datetime.datetime(2020, 3, 31, 15, 33, 30), 'arrived': True}, {'vehicle_id': '10431_490008697N_2020-03-31_in_0', 'bus_stop_name': 'Shrewsbury Road / Brunel Estate', 'direction': 'in', 'expected_arrival': datetime.datetime(2020, 3, 31, 15, 35, 34), 'time_of_req': datetime.datetime(2020, 3, 31, 15, 33, 59), 'arrived': True}, {'vehicle_id': '10544_490007707E_2020-03-31_in_0', 'bus_stop_name': 'Hammersmith Hospital', 'direction': 'in', 'expected_arrival': datetime.datetime(2020, 3, 31, 15, 34, 57), 'time_of_req': datetime.datetime(2020, 3, 31, 15, 33, 58), 'arrived': True}, {'vehicle_id': '10536_490007707E_2020-03-31_out_0', 'bus_stop_name': 'Hammersmith Hospital', 'direction': 'out', 'expected_arrival': datetime.datetime(2020, 3, 31, 16, 2, 8), 'time_of_req': datetime.datetime(2020, 3, 31, 15, 33, 59), 'arrived': True}, {'vehicle_id': '10430_490012646N_2020-03-31_out_0', 'bus_stop_name': 'St Charles Square', 'direction': 'out', 'expected_arrival': datetime.datetime(2020, 3, 31, 15, 43, 4), 'time_of_req': datetime.datetime(2020, 3, 31, 15, 33, 58), 'arrived': True}, {'vehicle_id': '10545_490G00007167_2020-03-31_in_0', 'bus_stop_name': 'George Street', 'direction': 'in', 'expected_arrival': datetime.datetime(2020, 3, 31, 15, 37, 21), 'time_of_req': datetime.datetime(2020, 3, 31, 15, 33, 58), 'arrived': True}, {'vehicle_id': '10544_490014869W_2020-03-31_in_0', 'bus_stop_name': 'Latymer Upper School / Playing Fields', 'direction': 'in', 'expected_arrival': datetime.datetime(2020, 3, 31, 15, 36, 24), 'time_of_req': datetime.datetime(2020, 3, 31, 15, 34), 'arrived': True}, {'vehicle_id': '10545_490013046N_2020-03-31_in_0', 'bus_stop_name': 'Sussex Gardens', 'direction': 'in', 'expected_arrival': datetime.datetime(2020, 3, 31, 15, 35, 35), 'time_of_req': datetime.datetime(2020, 3, 31, 15, 33, 58), 'arrived': True}, {'vehicle_id': '11142_490015061O_2020-03-31_in_0', 'bus_stop_name': 'Brunel Road', 'direction': 'in', 'expected_arrival': datetime.datetime(2020, 3, 31, 15, 56, 10), 'time_of_req': datetime.datetime(2020, 3, 31, 15, 33, 59), 'arrived': True}, {'vehicle_id': '10544_490014971D_2020-03-31_in_0', 'bus_stop_name': 'Wulfstan Street', 'direction': 'in', 'expected_arrival': datetime.datetime(2020, 3, 31, 15, 33, 28), 'time_of_req': datetime.datetime(2020, 3, 31, 15, 33, 30), 'arrived': True}, {'vehicle_id': '10430_490G00011158_2020-03-31_out_0', 'bus_stop_name': 'Powis Gardens', 'direction': 'out', 'expected_arrival': datetime.datetime(2020, 3, 31, 15, 37, 35), 'time_of_req': datetime.datetime(2020, 3, 31, 15, 34), 'arrived': True}, {'vehicle_id': '10431_490G00011366_2020-03-31_in_0', 'bus_stop_name': 'Queensway', 'direction': 'in', 'expected_arrival': datetime.datetime(2020, 3, 31, 15, 39, 57), 'time_of_req': datetime.datetime(2020, 3, 31, 15, 34), 'arrived': True}, {'vehicle_id': '10578_490006321D_2020-03-31_out_0', 'bus_stop_name': 'Paddington Stn / Eastbourne Terrace', 'direction': 'out', 'expected_arrival': datetime.datetime(2020, 3, 31, 15, 51, 11), 'time_of_req': datetime.datetime(2020, 3, 31, 15, 33, 58), 'arrived': True}, {'vehicle_id': '10536_490011365G_2020-03-31_out_0', 'bus_stop_name': 'Westbourne Grove / Queensway', 'direction': 'out', 'expected_arrival': datetime.datetime(2020, 3, 31, 15, 46, 24), 'time_of_req': datetime.datetime(2020, 3, 31, 15, 34), 'arrived': True}, {'vehicle_id': '11142_490007707E_2020-03-31_in_0', 'bus_stop_name': 'Hammersmith Hospital', 'direction': 'in', 'expected_arrival': datetime.datetime(2020, 3, 31, 16, 0, 53), 'time_of_req': datetime.datetime(2020, 3, 31, 15, 33, 58), 'arrived': True}, {'vehicle_id': '10578_490014406W_2020-03-31_out_0', 'bus_stop_name': 'Westbourne Terrace', 'direction': 'out', 'expected_arrival': datetime.datetime(2020, 3, 31, 15, 52, 18), 'time_of_req': datetime.datetime(2020, 3, 31, 15, 33, 59), 'arrived': True}, {'vehicle_id': '10544_490008697N_2020-03-31_in_0', 'bus_stop_name': 'Shrewsbury Road / Brunel Estate', 'direction': 'in', 'expected_arrival': datetime.datetime(2020, 3, 31, 15, 47, 35), 'time_of_req': datetime.datetime(2020, 3, 31, 15, 33, 59), 'arrived': True}, {'vehicle_id': '10536_490004699S_2020-03-31_out_0', 'bus_stop_name': 'Oxford Gardens', 'direction': 'out', 'expected_arrival': datetime.datetime(2020, 3, 31, 15, 52, 8), 'time_of_req': datetime.datetime(2020, 3, 31, 15, 33, 59), 'arrived': True}, {'vehicle_id': '10430_490014402A_2020-03-31_out_0', 'bus_stop_name': 'Artesian Road', 'direction': 'out', 'expected_arrival': datetime.datetime(2020, 3, 31, 15, 34, 16), 'time_of_req': datetime.datetime(2020, 3, 31, 15, 34), 'arrived': True}, {'vehicle_id': '10578_490012948S_2020-03-31_out_0', 'bus_stop_name': 'St Stephens Gardens', 'direction': 'out', 'expected_arrival': datetime.datetime(2020, 3, 31, 15, 57, 36), 'time_of_req': datetime.datetime(2020, 3, 31, 15, 33, 59), 'arrived': True}, {'vehicle_id': '10430_490014971D_2020-03-31_out_0', 'bus_stop_name': 'Wulfstan Street', 'direction': 'out', 'expected_arrival': datetime.datetime(2020, 3, 31, 15, 51, 22), 'time_of_req': datetime.datetime(2020, 3, 31, 15, 33, 59), 'arrived': True}, {'vehicle_id': '10544_490014406E1_2020-03-31_in_0', 'bus_stop_name': 'Bishops Bridge Road / Westbourne Terrace', 'direction': 'in', 'expected_arrival': datetime.datetime(2020, 3, 31, 15, 57), 'time_of_req': datetime.datetime(2020, 3, 31, 15, 34), 'arrived': True}, {'vehicle_id': '10548_490014869W_2020-03-31_out_0', 'bus_stop_name': 'Latymer Upper School / Playing Fields', 'direction': 'out', 'expected_arrival': datetime.datetime(2020, 3, 31, 15, 31, 24), 'time_of_req': datetime.datetime(2020, 3, 31, 15, 31, 46), 'arrived': True}, {'vehicle_id': '10536_490007167N1_2020-03-31_out_0', 'bus_stop_name': 'Burwood Place', 'direction': 'out', 'expected_arrival': datetime.datetime(2020, 3, 31, 15, 36, 28), 'time_of_req': datetime.datetime(2020, 3, 31, 15, 33, 59), 'arrived': True}, {'vehicle_id': '10545_490G01221H_2020-03-31_in_0', 'bus_stop_name': 'Paddington Station', 'direction': 'in', 'expected_arrival': datetime.datetime(2020, 3, 31, 15, 33, 18), 'time_of_req': datetime.datetime(2020, 3, 31, 15, 33, 32), 'arrived': True}, {'vehicle_id': '10430_490011365G_2020-03-31_out_0', 'bus_stop_name': 'Westbourne Grove / Queensway', 'direction': 'out', 'expected_arrival': datetime.datetime(2020, 3, 31, 15, 32, 4), 'time_of_req': datetime.datetime(2020, 3, 31, 15, 31, 47), 'arrived': True}, {'vehicle_id': '10548_490000065H_2020-03-31_out_0', 'bus_stop_name': 'East Acton Station', 'direction': 'out', 'expected_arrival': datetime.datetime(2020, 3, 31, 15, 35, 31), 'time_of_req': datetime.datetime(2020, 3, 31, 15, 33, 59), 'arrived': True}, {'vehicle_id': '10548_490009299N_2020-03-31_out_0', 'bus_stop_name': 'Long Drive', 'direction': 'out', 'expected_arrival': datetime.datetime(2020, 3, 31, 15, 37, 8), 'time_of_req': datetime.datetime(2020, 3, 31, 15, 33, 59), 'arrived': True}, {'vehicle_id': '10430_490G00008175_2020-03-31_out_0', 'bus_stop_name': 'Highlever Road', 'direction': 'out', 'expected_arrival': datetime.datetime(2020, 3, 31, 15, 44, 35), 'time_of_req': datetime.datetime(2020, 3, 31, 15, 33, 59), 'arrived': True}, {'vehicle_id': '10430_490011137E1_2020-03-31_out_0', 'bus_stop_name': 'Portobello Road', 'direction': 'out', 'expected_arrival': datetime.datetime(2020, 3, 31, 15, 38, 42), 'time_of_req': datetime.datetime(2020, 3, 31, 15, 33, 59), 'arrived': True}, {'vehicle_id': '10431_490G00011158_2020-03-31_in_0', 'bus_stop_name': 'Powis Gardens', 'direction': 'in', 'expected_arrival': datetime.datetime(2020, 3, 31, 15, 34, 1), 'time_of_req': datetime.datetime(2020, 3, 31, 15, 33, 58), 'arrived': True}, {'vehicle_id': '10430_490009299N_2020-03-31_out_0', 'bus_stop_name': 'Long Drive', 'direction': 'out', 'expected_arrival': datetime.datetime(2020, 3, 31, 15, 54, 13), 'time_of_req': datetime.datetime(2020, 3, 31, 15, 33, 59), 'arrived': True}, {'vehicle_id': '10578_490011137E1_2020-03-31_out_0', 'bus_stop_name': 'Portobello Road', 'direction': 'out', 'expected_arrival': datetime.datetime(2020, 3, 31, 16, 0, 26), 'time_of_req': datetime.datetime(2020, 3, 31, 15, 33, 59), 'arrived': True}, {'vehicle_id': '10536_490012646N_2020-03-31_out_0', 'bus_stop_name': 'St Charles Square', 'direction': 'out', 'expected_arrival': datetime.datetime(2020, 3, 31, 15, 53, 10), 'time_of_req': datetime.datetime(2020, 3, 31, 15, 33, 58), 'arrived': True}, {'vehicle_id': '10431_490014406E1_2020-03-31_in_0', 'bus_stop_name': 'Bishops Bridge Road / Westbourne Terrace', 'direction': 'in', 'expected_arrival': datetime.datetime(2020, 3, 31, 15, 42, 17), 'time_of_req': datetime.datetime(2020, 3, 31, 15, 34), 'arrived': True}, {'vehicle_id': '10536_490013046N_2020-03-31_out_0', 'bus_stop_name': 'Sussex Gardens', 'direction': 'out', 'expected_arrival': datetime.datetime(2020, 3, 31, 15, 37, 50), 'time_of_req': datetime.datetime(2020, 3, 31, 15, 33, 59), 'arrived': True}, {'vehicle_id': '10578_490G01221H_2020-03-31_out_0', 'bus_stop_name': 'Paddington Station', 'direction': 'out', 'expected_arrival': datetime.datetime(2020, 3, 31, 15, 49, 42), 'time_of_req': datetime.datetime(2020, 3, 31, 15, 33, 59), 'arrived': True}, {'vehicle_id': '10536_490G00015456_2020-03-31_out_0', 'bus_stop_name': 'Oakworth Road', 'direction': 'out', 'expected_arrival': datetime.datetime(2020, 3, 31, 15, 53, 47), 'time_of_req': datetime.datetime(2020, 3, 31, 15, 33, 58), 'arrived': True}, {'vehicle_id': '10430_490G00015456_2020-03-31_out_0', 'bus_stop_name': 'Oakworth Road', 'direction': 'out', 'expected_arrival': datetime.datetime(2020, 3, 31, 15, 43, 18), 'time_of_req': datetime.datetime(2020, 3, 31, 15, 33, 58), 'arrived': True}, {'vehicle_id': '10544_490015061O_2020-03-31_in_0', 'bus_stop_name': 'Brunel Road', 'direction': 'in', 'expected_arrival': datetime.datetime(2020, 3, 31, 15, 32, 10), 'time_of_req': datetime.datetime(2020, 3, 31, 15, 30, 43), 'arrived': True}, {'vehicle_id': '10429_490007167N1_2020-03-31_out_0', 'bus_stop_name': 'Burwood Place', 'direction': 'out', 'expected_arrival': datetime.datetime(2020, 3, 31, 15, 57, 57), 'time_of_req': datetime.datetime(2020, 3, 31, 15, 33, 59), 'arrived': True}, {'vehicle_id': '10544_490013349A_2020-03-31_in_0', 'bus_stop_name': 'The Fairway', 'direction': 'in', 'expected_arrival': datetime.datetime(2020, 3, 31, 15, 33, 27), 'time_of_req': datetime.datetime(2020, 3, 31, 15, 30, 43), 'arrived': True}, {'vehicle_id': '10578_490G00007429_2020-03-31_out_0', 'bus_stop_name': 'Great Western Road', 'direction': 'out', 'expected_arrival': datetime.datetime(2020, 3, 31, 15, 58, 38), 'time_of_req': datetime.datetime(2020, 3, 31, 15, 33, 59), 'arrived': True}, {'vehicle_id': '10578_490G00011158_2020-03-31_out_0', 'bus_stop_name': 'Powis Gardens', 'direction': 'out', 'expected_arrival': datetime.datetime(2020, 3, 31, 15, 59, 38), 'time_of_req': datetime.datetime(2020, 3, 31, 15, 31, 18), 'arrived': True}, {'vehicle_id': '10536_490014402A_2020-03-31_out_0', 'bus_stop_name': 'Artesian Road', 'direction': 'out', 'expected_arrival': datetime.datetime(2020, 3, 31, 15, 48, 17), 'time_of_req': datetime.datetime(2020, 3, 31, 15, 34), 'arrived': True}, {'vehicle_id': '10430_490007707E_2020-03-31_out_0', 'bus_stop_name': 'Hammersmith Hospital', 'direction': 'out', 'expected_arrival': datetime.datetime(2020, 3, 31, 15, 50, 7), 'time_of_req': datetime.datetime(2020, 3, 31, 15, 33, 59), 'arrived': True}, {'vehicle_id': '10578_490007167N1_2020-03-31_out_0', 'bus_stop_name': 'Burwood Place', 'direction': 'out', 'expected_arrival': datetime.datetime(2020, 3, 31, 15, 46, 16), 'time_of_req': datetime.datetime(2020, 3, 31, 15, 33, 59), 'arrived': True}, {'vehicle_id': '10544_490004699S_2020-03-31_in_0', 'bus_stop_name': 'Oxford Gardens', 'direction': 'in', 'expected_arrival': datetime.datetime(2020, 3, 31, 15, 42, 8), 'time_of_req': datetime.datetime(2020, 3, 31, 15, 33, 58), 'arrived': True}, {'vehicle_id': '10431_490013046N_2020-03-31_in_0', 'bus_stop_name': 'Sussex Gardens', 'direction': 'in', 'expected_arrival': datetime.datetime(2020, 3, 31, 15, 49, 15), 'time_of_req': datetime.datetime(2020, 3, 31, 15, 33, 58), 'arrived': True}, {'vehicle_id': '10536_490G00007252_2020-03-31_out_0', 'bus_stop_name': 'Gloucester Terrace', 'direction': 'out', 'expected_arrival': datetime.datetime(2020, 3, 31, 15, 43, 52), 'time_of_req': datetime.datetime(2020, 3, 31, 15, 33, 58), 'arrived': True}, {'vehicle_id': '10544_490G00011366_2020-03-31_in_0', 'bus_stop_name': 'Queensway', 'direction': 'in', 'expected_arrival': datetime.datetime(2020, 3, 31, 15, 52, 15), 'time_of_req': datetime.datetime(2020, 3, 31, 15, 34), 'arrived': True}, {'vehicle_id': '10578_490000144E_2020-03-31_out_0', 'bus_stop_name': 'Marble Arch / Edgware Road', 'direction': 'out', 'expected_arrival': datetime.datetime(2020, 3, 31, 15, 44, 22), 'time_of_req': datetime.datetime(2020, 3, 31, 15, 33, 59), 'arrived': True}, {'vehicle_id': '10431_490000144K_2020-03-31_in_0', 'bus_stop_name': 'Marble Arch Station', 'direction': 'in', 'expected_arrival': datetime.datetime(2020, 3, 31, 15, 52, 3), 'time_of_req': datetime.datetime(2020, 3, 31, 15, 33, 58), 'arrived': True}, {'vehicle_id': '10536_490G01221H_2020-03-31_out_0', 'bus_stop_name': 'Paddington Station', 'direction': 'out', 'expected_arrival': datetime.datetime(2020, 3, 31, 15, 40, 4), 'time_of_req': datetime.datetime(2020, 3, 31, 15, 33, 59), 'arrived': True}, {'vehicle_id': '10544_490G00011158_2020-03-31_in_0', 'bus_stop_name': 'Powis Gardens', 'direction': 'in', 'expected_arrival': datetime.datetime(2020, 3, 31, 15, 46, 21), 'time_of_req': datetime.datetime(2020, 3, 31, 15, 33, 58), 'arrived': True}, {'vehicle_id': '10578_490004702L_2020-03-31_out_0', 'bus_stop_name': 'Cambridge Gardens / Ladbroke Grove', 'direction': 'out', 'expected_arrival': datetime.datetime(2020, 3, 31, 16, 3, 4), 'time_of_req': datetime.datetime(2020, 3, 31, 15, 33, 58), 'arrived': True}, {'vehicle_id': '10544_490005936E_2020-03-31_in_0', 'bus_stop_name': 'Dalgarno Gardens', 'direction': 'in', 'expected_arrival': datetime.datetime(2020, 3, 31, 15, 39, 25), 'time_of_req': datetime.datetime(2020, 3, 31, 15, 33, 57), 'arrived': True}, {'vehicle_id': '10545_490000144K_2020-03-31_in_0', 'bus_stop_name': 'Marble Arch Station', 'direction': 'in', 'expected_arrival': datetime.datetime(2020, 3, 31, 15, 39, 31), 'time_of_req': datetime.datetime(2020, 3, 31, 15, 33, 58), 'arrived': True}, {'vehicle_id': '10429_490012797N_2020-03-31_out_0', 'bus_stop_name': "St Mary's Hospital", 'direction': 'out', 'expected_arrival': datetime.datetime(2020, 3, 31, 16, 0, 6), 'time_of_req': datetime.datetime(2020, 3, 31, 15, 33, 59), 'arrived': True}, {'vehicle_id': '10545_490014406E1_2020-03-31_in_0', 'bus_stop_name': 'Bishops Bridge Road / Westbourne Terrace', 'direction': 'in', 'expected_arrival': datetime.datetime(2020, 3, 31, 15, 30, 49), 'time_of_req': datetime.datetime(2020, 3, 31, 15, 30, 44), 'arrived': True}, {'vehicle_id': '10536_490014406W_2020-03-31_out_0', 'bus_stop_name': 'Westbourne Terrace', 'direction': 'out', 'expected_arrival': datetime.datetime(2020, 3, 31, 15, 43, 3), 'time_of_req': datetime.datetime(2020, 3, 31, 15, 33, 59), 'arrived': True}, {'vehicle_id': '10430_490G00007429_2020-03-31_out_0', 'bus_stop_name': 'Great Western Road', 'direction': 'out', 'expected_arrival': datetime.datetime(2020, 3, 31, 15, 36, 32), 'time_of_req': datetime.datetime(2020, 3, 31, 15, 33, 59), 'arrived': True}, {'vehicle_id': '10578_490010531OS_2020-03-31_out_0', 'bus_stop_name': 'New Bond Street', 'direction': 'out', 'expected_arrival': datetime.datetime(2020, 3, 31, 15, 40, 50), 'time_of_req': datetime.datetime(2020, 3, 31, 15, 33, 59), 'arrived': True}, {'vehicle_id': '10544_490006211B_2020-03-31_in_0', 'bus_stop_name': 'East Acton Station  / Erconwald Street', 'direction': 'in', 'expected_arrival': datetime.datetime(2020, 3, 31, 15, 31, 36), 'time_of_req': datetime.datetime(2020, 3, 31, 15, 31, 45), 'arrived': True}, {'vehicle_id': '10430_490005936E_2020-03-31_out_0', 'bus_stop_name': 'Dalgarno Gardens', 'direction': 'out', 'expected_arrival': datetime.datetime(2020, 3, 31, 15, 43, 45), 'time_of_req': datetime.datetime(2020, 3, 31, 15, 33, 59), 'arrived': True}, {'vehicle_id': '10536_490010531OS_2020-03-31_out_0', 'bus_stop_name': 'New Bond Street', 'direction': 'out', 'expected_arrival': datetime.datetime(2020, 3, 31, 15, 32, 15), 'time_of_req': datetime.datetime(2020, 3, 31, 15, 31, 46), 'arrived': True}, {'vehicle_id': '10536_490G00007429_2020-03-31_out_0', 'bus_stop_name': 'Great Western Road', 'direction': 'out', 'expected_arrival': datetime.datetime(2020, 3, 31, 15, 50, 20), 'time_of_req': datetime.datetime(2020, 3, 31, 15, 33, 59), 'arrived': True}, {'vehicle_id': '10544_490012645S_2020-03-31_in_0', 'bus_stop_name': 'St Charles Health & Wellbeing Centre', 'direction': 'in', 'expected_arrival': datetime.datetime(2020, 3, 31, 15, 40, 22), 'time_of_req': datetime.datetime(2020, 3, 31, 15, 33, 58), 'arrived': True}, {'vehicle_id': '10431_490000131A_2020-03-31_in_0', 'bus_stop_name': 'Ladbroke Grove Station', 'direction': 'in', 'expected_arrival': datetime.datetime(2020, 3, 31, 15, 31, 52), 'time_of_req': datetime.datetime(2020, 3, 31, 15, 31, 45), 'arrived': True}, {'vehicle_id': '10536_490014869W_2020-03-31_out_0', 'bus_stop_name': 'Latymer Upper School / Playing Fields', 'direction': 'out', 'expected_arrival': datetime.datetime(2020, 3, 31, 16, 1, 7), 'time_of_req': datetime.datetime(2020, 3, 31, 15, 33, 58), 'arrived': True}, {'vehicle_id': '10430_490014869W_2020-03-31_out_0', 'bus_stop_name': 'Latymer Upper School / Playing Fields', 'direction': 'out', 'expected_arrival': datetime.datetime(2020, 3, 31, 15, 49, 1), 'time_of_req': datetime.datetime(2020, 3, 31, 15, 33, 58), 'arrived': True}, {'vehicle_id': '10536_490012948S_2020-03-31_out_0', 'bus_stop_name': 'St Stephens Gardens', 'direction': 'out', 'expected_arrival': datetime.datetime(2020, 3, 31, 15, 49, 18), 'time_of_req': datetime.datetime(2020, 3, 31, 15, 33, 59), 'arrived': True}, {'vehicle_id': '10536_490005936E_2020-03-31_out_0', 'bus_stop_name': 'Dalgarno Gardens', 'direction': 'out', 'expected_arrival': datetime.datetime(2020, 3, 31, 15, 55, 17), 'time_of_req': datetime.datetime(2020, 3, 31, 15, 33, 59), 'arrived': True}, {'vehicle_id': '10536_490004702L_2020-03-31_out_0', 'bus_stop_name': 'Cambridge Gardens / Ladbroke Grove', 'direction': 'out', 'expected_arrival': datetime.datetime(2020, 3, 31, 15, 51, 21), 'time_of_req': datetime.datetime(2020, 3, 31, 15, 33, 58), 'arrived': True}, {'vehicle_id': '10536_490006321D_2020-03-31_out_0', 'bus_stop_name': 'Paddington Stn / Eastbourne Terrace', 'direction': 'out', 'expected_arrival': datetime.datetime(2020, 3, 31, 15, 41, 32), 'time_of_req': datetime.datetime(2020, 3, 31, 15, 33, 58), 'arrived': True}, {'vehicle_id': '10536_490G00008175_2020-03-31_out_0', 'bus_stop_name': 'Highlever Road', 'direction': 'out', 'expected_arrival': datetime.datetime(2020, 3, 31, 15, 55, 53), 'time_of_req': datetime.datetime(2020, 3, 31, 15, 33, 59), 'arrived': True}, {'vehicle_id': '10545_490011985E_2020-03-31_in_0', 'bus_stop_name': 'Selfridges', 'direction': 'in', 'expected_arrival': datetime.datetime(2020, 3, 31, 15, 41, 3), 'time_of_req': datetime.datetime(2020, 3, 31, 15, 33, 58), 'arrived': True}, {'vehicle_id': '10430_490004702L_2020-03-31_out_0', 'bus_stop_name': 'Cambridge Gardens / Ladbroke Grove', 'direction': 'out', 'expected_arrival': datetime.datetime(2020, 3, 31, 15, 41), 'time_of_req': datetime.datetime(2020, 3, 31, 15, 33, 58), 'arrived': True}, {'vehicle_id': '10578_490014402A_2020-03-31_out_0', 'bus_stop_name': 'Artesian Road', 'direction': 'out', 'expected_arrival': datetime.datetime(2020, 3, 31, 15, 56, 32), 'time_of_req': datetime.datetime(2020, 3, 31, 15, 34), 'arrived': True}, {'vehicle_id': '10578_490G00007252_2020-03-31_out_0', 'bus_stop_name': 'Gloucester Terrace', 'direction': 'out', 'expected_arrival': datetime.datetime(2020, 3, 31, 15, 53, 7), 'time_of_req': datetime.datetime(2020, 3, 31, 15, 33, 58), 'arrived': True}, {'vehicle_id': '11142_490000065C_2020-03-31_in_0', 'bus_stop_name': 'East Acton Station / Fitzneal Street', 'direction': 'in', 'expected_arrival': datetime.datetime(2020, 3, 31, 15, 58, 25), 'time_of_req': datetime.datetime(2020, 3, 31, 15, 33, 59), 'arrived': True}, {'vehicle_id': '10431_490G01221H_2020-03-31_in_0', 'bus_stop_name': 'Paddington Station', 'direction': 'in', 'expected_arrival': datetime.datetime(2020, 3, 31, 15, 47, 43), 'time_of_req': datetime.datetime(2020, 3, 31, 15, 34), 'arrived': True}, {'vehicle_id': '10544_490011962E_2020-03-31_in_0', 'bus_stop_name': 'North Pole Road / Scrubs Lane', 'direction': 'in', 'expected_arrival': datetime.datetime(2020, 3, 31, 15, 37, 39), 'time_of_req': datetime.datetime(2020, 3, 31, 15, 33, 59), 'arrived': True}, {'vehicle_id': '10429_490000144E_2020-03-31_out_0', 'bus_stop_name': 'Marble Arch / Edgware Road', 'direction': 'out', 'expected_arrival': datetime.datetime(2020, 3, 31, 15, 56, 8), 'time_of_req': datetime.datetime(2020, 3, 31, 15, 33, 59), 'arrived': True}, {'vehicle_id': '10431_490014402E_2020-03-31_in_0', 'bus_stop_name': 'Chepstow Road / Westbourne Grove', 'direction': 'in', 'expected_arrival': datetime.datetime(2020, 3, 31, 15, 38, 49), 'time_of_req': datetime.datetime(2020, 3, 31, 15, 33, 59), 'arrived': True}, {'vehicle_id': '10536_490G00011158_2020-03-31_out_0', 'bus_stop_name': 'Powis Gardens', 'direction': 'out', 'expected_arrival': datetime.datetime(2020, 3, 31, 15, 51, 19), 'time_of_req': datetime.datetime(2020, 3, 31, 15, 31, 47), 'arrived': True}]
    
    delete_duplicates(to_write, "7")
    
    return {
        'statusCode': 200,
        'body': json.dumps('Success')
    }

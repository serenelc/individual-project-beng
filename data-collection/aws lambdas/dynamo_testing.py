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
            
        
        
def convert_time_to_datetime(given_time):
    year = int(given_time[:4])
    month = int(given_time[5:7])
    day = int(given_time[8:10])
    hour = int(given_time[11:13])
    minute = int(given_time[14:16])
    second = int(given_time[17:19])

    date_time = datetime.datetime(year, month, day, hour, minute, second)
    return date_time
        
        
def convert_types_db(bus):
    vehicle_id = bus.get("vehicle_id").get("S")
    bus_stop_name = bus.get("bus_stop_name").get("S")
    direction = bus.get("direction").get("S")
    eta = bus.get("expected_arrival").get("S")
    time_of_req = bus.get("time_of_req").get("S")
    arrived = True if bus.get("arrived").get("S") else False
    return vehicle_id, bus_stop_name, direction, eta, time_of_req, arrived
        

def get_vehicles_with_id_ending_in_1(route):
    start = time.time()
    dynamodb = boto3.client('dynamodb')
    tablename = "bus_arrivals_" + route
    
    results = []
    
    try:
        response = dynamodb.scan(
            TableName = tablename
        )
        
        for i in response['Items']:
            vehicle_id = i.get("vehicle_id").get("S")
            [_, _, _, _, num_trip] = vehicle_id.split('_')
            if int(num_trip) > 3:
                results.append(i)
        
        # Can only scan up to 1MB at a time.
        while 'LastEvaluatedKey' in response:
            response = dynamodb.scan(
                TableName = tablename,
                ExclusiveStartKey=response['LastEvaluatedKey']
            )
            for i in response['Items']:
                vehicle_id = i.get("vehicle_id").get("S")
                [_, _, _, _, num_trip] = vehicle_id.split('_')
                if int(num_trip) > 3:
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
        print("Get vehicles with ids ending in blah: ", comp_time)
        return old_information
        
        
def delete_duplicates(to_delete, route):
    table_name = "bus_arrivals_" + route
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
    [a, b, c, d, _] = vehicle_id.split('_')
    match = a + b + c + d + '_0'
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
                        
        return vehicle_info
        

def try_write_to_db(self, dynamodb, route, bus_information):
    print("This is not the 1st journey of the day. Updated vehicle id: ", bus_information)
    table_name = "bus_arrivals_" + route
    
    try:
        dynamodb.put_item(TableName=table_name, 
                              Item=bus_information,
                              ConditionExpression='attribute_not_exists(vehicle_id)')
                              
    except ClientError as e:
        if e.response['Error']['Code'] != 'ConditionalCheckFailedException':
            raise
        # Try adding a different error check here and in try_write too
        else: #ConditionalCheckFailedException i.e. key already exists -> 2nd journey of the day
            vehicle_id = bus_information.get("vehicle_id").get("S")
            [_, _, _, _, num_trip] = vehicle_id.split('_')
            trip_num = int(num_trip) + 1
            new_id = vehicle_id[:-1] + str(trip_num)
            bus_information["vehicle_id"]['S'] = new_id
            print("Failed to write. Try again")
            try_write_to_db(dynamodb, route, bus_information)
            
            
def write_to_db(dynamodb, route, bus_information):
    start = time.time()
    table_name = "bus_arrivals_" + route
    
    vehicle_id = bus_information.get("vehicle_id")
    bus_stop_name = bus_information.get("bus_stop_name")
    direction = str(bus_information.get("direction"))
    eta = str(bus_information.get("expected_arrival"))
    time_of_req = str(bus_information.get("time_of_req"))
    arrived = "True" if bus_information.get("arrived") else "False"
    
    item = {'vehicle_id': {'S': vehicle_id},
            'bus_stop_name': {'S': bus_stop_name},
            'direction': {'S': direction},
            'expected_arrival': {'S': eta},
            'time_of_req': {'S': time_of_req},
            'arrived': {'S': arrived}}
    
    try:
        dynamodb.put_item(TableName=table_name, 
                          Item=item,
                          ConditionExpression='attribute_not_exists(vehicle_id)')
                          
    except ClientError as e:
        if e.response['Error']['Code'] != 'ConditionalCheckFailedException':
            print(e)
            raise
        # Try adding a different error check here and in try_write too
        else: #ConditionalCheckFailedException i.e. key already exists -> 2nd journey of the day
            msg = e.response['Error']['Code']
            print("ERROR: ", msg)
            print("ERROR: ", e)
            # [_, _, _, _, num_trip] = vehicle_id.split('_')
            # trip_num = int(num_trip) + 1
            # new_id = vehicle_id[:-1] + str(trip_num)
            # item["vehicle_id"]['S'] = new_id
            # self.try_write_to_db(dynamodb, route, item)
    
    comp_time = time.time() - start
    # print("Write arrived items to db: ", comp_time)
        

def get_old_info(route):
    start = time.time()
    dynamodb = boto3.client('dynamodb')
    tablename = "bus_arrivals_" + route
    
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


def lambda_handler(event, context):
    
    # delete all data collected today from all tables
    
    # Routes to debug
    # 6: 2500 -
    info = get_old_info(bus_routes[10])
    to_delete = []
    for item in info:
        today = "2020-04-05"
        if item.get("expected_arrival").startswith(today):
            to_delete.append(item)
    print(len(to_delete))
    delete_duplicates(to_delete, bus_routes[10])
        
    # print("Number of items in database: ", len(info))
    
    # duplicates = []
    # copy = info
    # for item in info[20500:]:
    #     a = item.get("bus_stop_name")
    #     b = item.get("direction")
    #     c = item.get("time_of_req")
    #     d = item.get("expected_arrival")
    #     copy.remove(item)
        
    #     for j in copy:
    #         if (a == j.get("bus_stop_name")) & (b == j.get("direction")) & (c == j.get("time_of_req")) & (d == j.get("expected_arrival")):
    #             duplicates.append(j)
    
    # if len(duplicates) > 0:
    #     print(duplicates[0])
    #     print("num duplicates to delete: ", len(duplicates))
    #     delete_duplicates(duplicates, "277")
    
    return {
        'statusCode': 200,
        'body': json.dumps('Success')
    }

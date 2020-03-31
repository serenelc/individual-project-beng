import json
import boto3
import botocore
import csv
import time
import datetime as dt
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
        direction = item.get("direction").get("N")
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
    tablename = "bus_arrivals_" + route
    
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
    match = vehicle_id[:-1] + '0'
    tablename = "bus_arrivals_" + route
    
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
                        "vehicle_id": vehicle_id,
                        "bus_stop_name": bus_stop_name,
                        "direction": direction,
                        "expected_arrival": eta,
                        "time_of_req": time_of_req,
                        "arrived": arrived
                        }
                        
        print("Current: {}, Match: {}".format(vehicle, vehicle_info))
        

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
        time_of_req = str(bus_information.get("time_of_req"))
        arrived = "True" if bus_information.get("arrived") else "False"
        
        item = {'vehicle_id': {'S': vehicle_id},
                'bus_stop_name': {'S': bus_stop_name},
                'direction': {'N': direction},
                'expected_arrival': {'S': eta},
                'time_of_req': {'S': time_of_req},
                'arrived': {'S': arrived}}
        
        try:
            dynamodb.put_item(TableName=table_name, 
                              Item=item,
                              ConditionExpression='attribute_not_exists(vehicle_id)')
                              
        except ClientError as e:
            if e.response['Error']['Code'] != 'ConditionalCheckFailedException':
                raise
            else: #ConditionalCheckFailedException i.e. key already exists -> 2nd journey of the day
                trip_num = int(vehicle_id[-1]) + 1
                new_id = vehicle_id[:-1] + str(trip_num)
                item["vehicle_id"]['S'] = new_id
                print("Updated vehicle id: ", item)
                write_to_db(dynamodb, table_name, item, True)


def lambda_handler(event, context):
    
    vehicles_ending_in_1 = get_vehicles_with_id_ending_in_1("9")
    print(len(vehicles_ending_in_1))
    for vehicle in vehicles_ending_in_1:
        get_matching_vehicles(vehicle, "9")
    delete_duplicates(vehicles_ending_in_1, "9")
    
    # dynamodb = boto3.client('dynamodb')
    # test = '16434_490008690E_2020-03-11_out_0'
    # tablename = "bus_arrivals_267"
    # bus_information = {'vehicle_id': test,
    #             'bus_stop_name': "test",
    #             'direction': 0,
    #             'expected_arrival': "test",
    #             'time_of_req': "test",
    #             'arrived': True}
    # write_to_db(dynamodb, tablename, bus_information, False)
    
    
    return {
        'statusCode': 200,
        'body': json.dumps('Success')
    }

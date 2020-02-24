import json
import boto3
import csv
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
        
    
def write_csv_to_dynamo(dynamo, tablename, csv):
    file_name = csv
    bus_file = Path.cwd() / file_name

    if bus_file.is_file():
        try:
            with open(file_name) as csv_file:
                csv_reader = csv.reader(csv_file, delimiter = ',')
                line_count = 0
                for row in csv_reader:
                    if line_count != 0:
                        vehicle_id = row[0]
                        bus_stop_name = row[1]
                        direction = row[2]
                        eta = row[3]
                        time_of_req = row[4]
                        arrived = 'True' if row[5] == 'True' else 'False'
                        
                        dynamo.put_item(TableName=tablename, Item={'vehicle_id': {'S': vehicle_id},
                                                                   'bus_stop_name': {'S': bus_stop_name},
                                                                   'direction': {'N': direction},
                                                                   'expected_arrival': {'S': eta},
                                                                   'time_of_req': {'S': time_of_req},
                                                                   'arrived': {'S': arrived}})
    

                    line_count += 1
        except IOError:
            print("I/O error in loading information from csv file into dynamodb")
            
            
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
    

def lambda_handler(event, context):
    dynamodb = boto3.client('dynamodb')

    file_name = 'bus_data.csv'
    
    info_to_update = {"expected_arrival": "TIME", "time_of_req": "REQ"}
    
    # read_from_dynamo(dynamodb, "bus_arrivals", "14462_490003112J_2020-02-17_0")
    # write_csv_to_dynamo(dynamodb, "bus_arrivals", file_name)
    
    # update_dynamo(dynamodb, "bus_arrivals_9", "14576_490000130KE_2020-02-23_0_0", info_to_update)
    
    # check_if_vehicle_exists(dynamodb, "bus_arrivals", "14462_490003112J_2020-02-17_0")
    # get_all_buses_not_arrived(dynamodb, "bus_arrivals")
    
    get_valid_stop_ids("9")
    
    return {
        'statusCode': 200,
        'body': json.dumps('Success')
    }

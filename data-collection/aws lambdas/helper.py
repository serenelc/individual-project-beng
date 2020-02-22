import json
import boto3
import csv
import datetime as dt
from pathlib import Path

class Helper(object):

    def convert_time_to_datetime(self, given_time):
        year = int(given_time[:4])
        month = int(given_time[5:7])
        day = int(given_time[8:10])
        hour = int(given_time[11:13])
        minute = int(given_time[14:16])
        second = int(given_time[17:19])

        date_time = dt.datetime(year, month, day, hour, minute, second)
        return date_time
        
        
    def read_from_db(self, table_name, vehicle_id):
        dynamodb = boto3.client('dynamodb')
        try:
            response = dynamo.get_item(
                TableName=tablename,
                Key={ 
                    'vehicle_id': {'S': vehicle_id}
                }
            )
        except ClientError as e:
            print("Error reading information from dynamodb")
            print(e.response['Error']['Message'])
        
        else:
            item = response['Item']
            vehicle_id = vehicle_id
            bus_stop_name = item.get("bus_stop_name").get("S")
            direction = item.get("direction").get("N")
            eta = convert_time_to_datetime(item.get("expected_arrival").get("S"))
            timestamp = convert_time_to_datetime(item.get("timestamp").get("S"))
            arrived = True if item.get("arrived").get("S") else False
            
            vehicle_info = {
                            "vehicle_id": vehicle_id,
                            "bus_stop_name": bus_stop_name,
                            "direction": direction,
                            "expected_arrival": eta,
                            "timestamp": timestamp,
                            "arrived": arrived
                            }
                            
            return vehicle_info


    def update_dynamo(self, tablename, info_to_update):
        dynamodb = boto3.client('dynamodb')
        vehicle_id = info_to_update.get("vehicle_id")
        eta = info_to_update.get("expected_arrival")
        timestamp = info_to_update.get("timestamp")
        
        try:
            response = dynamodb.update_item(
                TableName=tablename,
                Key={'vehicle_id': {'S': vehicle_id}},
                UpdateExpression="set expected_arrival = :eta, timestamp = :t",
                ExpressionAttributeValues={
                    ':eta': {'S': eta},
                    ':t': {'S': timestamp}
                }
            )
        except ClientError as e:
            print(e.response['Error']['Message'])
            
        else:
            print("Update succeeded:")
        

    def write_to_db(self, table_name, arrival_array, bus_route_id):
        dynamodb = boto3.client('dynamodb')
        today = dt.datetime.today().strftime('%Y-%m-%d')
    
        file_name = 'bus_data.csv'
        bus_file = Path.cwd() / file_name

        try:
            for data in arrival_array:
                vehicle_id = data[0]
                bus_stop_name = data[1]
                direction = str(data[2])
                expected_arrival = data[3]
                timestamp = data[4]
                arrived = data[5]
                dynamodb.put_item(TableName=table_name, Item={'vehicle_id': {'S': vehicle_id},
                                                              'bus_stop_name': {'S': bus_stop_name},
                                                              'direction': {'N': direction},
                                                              'expected_arrival': {'S': eta},
                                                              'timestamp': {'S': timestamp},
                                                              'arrived': {'S': arrived}})
        
    
        except IOError:
            print("I/O error in loading information from csv file into dynamodb")

    
    def check_if_vehicle_exists(self, tablename, vehicle_id):
        dynamodb = boto3.client('dynamodb')

        try:
            response = dynamodb.query(
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
            #len = 0 if the vehicle doesn't exist in the table
            return len(response['Items'])


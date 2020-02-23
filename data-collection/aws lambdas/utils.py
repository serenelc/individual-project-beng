import json
import boto3
import csv
import datetime as dt
from pathlib import Path

class Utilities(object):

    def convert_time_to_datetime(self, given_time):
        year = int(given_time[:4])
        month = int(given_time[5:7])
        day = int(given_time[8:10])
        hour = int(given_time[11:13])
        minute = int(given_time[14:16])
        second = int(given_time[17:19])

        date_time = dt.datetime(year, month, day, hour, minute, second)
        return date_time


    def convert_types_db(self, bus):
        vehicle_id = bus.get("vehicle_id").get("S")
        bus_stop_name = bus.get("bus_stop_name").get("S")
        direction = bus.get("direction").get("N")
        eta = self.convert_time_to_datetime(bus.get("expected_arrival").get("S"))
        time_of_req = self.convert_time_to_datetime(bus.get("time_of_req").get("S"))
        arrived = True if bus.get("arrived").get("S") else False
        return vehicle_id, bus_stop_name, direction, eta, time_of_req, arrived
        
        
    def read_from_db(self, table_name, vehicle_id):
        print("Reading from dynamo")
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
            vehicle_id, bus_stop_name, direction, eta, time_of_req, arrived = self.convert_types_db(item)
            
            vehicle_info = {
                            "vehicle_id": vehicle_id,
                            "bus_stop_name": bus_stop_name,
                            "direction": direction,
                            "expected_arrival": eta,
                            "time_of_req": time_of_req,
                            "arrived": arrived
                            }
                            
            return vehicle_info


    def update_dynamo(self, tablename, vehicle_id, info_to_update):
        dynamodb = boto3.client('dynamodb')
        eta = info_to_update.get("expected_arrival")
        time_of_req = info_to_update.get("time_of_req")
        
        try:
            response = dynamodb.update_item(
                TableName=tablename,
                Key={'vehicle_id': {'S': vehicle_id}},
                UpdateExpression="set expected_arrival = :eta, time_of_req = :t",
                ExpressionAttributeValues={
                    ':eta': {'S': eta},
                    ':t': {'S': time_of_req}
                }
            )
        except ClientError as e:
            print(e.response['Error']['Message'])
            
        else:
            print("Update succeeded:")
        

    def write_to_db(self, table_name, bus_information):
        # If an item that has the same primary key as the new item already exists 
        # in the specified table, the new item completely replaces the existing item.

        dynamodb = boto3.client('dynamodb')

        try:
            vehicle_id = bus_information.get("vehicle_id")
            bus_stop_name = bus_information.get("bus_stop_name")
            direction = str(bus_information.get("direction"))
            eta = str(bus_information.get("expected_arrival"))
            time_of_req = str(bus_information.get("time_of_req"))
            arrived = "True" if bus_information.get("arrived") else "False"
            dynamodb.put_item(TableName=table_name, Item={'vehicle_id': {'S': vehicle_id},
                                                              'bus_stop_name': {'S': bus_stop_name},
                                                              'direction': {'N': direction},
                                                              'expected_arrival': {'S': eta},
                                                              'time_of_req': {'S': time_of_req},
                                                              'arrived': {'S': arrived}})
    
        except IOError:
            print("I/O error in writing information into dynamodb")

    
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
            #response = [] if the vehicle doesn't exist in the table
            return response['Items']


    def get_all_buses_not_arrived(self, tablename):
        dynamodb = boto3.client('dynamodb')
        results = []
        
        try:
            filter_expression = 'arrived = :a'
            express_attr_val = {':a': {'S': 'False'}}
            
            response = dynamodb.scan(
                TableName = tablename,
                ExpressionAttributeValues = express_attr_val,
                FilterExpression = filter_expression
            )
            
            for i in response['Items']:
                results.append(i)
            
            # Can only scan up to 1MB at a time.
            while 'LastEvaluatedKey' in response:
                response = dynamodb.scan(
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
            buses_not_arrived = []
            for bus in results:
                vehicle_id, bus_stop_name, direction, eta, time_of_req, arrived = self.convert_types_db(bus)
            
                vehicle_info = {
                                "vehicle_id": vehicle_id,
                                "bus_stop_name": bus_stop_name,
                                "direction": direction,
                                "expected_arrival": eta,
                                "time_of_req": time_of_req,
                                "arrived": arrived
                                }
                buses_not_arrived.append(vehicle_info)
            return buses_not_arrived


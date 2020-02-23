import json
import boto3
import csv
import datetime as dt
from pathlib import Path
from data_collection import Data_Collection

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


    def convert_types_db(self, bus):
        vehicle_id = bus.get("vehicle_id").get("S")
        bus_stop_name = bus.get("bus_stop_name").get("S")
        direction = bus.get("direction").get("N")
        eta = convert_time_to_datetime(bus.get("expected_arrival").get("S"))
        timestamp = convert_time_to_datetime(bus.get("timestamp").get("S"))
        arrived = True if bus.get("arrived").get("S") else False
        return vehicle_id, bus_stop_name, direction, eta, timestamp, arrived
        
        
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
            vehicle_id, bus_stop_name, direction, eta, timestamp, arrived = self.convert_types_db(item)
            
            vehicle_info = {
                            "vehicle_id": vehicle_id,
                            "bus_stop_name": bus_stop_name,
                            "direction": direction,
                            "expected_arrival": eta,
                            "timestamp": timestamp,
                            "arrived": arrived
                            }
                            
            return vehicle_info


    def update_dynamo(self, tablename, vehicle_id, info_to_update):
        dynamodb = boto3.client('dynamodb')
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
        

    def write_to_db(self, table_name, bus_information):
        # If an item that has the same primary key as the new item already exists 
        # in the specified table, the new item completely replaces the existing item.

        dynamodb = boto3.client('dynamodb')

        try:
            vehicle_id = bus_information.get("vehicle_id")
            bus_stop_name = bus_information.get("bus_stop_name")
            direction = str(bus_information.get("direction"))
            eta = str(bus_information.get("expected_arrival"))
            timestamp = str(bus_information.get("expected_arrival"))
            arrived = "True" if bus_information.get("arrived") else "False"
            dynamodb.put_item(TableName=table_name, Item={'vehicle_id': {'S': vehicle_id},
                                                              'bus_stop_name': {'S': bus_stop_name},
                                                              'direction': {'N': direction},
                                                              'expected_arrival': {'S': eta},
                                                              'timestamp': {'S': timestamp},
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
                vehicle_id, bus_stop_name, direction, eta, timestamp, arrived = self.convert_types_db(bus)
            
                vehicle_info = {
                                "vehicle_id": vehicle_id,
                                "bus_stop_name": bus_stop_name,
                                "direction": direction,
                                "expected_arrival": eta,
                                "timestamp": timestamp,
                                "arrived": arrived
                                }
                buses_not_arrived.append(vehicle_info)
            return buses_not_arrived


    def get_valid_bus_stop_ids(self, bus_route):
        data = Data_Collection()

        bus_stop_info = data.get_stop_info(bus_route)
        print("Getting list of all bus stop IDs on route {}".format(bus_route))
        print(len(bus_stop_info))
        
        for i, bus_stop in enumerate(bus_stop_info):
            bus_stop_id = bus_stop.get("stopID")
            expected_arrival_times = data.get_expected_arrival_times(bus_stop_id, bus_route)
            if len(expected_arrival_times) == 0:
                bus_stop_info.remove(bus_stop)

        print(len(bus_stop_info))
        return bus_stop_info


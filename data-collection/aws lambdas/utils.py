import json
import boto3
import csv
import datetime as dt
from pathlib import Path
from boto3.dynamodb.conditions import Key, Attr
from botocore.exceptions import ClientError
import time

class Utilities(object):

    def convert_time_to_datetime(self, given_time):
        start = time.time()
        year = int(given_time[:4])
        month = int(given_time[5:7])
        day = int(given_time[8:10])
        hour = int(given_time[11:13])
        minute = int(given_time[14:16])
        second = int(given_time[17:19])

        date_time = dt.datetime(year, month, day, hour, minute, second)
        comp_time = time.time() - start
        print("Convert time to datetime: ", comp_time)
        return date_time


    def convert_types_db(self, bus):
        start = time.time()
        vehicle_id = bus.get("vehicle_id").get("S")
        bus_stop_name = bus.get("bus_stop_name").get("S")
        direction = bus.get("direction").get("N")
        eta = self.convert_time_to_datetime(bus.get("expected_arrival").get("S"))
        time_of_req = self.convert_time_to_datetime(bus.get("time_of_req").get("S"))
        arrived = True if bus.get("arrived").get("S") else False
        comp_time = time.time() - start
        print("Convert types db: ", comp_time)
        return vehicle_id, bus_stop_name, direction, eta, time_of_req, arrived
        
        
    def get_valid_stop_ids(self, route):
        start = time.time()
        print("Reading stop IDs from dynamo")
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
            comp_time = time.time() - start
            print("Get valid stop IDs: ", comp_time)
            return results
        

    def write_to_db(self, table_name, bus_information):
        # If an item that has the same primary key as the new item already exists 
        # in the specified table, the new item completely replaces the existing item.
        start = time.time()

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
        else:
            comp_time = time.time() - start
            print("Write to db: ", comp_time)
            
            
    def get_old_info(self, route):
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
                vehicle_id, bus_stop_name, direction, eta, time_of_req, arrived = self.convert_types_db(res)
            
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


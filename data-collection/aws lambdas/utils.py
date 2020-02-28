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
        year = int(given_time[:4])
        month = int(given_time[5:7])
        day = int(given_time[8:10])
        hour = int(given_time[11:13])
        minute = int(given_time[14:16])
        second = int(given_time[17:19])

        date_time = dt.datetime(year, month, day, hour, minute, second)
        return date_time
        
        
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
        

    def write_to_db(self, table_name, bus_info_to_write):
        # If an item that has the same primary key as the new item already exists 
        # in the specified table, the new item completely replaces the existing item.
        start = time.time()

        dynamodb = boto3.client('dynamodb')

        try:
            for bus_information in bus_info_to_write:
                vehicle_id = bus_information.get("vehicle_id")
                bus_stop_name = bus_information.get("bus_stop_name")
                direction = str(bus_information.get("direction"))
                eta = str(bus_information.get("expected_arrival"))
                time_of_req = str(bus_information.get("time_of_req"))
                arrived = "True"
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


    def read_bus_info_from_csv(self, bus_id, date):
        bus_information = []
        file_name = 'bus_arrivals_' + bus_id + '.csv'
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
                            direction = int(row[2])
                            eta = self.convert_time_to_datetime(row[3])
                            time_of_req = self.convert_time_to_datetime(row[4])
                            arrived = True if row[5] == 'True' else False

                            vehicle_info = {
                                "vehicle_id": vehicle_id,
                                "bus_stop_name": bus_stop_name,
                                "direction": direction,
                                "expected_arrival": eta,
                                "time_of_req": time_of_req,
                                "arrived": arrived
                            }

                            bus_information.append(vehicle_info)
                        line_count += 1
            except IOError:
                print("I/O error in loading information from csv file")
        
        return bus_information


    def write_to_csv(self, arrival_array, bus_route_id):

        csv_columns = ['vehicle_id', 'bus_stop_name', 'direction', 'expected_arrival', 'time_of_req', 'arrived']
        csv_file = 'bus_arrivals_' + bus_route_id + '.csv'

        try:
            with open(csv_file, 'w') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames = csv_columns)
                writer.writeheader()
                for data in arrival_array:
                    writer.writerow(data)
        except IOError:
            print("I/O error in loading information into csv file")
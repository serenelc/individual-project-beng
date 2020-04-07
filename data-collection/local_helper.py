import json
import psycopg2
import csv
import datetime as dt
from pathlib import Path
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


    def convert_types_db(self, bus):
        # each bus is a tuple (key, vehicle_id, arrived, bus_stop_name, direction, expected_arrival, time_of_req)

        vehicle_id = bus[1]
        bus_stop_name = bus[3]
        direction = bus[4]
        eta = self.convert_time_to_datetime(bus[5])
        time_of_req = self.convert_time_to_datetime(bus[6])
        arrived = bus[2] # should be a boolean type anyway
        return vehicle_id, bus_stop_name, direction, eta, time_of_req, arrived
        
        
    def get_valid_stop_ids(self, route):
        start = time.time()
        print("Reading stop IDs from postgresql")
        table_name = "valid_stop_ids_" + route
        
        results = []

        conn = None
        try:
            conn = psycopg2.connect(host="localhost", database=table_name, user="postgres", password="postgres")
            cursor = conn.cursor()
            sql = "SELECT * FROM " + table_name 
            cursor.execute(sql)
            results = cursor.fetchall() #list of tuples (key, stop_id, stop_name)
            cursor.close()
        except (Exception, psycopg2.DatabaseError) as error:
            print("Error in getting valid stop IDs: ", error)
        finally:
            comp_time = time.time() - start
            print("Get valid stop IDs: ", comp_time)
            if conn is not None:
                conn.close()
            return results
            
    
    def try_write_to_db(self, dynamodb, route, bus_information):
        print("This is not the 1st journey of the day. Updated vehicle id: ", bus_information)
        table_name = "bus_arrivals_" + route
        
        try:
            dynamodb.put_item(TableName=table_name, 
                              Item=bus_information,
                              ConditionExpression='attribute_not_exists(vehicle_id)')
                                  
        except ClientError as e:
            if e.response['Error']['Code'] != 'ConditionalCheckFailedException':
                print("Error in try write db: ", e)
                raise
            else: #ConditionalCheckFailedException i.e. key already exists -> 2nd journey of the day
                print("Failed to write. Try again: ", e)
                vehicle_id = bus_information.get("vehicle_id").get("S")
                [a, b, c, d, num_trip] = vehicle_id.split('_')
                trip_num = int(num_trip) + 1
                new_id = a + "_" + b + "_" + c + "_" + d + "_" + str(trip_num)
                bus_information["vehicle_id"]['S'] = new_id
                self.try_write_to_db(dynamodb, route, bus_information)
            
            
    def write_to_db(self, dynamodb, route, bus_information):
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
                print("Error in write to db: ", e)
                raise
            else: #ConditionalCheckFailedException i.e. key already exists -> 2nd journey of the day
                print("ERROR: ", e)
                [a, b, c, d, num_trip] = vehicle_id.split('_')
                trip_num = int(num_trip) + 1
                new_id = a + "_" + b + "_" + c + "_" + d + "_" + str(trip_num)
                item["vehicle_id"]['S'] = new_id
                self.try_write_to_db(dynamodb, route, item)
        
        comp_time = time.time() - start
        # print("Write arrived items to db: ", comp_time)
                
            
    def batch_write_to_db(self, table_name, bus_info_to_write):
        start = time.time()
        
        if len(bus_info_to_write) == 0:
            print("Nothing to batch write to {}".format(table_name))
        else:
           
            print("Batch writing {} items to {}".format(len(bus_info_to_write), table_name))

            items_to_write = ()
            for bus_information in bus_info_to_write:
                vehicle_id = bus_information.get("vehicle_id")
                bus_stop_name = bus_information.get("bus_stop_name")
                direction = bus_information.get("direction")
                eta = str(bus_information.get("expected_arrival"))
                time_of_req = str(bus_information.get("time_of_req"))
                arrived = bus_information.get("arrived")
                tuple_item = (vehicle_id, arrived, bus_stop_name, direction, eta, time_of_req)
                items_to_write = items_to_write + tuple_item


            sql = ''.join(("INSERT INTO " + table_name + "(vehicle_id, arrived, bus_stop_name, direction, expected_arrival, time_of_req) ",
                            "VALUES (%s, %s, %s, %r, %s, %s)",
                            "ON CONFLICT (vehicle_id)",
                            "DO",
                            "UPDATE",
                            "SET vehicle_id = EXCLUDED.vehicle_id, expected_arrival = EXCLUDED.expected_arrival, time_of_req = EXCLUDED.time_of_req"
                            ))
            
            conn = None
            try:
                conn = psycopg2.connect(host="localhost", database=table_name, user="postgres", password="postgres")
                cursor = conn.cursor()
                cursor.executemany(sql, items_to_write)
                conn.commit()
                cursor.close()
            except (Exception, psycopg2.DatabaseError) as error:
                print("Error in batch writing to database: ", error)
            finally:
                if conn is not None:
                    conn.close()
            
            comp_time = time.time() - start
            print("Batch write to db: ", comp_time)
            
            
    def delete_arrived_items(self, table_name, arrived_items):
        start = time.time()

        if len(arrived_items) == 0:
            print("Nothing to delete in {}".format(table_name))
        else:
            print("Number of arrived items to delete {}".format(len(arrived_items)))
            try:
                dynamodb = boto3.client('dynamodb')
                
                for bus in arrived_items:
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
                raise
            
            comp_time = time.time() - start
            print("Delete from db: ", comp_time)
            
            
    def get_old_info(self, route):
        start = time.time()
        table_name = "bus_information_" + route
        
        results = []

        conn = None
        try:
            conn = psycopg2.connect(host="localhost", database=table_name, user="postgres", password="postgres")
            cursor = conn.cursor()
            sql = "SELECT * FROM " + table_name 
            cursor.execute(sql)
            results = cursor.fetchall() #list of tuples (vehicle_id, arrived, bus_stop_name, direction, expected_arrival, time_of_req)
            cursor.close()
        except (Exception, psycopg2.DatabaseError) as error:
            print("Error in getting old information: ", error)
        finally:
            if conn is not None:
                conn.close()
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
            print("Get old info: ", comp_time)
            return results
    
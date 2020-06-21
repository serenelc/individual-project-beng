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
        # each bus is a tuple (vehicle_id, bus_stop_name, expected_arrival, time_of_req, direction)
        vehicle_id = bus[0]
        bus_stop_name = bus[1]
        direction = bus[4]
        eta = bus[2]
        time_of_req = bus[3]
        return vehicle_id, bus_stop_name, direction, eta, time_of_req
        
        
    def get_valid_stop_ids(self, route):
        start = time.time()
        print("Reading stop IDs from postgresql")
        table_name = "valid_stop_ids_" + route
        
        results = []

        conn = None
        try:
            conn = psycopg2.connect(host="localhost", database="bus_predictions", user="postgres", password='example', port="5432")
            cursor = conn.cursor()
            sql = "SELECT * FROM " + table_name 
            cursor.execute(sql)
            results = cursor.fetchall() #list of tuples (stop_id, stop_name)
            cursor.close()
        except (Exception, psycopg2.DatabaseError) as error:
            print("Error in getting valid stop IDs: ", error)
        finally:
            comp_time = time.time() - start
            print("Get valid stop IDs: ", comp_time)
            if conn is not None:
                conn.close()
            return results
            
            
    def write_to_db(self, table_name, bus_info_to_write):

        start = time.time()
        if len(bus_info_to_write) == 0:
            print("Nothing to write to {}".format(table_name))
        else:
            print("Writing {} items to {}".format(len(bus_info_to_write), table_name))

            for item in bus_info_to_write:
                vehicle_id = item.get("vehicle_id")
                bus_stop_name = item.get("bus_stop_name")
                direction = str(item.get("direction"))
                eta = str(item.get("expected_arrival"))
                time_of_req = str(item.get("time_of_req"))
                journey = 0

                sql_select = "SELECT vehicle_id FROM " + table_name + " WHERE vehicle_id LIKE '" + vehicle_id + "'"

                conn = None
                try:
                    conn = psycopg2.connect(host="localhost", database="bus_predictions", user="postgres", password='example', port="5432")
                    cursor = conn.cursor()

                    cursor.execute(sql_select)
                    rows = cursor.fetchall()
                    if len(rows) > 0:
                        journey = journey + len(rows) - 1

                    tuple_item = (vehicle_id, bus_stop_name, direction, eta, time_of_req, journey)
                    sql_put = "INSERT INTO " + table_name + "(vehicle_id, bus_stop_name, direction, time_of_arrival, time_of_req, journey) "
                    sql_put = sql_put + "VALUES (%s, %s, %s, %s, %s, %s)"

                    cursor.execute(sql_put, (tuple_item))

                    conn.commit()
                    cursor.close()
                except (Exception, psycopg2.DatabaseError) as error:
                    print("Error in writing arrived items: ", error)
                finally:
                    if conn is not None:
                        conn.close()

            comp_time = time.time() - start
            print("Write arrived items to db: ", comp_time)


    def write_to_db_2(self, table_name, bus_info_to_write):
        start = time.time()
        if len(bus_info_to_write) == 0:
            print("Nothing to write to {}".format(table_name))
        else:
            print("Writing {} items to {}".format(len(bus_info_to_write), table_name))

            for item in bus_info_to_write:
                vehicle_id = item.get("vehicle_id")
                bus_stop_name = item.get("bus_stop_name")
                direction = item.get("direction")
                eta = str(item.get("expected_arrival"))
                time_of_req = str(item.get("time_of_req"))
                tuple_item = (vehicle_id, bus_stop_name, direction, eta, time_of_req)

                conn = None
                try:
                    conn = psycopg2.connect(host="localhost", database="bus_predictions", user="postgres", password='example', port="5432")
                    cursor = conn.cursor()
                    sql = ''.join(("INSERT INTO " + table_name + "(vehicle_id, bus_stop_name, direction, expected_arrival, time_of_req) ",
                            "VALUES (%s, %s, %s, %s, %s) ",
                            "ON CONFLICT (vehicle_id) ",
                            "DO ",
                            "UPDATE ",
                            "SET vehicle_id = EXCLUDED.vehicle_id, expected_arrival = EXCLUDED.expected_arrival, time_of_req = EXCLUDED.time_of_req"
                            ))

                    cursor.execute(sql, (tuple_item))
                    conn.commit()
                    cursor.close()
                except (Exception, psycopg2.DatabaseError) as error:
                    print("Error in writing not arrived items: ", error)
                finally:
                    if conn is not None:
                        conn.close()

            comp_time = time.time() - start
            print("Write not arrived items to db: ", comp_time)
            
            
    def delete_arrived_items(self, table_name, arrived_items):
        start = time.time()

        if len(arrived_items) == 0:
            print("Nothing to delete in {}".format(table_name))
        else:
            print("Number of arrived items to delete {}".format(len(arrived_items)))
            conn = None
            try:
                conn = psycopg2.connect(host="localhost", database="bus_predictions", user="postgres", password='example', port="5432")
                cursor = conn.cursor()
                for arrived in arrived_items:
                    vehicle_id = arrived.get("vehicle_id")
                    vehicle_id = "'" + vehicle_id + "'"
                    sql = "DELETE FROM " + table_name + " WHERE vehicle_id = " + vehicle_id
                    cursor.execute(sql)

                conn.commit()
                cursor.close()
            except (Exception, psycopg2.DatabaseError) as error:
                print("Error in deleting arrived items: ", error)
            finally:
                if conn is not None:
                    conn.close()
            
            comp_time = time.time() - start
            print("Delete from db: ", comp_time)
            
            
    def get_old_info(self, route):
        start = time.time()
        table_name = "bus_information_" + route
        
        results = []

        conn = None
        try:
            conn = psycopg2.connect(host="localhost", database="bus_predictions", user="postgres", password='example', port="5432")
            cursor = conn.cursor()
            sql = "SELECT * FROM " + table_name 
            cursor.execute(sql)
            results = cursor.fetchall() #list of tuples (vehicle_id, bus_stop_name, expected_arrival, time_of_req, direction)
            cursor.close()
        except (Exception, psycopg2.DatabaseError) as error:
            print("Error in getting old information: ", error)
        finally:
            if conn is not None:
                conn.close()

            old_information = []
            
            for res in results:
                vehicle_id, bus_stop_name, direction, eta, time_of_req = self.convert_types_db(res)
            
                vehicle_info = {
                                "vehicle_id": vehicle_id,
                                "bus_stop_name": bus_stop_name,
                                "direction": direction,
                                "expected_arrival": eta,
                                "time_of_req": time_of_req
                                }
                old_information.append(vehicle_info)

            comp_time = time.time() - start
            print("Get old info: ", comp_time)
            return old_information
    
import csv
import datetime as dt
from pathlib import Path
from os import path
import time

class Helper(object):

    def read_bus_info_from_csv(self, bus_id):
        bus_information = []
        file_name = 'past_data/bus_information_' + bus_id + '.csv'
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

    
    def get_valid_stop_ids(self, bus_route):
        start = time.time()
        route_information = []
        file_name = 'valid_stops/valid_stop_ids_' + bus_route + '.csv'
        bus_file = Path.cwd() / file_name
    
        if bus_file.is_file():
            try:
                with open(file_name) as csv_file:
                    csv_reader = csv.reader(csv_file, delimiter = ',')
                    line_count = 0
                    for row in csv_reader:
                        if line_count != 0:
                            stop_name = row[0]
                            stop_id = row[1]

                            valid_stop = {
                                "stop_name": stop_name,
                                "stop_id": stop_id
                            }

                            route_information.append(valid_stop)
                        line_count += 1
            except IOError:
                print("I/O error in loading valid stop information from csv file")
        
        comp_time = time.time() - start
        print("Get valid stop ids: ", comp_time)
        return route_information


    def append_to_csv(self, csv_name, bus_info_to_append):
        start = time.time()
        
        if len(bus_info_to_append) == 0:
            print("Nothing to write to {}".format(csv_name))
        else:
            print("Writing {} items to {}".format(len(bus_info_to_append), csv_name))

            try:
                csv_columns = ['vehicle_id', 'bus_stop_name', 'direction', 'expected_arrival', 'time_of_req', 'arrived']
                with open(csv_name, 'a+') as csv_file:
                    writer = csv.DictWriter(csv_file, fieldnames = csv_columns)
                    for data in bus_info_to_append:
                        writer.writerow(data)
            except IOError:
                print("I/O error in appending information to csv file")

        comp_time = time.time() - start
        print("Appending to csv: ", comp_time)


    def write_to_csv(self, csv_name, bus_info_to_write):
        start = time.time()
        
        if len(bus_info_to_write) == 0:
            print("Nothing to write to {}".format(csv_name))
        else:
            print("Writing {} items to {}".format(len(bus_info_to_write), csv_name))

            csv_columns = ['vehicle_id', 'bus_stop_name', 'direction', 'expected_arrival', 'time_of_req', 'arrived']
            try:
                with open(csv_name, 'w') as csv_file:
                    writer = csv.DictWriter(csv_file, fieldnames = csv_columns)
                    writer.writeheader()
                    for data in bus_info_to_write:
                        writer.writerow(data)
            except IOError:
                print("I/O error in loading information into csv file")

        comp_time = time.time() - start
        print("Write to csv: ", comp_time)

    
    def delete_arrived_items(self, csv_file, arrived_items):
        start = time.time()

        if len(arrived_items) == 0:
            print("Nothing to delete in {}".format(csv_file))
        else:
            print("Number of arrived items to delete {}".format(len(arrived_items)))
            try:
                items = []
                with open(csv_file, 'r') as read_file:
                    csv_reader = csv.reader(read_file)
                    for row in csv_reader:
                        for arrived in arrived_items:
                            items.append(arrived)
                            if row[0] == arrived.get("vehicle_id"):
                                items.pop()
                print("before issue")
                self.write_to_csv(csv_file, items)
                print("after issue")
            except IOError:
                    print("I/O error in deleting information from csv")
            
            comp_time = time.time() - start
            print("Delete from csv: ", comp_time)


    def convert_time_to_datetime(self, given_time):
        year = int(given_time[:4])
        month = int(given_time[5:7])
        day = int(given_time[8:10])
        hour = int(given_time[11:13])
        minute = int(given_time[14:16])
        second = int(given_time[17:19])

        date_time = dt.datetime(year, month, day, hour, minute, second)
        return date_time


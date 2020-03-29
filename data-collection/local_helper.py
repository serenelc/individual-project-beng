import csv
import datetime as dt
from pathlib import Path
from local_data_collection import Data_Collection

class Helper(object):

    def read_bus_info_from_csv(self, bus_id, date):
        bus_information = []
        file_name = 'bus_arrivals_' + bus_id + '_' + date + '.csv'
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
                            timestamp = self.convert_time_to_datetime(row[4])
                            arrived = True if row[5] == 'True' else False

                            vehicle_info = {
                                "vehicle_id": vehicle_id,
                                "bus_stop_name": bus_stop_name,
                                "direction": direction,
                                "expected_arrival": eta,
                                "timestamp": timestamp,
                                "arrived": arrived
                            }

                            bus_information.append(vehicle_info)
                        line_count += 1
            except IOError:
                print("I/O error in loading information from csv file")
        
        return bus_information

    
    def get_valid_bus_stop_ids(self, bus_route):
        bus_information = []
        file_name = 'valid_stop_ids_' + bus_route + '.csv'
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
                            timestamp = self.convert_time_to_datetime(row[4])
                            arrived = True if row[5] == 'True' else False

                            vehicle_info = {
                                "vehicle_id": vehicle_id,
                                "bus_stop_name": bus_stop_name,
                                "direction": direction,
                                "expected_arrival": eta,
                                "timestamp": timestamp,
                                "arrived": arrived
                            }

                            bus_information.append(vehicle_info)
                        line_count += 1
            except IOError:
                print("I/O error in loading information from csv file")
        
        return bus_information


    def write_to_csv(self, arrival_array, bus_route_id):
        today = dt.datetime.today().strftime('%Y-%m-%d')

        csv_columns = ['vehicle_id', 'bus_stop_name', 'direction', 'expected_arrival', 'timestamp', 'arrived']
        csv_file = 'bus_arrivals_' + bus_route_id + '_' + today + '.csv'

        try:
            with open(csv_file, 'w') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames = csv_columns)
                writer.writeheader()
                for data in arrival_array:
                    writer.writerow(data)
        except IOError:
            print("I/O error in loading information into csv file")


    def convert_time_to_datetime(self, given_time):
        year = int(given_time[:4])
        month = int(given_time[5:7])
        day = int(given_time[8:10])
        hour = int(given_time[11:13])
        minute = int(given_time[14:16])
        second = int(given_time[17:19])

        date_time = dt.datetime(year, month, day, hour, minute, second)
        return date_time

    
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


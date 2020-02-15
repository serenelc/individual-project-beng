import csv
import datetime as dt
from pathlib import Path

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
                            direction = row[2]
                            eta = self.convert_time_to_datetime(row[3])
                            timestamp = self.convert_time_to_datetime(row[4])
                            arrived = row[5]

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

